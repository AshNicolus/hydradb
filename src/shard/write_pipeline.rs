use super::*;
use crate::core::memory_diagnostics::{
    request_estimated_bytes, MemoryDiagnosticGuard, MemoryStage, TrackedMemory,
};
use std::future::Future;
use std::sync::atomic::AtomicU64;
use tracing::Instrument as _;

pub(crate) const MAX_PENDING_DURABLE_WRITES: usize = 16;

tokio::task_local! {
    static PIPELINE: Arc<WritePipeline>;
}

pub(crate) struct WritePipeline {
    writer: TrackedMemory<slatedb::Db>,
    path: String,
    writer_epoch: u64,
    required_sequence: AtomicU64,
    _admission: OwnedSemaphorePermit,
}

pub(crate) fn current() -> Option<Arc<WritePipeline>> {
    PIPELINE.try_with(Arc::clone).ok()
}

pub(crate) fn read_durability() -> DurabilityLevel {
    if current().is_some() {
        DurabilityLevel::Memory
    } else {
        DurabilityLevel::Remote
    }
}

pub(crate) async fn wait_durable(writer: &slatedb::Db, sequence: u64) -> Result<()> {
    let mut status = writer.subscribe();
    loop {
        if status.borrow_and_update().durable_seq >= sequence {
            return Ok(());
        }
        // Surface the exact closed/fenced error from this writer generation.
        writer.snapshot().await?;
        status
            .changed()
            .await
            .map_err(|error| GraphError::CorruptValue {
                key: "write_pipeline/durability".into(),
                reason: format!("writer status channel closed: {error}"),
            })?;
    }
}

impl WritePipeline {
    fn validate_writer(&self, shard: &GraphShard) -> Result<()> {
        if shard.db.writer_epoch() != Some(self.writer_epoch) {
            return Err(GraphError::ConditionalWriteConflict {
                operation: "write_pipeline_writer_changed",
                key: self.path.clone(),
            });
        }
        Ok(())
    }

    pub(crate) async fn commit_local(
        self: &Arc<Self>,
        txn: DbTransaction,
        sequence: u64,
    ) -> Result<Option<StorageSequence>> {
        let context = Arc::clone(self);
        let (send, receive) = tokio::sync::oneshot::channel();
        let commit_span = tracing::Span::current();
        // The owned task retains admission and the exact writer until durability,
        // even if its client disappears during local apply or the S3 wait.
        tokio::spawn(async move {
            match txn
                .commit_with_options(&WriteOptions { seqnum: sequence })
                .instrument(commit_span)
                .await
            {
                Ok(handle) => {
                    let committed = handle.map(|handle| handle.seqnum());
                    if let Some(sequence) = committed {
                        context
                            .required_sequence
                            .fetch_max(sequence, Ordering::AcqRel);
                    }
                    let _ = send.send(Ok(committed));
                    let _durability =
                        MemoryDiagnosticGuard::new(MemoryStage::ShardWriteDurability, 0);
                    let _ = wait_durable(
                        &context.writer,
                        context.required_sequence.load(Ordering::Acquire),
                    )
                    .await;
                }
                Err(error) => {
                    let _ = send.send(Err(error.into()));
                }
            }
        });
        receive.await.map_err(|error| GraphError::CorruptValue {
            key: "write_pipeline/local_commit".into(),
            reason: format!("local commit task did not complete: {error}"),
        })?
    }
}

impl GraphShard {
    pub(crate) async fn run_write_pipeline<T>(
        &self,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        if !self.await_durable_writes {
            return operation.await;
        }
        if let Some(context) =
            current().filter(|context| context.path == self.db.store_path().as_ref())
        {
            context.validate_writer(self)?;
            return operation.await;
        }
        let waiting = MemoryDiagnosticGuard::new(
            MemoryStage::ShardWritePipelineWait,
            request_estimated_bytes(),
        );
        if self.write_pipeline_gate.available_permits() == 0 {
            self.operation_metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        let acquire = Arc::clone(&self.write_pipeline_gate)
            .acquire_owned()
            .instrument(tracing::info_span!("shard.write_pipeline_admission"));
        let admission = match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout { operation: "query_cancelled", elapsed_ms: 0, limit_ms: 0 }),
                permit = acquire => permit,
            },
            None => acquire.await,
        }.map_err(|error| GraphError::CorruptValue {
            key: "write_pipeline/admission".into(), reason: error.to_string(),
        })?;
        drop(waiting);
        let writer = self.db.writer()?;
        let sequence = match writer.snapshot().await {
            Ok(snapshot) => snapshot.seq(),
            Err(error) => {
                // Preserve fenced-handle cleanup and re-open backoff. Healthy
                // writers never need a manifest GET at this entry point.
                self.db.refresh_writer_fence().await?;
                return Err(error.into());
            }
        };
        let writer_epoch = writer.status().current_manifest.writer_epoch();
        let context = Arc::new(WritePipeline {
            writer,
            writer_epoch,
            path: self.db.store_path().to_string(),
            required_sequence: AtomicU64::new(sequence),
            _admission: admission,
        });
        let result = PIPELINE.scope(Arc::clone(&context), operation).await;
        // All local guards and the graph admission permit have dropped here.
        // No-op and replay responses also wait for the state they observed.
        let required = context
            .required_sequence
            .load(Ordering::Acquire)
            .max(context.writer.snapshot().await?.seq());
        let durable = wait_durable(&context.writer, required).instrument(tracing::info_span!(
            "storage.durability_wait",
            hydradb.storage_sequence = required
        ));
        match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout { operation: "query_cancelled", elapsed_ms: 0, limit_ms: 0 }),
                result = durable => result?,
            },
            None => durable.await?,
        }
        result
    }

    pub(crate) async fn await_prior_write_durability(&self) -> Result<()> {
        if !self.await_durable_writes {
            return Ok(());
        }
        if let Some(context) =
            current().filter(|context| context.path == self.db.store_path().as_ref())
        {
            return context.validate_writer(self);
        }
        // Legacy/maintenance transactions still read Remote. Drain predecessors
        // under their local guard so they never base a new sequence on old data.
        let writer = self.db.writer()?;
        let result = async {
            let sequence = writer.snapshot().await?.seq();
            wait_durable(&writer, sequence).await
        }
        .await;
        if result.is_err() {
            self.db.refresh_writer_fence().await?;
        }
        result
    }
}

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use async_trait::async_trait;
use slatedb::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache};
use slatedb::object_store::ObjectStore;

use crate::{GraphError, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlateDbCacheMetricsSnapshot {
    pub capacity_bytes: u64,
    /// Accounted cached payload bytes; excludes cache metadata and allocator overhead.
    pub resident_bytes: u64,
    pub entries: u64,
}

/// One bounded SlateDB RAM cache shared by every reader and writer opened over
/// the same process-local object-store handle. SlateDB scopes every cache key,
/// so entries from different graph databases cannot collide.
pub(crate) struct SharedSlateDbCache {
    inner: Option<foyer::Cache<CachedKey, CachedEntry>>,
    capacity_bytes: u64,
}

impl SharedSlateDbCache {
    fn new(capacity_bytes: usize) -> Self {
        let inner = (capacity_bytes > 0).then(|| {
            foyer::CacheBuilder::new(capacity_bytes)
                .with_weighter(|_, value: &CachedEntry| value.size())
                .build()
        });
        Self {
            inner,
            capacity_bytes: capacity_bytes as u64,
        }
    }

    pub(crate) fn snapshot(&self) -> SlateDbCacheMetricsSnapshot {
        SlateDbCacheMetricsSnapshot {
            capacity_bytes: self.capacity_bytes,
            resident_bytes: self.inner.as_ref().map_or(0, |cache| cache.usage() as u64),
            entries: self
                .inner
                .as_ref()
                .map_or(0, |cache| cache.entries() as u64),
        }
    }

    async fn fetch(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> std::result::Result<CachedEntry, slatedb::Error> {
        let Some(cache) = &self.inner else {
            return loader().await;
        };
        cache
            .get_or_fetch(&key, move || async move {
                // Entry weights count visible bytes, not shared range-buffer capacity.
                loader().await.map(|value| value.clamp_allocated_size())
            })
            .await
            .map(|entry| entry.value().clone())
            .map_err(|error| slatedb::Error::unavailable(error.to_string()))
    }
}

#[async_trait]
impl DbCache for SharedSlateDbCache {
    async fn get_block(
        &self,
        key: &CachedKey,
    ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self
            .inner
            .as_ref()
            .and_then(|cache| cache.get(key).map(|entry| entry.value().clone())))
    }

    async fn get_index(
        &self,
        key: &CachedKey,
    ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }

    async fn get_filter(
        &self,
        key: &CachedKey,
    ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }

    async fn get_stats(
        &self,
        key: &CachedKey,
    ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        if let Some(cache) = &self.inner {
            cache.insert(key, value.clamp_allocated_size());
        }
    }

    async fn remove(&self, key: &CachedKey) {
        if let Some(cache) = &self.inner {
            cache.remove(key);
        }
    }

    fn entry_count(&self) -> u64 {
        self.snapshot().entries
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> std::result::Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> std::result::Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> std::result::Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> std::result::Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }
}

static PROCESS_SLATE_DB_CACHES: OnceLock<Mutex<BTreeMap<usize, Weak<SharedSlateDbCache>>>> =
    OnceLock::new();

pub(crate) fn process_slate_db_cache(
    object_store: &Arc<dyn ObjectStore>,
    capacity_bytes: usize,
) -> Result<Arc<SharedSlateDbCache>> {
    let key = Arc::as_ptr(object_store) as *const () as usize;
    let caches = PROCESS_SLATE_DB_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut caches = caches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    caches.retain(|_, cache| cache.strong_count() > 0);
    if let Some(cache) = caches.get(&key).and_then(Weak::upgrade) {
        let configured = cache.snapshot().capacity_bytes;
        if configured != capacity_bytes as u64 {
            return Err(GraphError::CorruptValue {
                key: "slatedb/process-cache".to_string(),
                reason: format!(
                    "object store already uses a {configured}-byte SlateDB cache; requested {capacity_bytes} bytes"
                ),
            });
        }
        return Ok(cache);
    }
    let cache = Arc::new(SharedSlateDbCache::new(capacity_bytes));
    caches.insert(key, Arc::downgrade(&cache));
    Ok(cache)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{BlockTransformer, Db, DbReader, DbReaderMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct OversizedBacking {
        bytes: Vec<u8>,
        len: usize,
        retained: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for OversizedBacking {
        fn as_ref(&self) -> &[u8] {
            &self.bytes[..self.len]
        }
    }

    impl Drop for OversizedBacking {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes.len(), Ordering::SeqCst);
        }
    }

    struct OversizedDecode(Arc<AtomicUsize>);

    #[async_trait]
    impl BlockTransformer for OversizedDecode {
        async fn encode(&self, data: Bytes) -> std::result::Result<Bytes, slatedb::Error> {
            Ok(data)
        }

        async fn decode(&self, data: Bytes) -> std::result::Result<Bytes, slatedb::Error> {
            // Model a small decoded slice retaining an object-store range buffer.
            let mut bytes = vec![0; data.len().max(4 * 1024 * 1024)];
            bytes[..data.len()].copy_from_slice(&data);
            self.0.fetch_add(bytes.len(), Ordering::SeqCst);
            Ok(Bytes::from_owner(OversizedBacking {
                bytes,
                len: data.len(),
                retained: Arc::clone(&self.0),
            }))
        }
    }

    async fn assert_cache_releases_oversized_backing(scan: bool) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = Db::builder("cache-backing", Arc::clone(&store))
            .with_db_cache_disabled()
            .build()
            .await
            .unwrap();
        for id in 0..32 {
            writer
                .put(format!("key-{id:04}"), vec![42u8; 1024])
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
        writer.close().await.unwrap();
        drop(writer);

        let retained = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(SharedSlateDbCache::new(1024 * 1024));
        let reader = DbReader::builder("cache-backing", store)
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_db_cache(cache.clone())
            .with_block_transformer(Arc::new(OversizedDecode(Arc::clone(&retained))))
            .build()
            .await
            .unwrap();
        if scan {
            let mut rows = reader
                .scan_with_options(
                    ..,
                    &slatedb::config::ScanOptions {
                        cache_blocks: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut count = 0;
            while let Some(row) = rows.next().await.unwrap() {
                assert_eq!(row.value.as_ref(), &[42u8; 1024]);
                count += 1;
            }
            assert_eq!(count, 32);
        } else {
            for id in 0..32 {
                let value = reader.get(format!("key-{id:04}")).await.unwrap().unwrap();
                assert_eq!(value.as_ref(), &[42u8; 1024]);
            }
        }
        reader.close().await.unwrap();
        drop(reader);
        let usage = cache.snapshot();
        println!(
            "scan={scan},accounted_bytes={},capacity_bytes={},oversized_backing_bytes={}",
            usage.resident_bytes,
            usage.capacity_bytes,
            retained.load(Ordering::SeqCst)
        );
        assert!(
            usage.entries > 0,
            "test must leave data resident in the cache"
        );
        assert!(usage.resident_bytes < usage.capacity_bytes);
        assert_eq!(
            retained.load(Ordering::SeqCst),
            0,
            "cache retains oversized backing allocations despite small accounted entries"
        );
    }

    #[tokio::test]
    async fn point_fetch_releases_oversized_backing() {
        assert_cache_releases_oversized_backing(false).await;
    }

    #[tokio::test]
    async fn scan_insert_releases_oversized_backing() {
        assert_cache_releases_oversized_backing(true).await;
    }

    #[test]
    fn one_object_store_gets_one_process_cache() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = process_slate_db_cache(&object_store, 1024).unwrap();
        let second = process_slate_db_cache(&object_store, 1024).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.snapshot().capacity_bytes, 1024);
    }

    #[test]
    fn conflicting_process_cache_budgets_fail_closed() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let _cache = process_slate_db_cache(&object_store, 1024).unwrap();
        let error = match process_slate_db_cache(&object_store, 2048) {
            Ok(_) => panic!("conflicting cache budget must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("already uses a 1024-byte"));
    }

    #[test]
    fn zero_budget_disables_ram_admission() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = process_slate_db_cache(&object_store, 0).unwrap();
        assert_eq!(cache.snapshot(), SlateDbCacheMetricsSnapshot::default());
    }
}

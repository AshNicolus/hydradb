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
                reason: format!("object store already uses a {configured}-byte SlateDB cache; requested {capacity_bytes} bytes"),
            });
        }
        return Ok(cache);
    }
    let cache = Arc::new(SharedSlateDbCache::new(capacity_bytes));
    caches.insert(key, Arc::downgrade(&cache));
    Ok(cache)
}

// Adapter: implement `elide_fetch::RangeFetcher` on top of `object_store`.
//
// The coordinator already owns an `Arc<dyn ObjectStore>` for upload, list and
// delete; demand-fetch (called from `spawn_blocking` worker threads) needs a
// sync interface. This wrapper bridges the two by capturing the current tokio
// runtime handle at construction time and using `Handle::block_on` to drive
// the async `get_range` call.
//
// Must be constructed inside a tokio runtime (so `Handle::current()` resolves);
// `get_range` itself must be called from a blocking context — i.e. a thread
// outside the reactor's worker pool, such as one spawned by `spawn_blocking`.

use std::io;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as StorePath;

use elide_fetch::RangeFetcher;

pub struct ObjectStoreRangeFetcher {
    store: Arc<dyn ObjectStore>,
    handle: tokio::runtime::Handle,
}

impl ObjectStoreRangeFetcher {
    /// Construct an adapter capturing the current tokio runtime handle.
    /// Panics if called outside a tokio runtime.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl RangeFetcher for ObjectStoreRangeFetcher {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
        let path = StorePath::from(key);
        let range = (start as usize)..(end_exclusive as usize);
        let store = self.store.clone();
        let result = self
            .handle
            .block_on(async move { store.get_range(&path, range).await });
        match result {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(object_store::Error::NotFound { .. }) => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{key} not found"),
            )),
            Err(e) => Err(io::Error::other(format!(
                "object_store get_range {key}: {e}"
            ))),
        }
    }
}

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct AppState {
    pub data_root: Arc<PathBuf>,
    pub inflight: Arc<Semaphore>,
    pub max_size: u64,
}

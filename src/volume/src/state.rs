use std::sync::Arc;
use std::path::PathBuf;
use reqwest::Client;

use crate::fault_injection::FaultInjector;

#[derive(Clone)]
pub struct VolumeState {
    pub http_client: Client,
    pub data_root: Arc<PathBuf>,
    pub coordinator_url: String,
    pub node_id: String,
    pub public_url: String,
    pub internal_url: String,
    pub subvols: u16,
    pub heartbeat_interval_secs: u64,
    pub http_timeout_secs: u64,
    pub fault_injector: Arc<FaultInjector>,
}
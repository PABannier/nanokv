use reqwest::Client;
use std::path::PathBuf;
use std::sync::Arc;

use crate::fault_injection::FaultInjector;

#[derive(Clone, Debug)]
pub enum DurabilityLevel {
    Immediate, // fsync at end of each PUT (default, safest)
    OS,        // no explicit fsync; rely on OS (fastest)
}

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
    pub durability_level: DurabilityLevel,
}

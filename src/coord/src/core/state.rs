use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Semaphore;

use crate::core::meta::KvDb;
use crate::core::node::NodeRuntime;

#[derive(Clone)]
pub struct CoordinatorState {
    pub http_client: Client,

    // Control-plane inflight (placement/meta)
    pub control_inflight: Arc<Semaphore>,
    // Data-plane inflight (objects)
    pub data_inflight: Arc<Semaphore>,
    // Per-node transfer inflight
    pub per_node_inflight: Arc<RwLock<HashMap<String, Arc<Semaphore>>>>,

    pub max_size: u64,
    pub db: KvDb,
    pub nodes: Arc<RwLock<HashMap<String, NodeRuntime>>>,

    pub n_replicas: usize,
    pub hb_alive_secs: u64,
    pub hb_down_secs: u64,
    pub node_status_sweep_secs: u64,

    pub max_control_inflight: usize,
    pub max_data_inflight: usize,
    pub max_per_node_inflight: usize,
}

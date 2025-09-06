use std::time::Instant;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,              // provided by volume (stable)
    pub public_url: String,           // where the client GET blobs
    pub internal_url: String,         // where coord does internal ops
    pub subvols: u16,                 // number of disks (for later)
    pub last_heartbeat_ms: i128,      // persisted wall-clock (UTC ms)
    pub capacity_bytes: Option<u64>,  // optional metrics
    pub used_bytes: Option<u64>,
    pub status: NodeStatus,
    pub version: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NodeRuntime {
    pub info: NodeInfo,
    pub last_seen: Instant,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Down,
}

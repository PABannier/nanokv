use time::OffsetDateTime;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,              // provided by volume (stable)
    pub public_url: String,           // e.g. http://host:3001
    pub internal_url: String,         // same or different port
    pub subvols: u16,                 // number of disks (for later)
    pub last_hearbeat_ms: i128,
    pub capacity_bytes: Option<u64>,  // optional metrics
    pub used_bytes: Option<u64>,
    pub status: NodeStatus,
    pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Down,
}

use std::time::Instant;
use serde::{Serialize, Deserialize};

use common::constants::NODE_KEY_PREFIX;
use common::url_utils::node_id_from_url;
use common::time_utils::utc_now_ms;

use crate::core::meta::KvDb;

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

pub fn discover_alive_nodes(db: &KvDb, include_suspect: bool) -> anyhow::Result<Vec<NodeInfo>> {
    let mut nodes = Vec::new();
    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(NODE_KEY_PREFIX.as_bytes()) { continue; }
        let node_info: NodeInfo = serde_json::from_slice(&v)?;
        match node_info.status {
            NodeStatus::Alive => nodes.push(node_info),
            NodeStatus::Suspect => if include_suspect { nodes.push(node_info) },
            NodeStatus::Down => (),
        }
    }
    Ok(nodes)
}

pub fn build_explicit_volume_urls(volume_urls: Option<Vec<String>>) -> Vec<NodeInfo> {
    let mut nodes = Vec::new();
    match volume_urls {
        None => nodes,
        Some(volume_urls) => {
            for volume_url in volume_urls {
                nodes.push(NodeInfo {
                    node_id: node_id_from_url(&volume_url),
                    public_url: volume_url.clone(),
                    internal_url: volume_url,
                    subvols: 1,
                    capacity_bytes: None,
                    used_bytes: None,
                    version: None,
                    last_heartbeat_ms: utc_now_ms(),
                    status: NodeStatus::Alive,
                });
            }
            nodes
        }
    }
}
use blake3::Hasher;
use serde::{Serialize, Deserialize};


const N_TOP_BYTES_FOR_SCORE: usize = 16;


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

pub fn rank_nodes<'a>(key: &str, nodes: &'a [NodeInfo]) -> Vec<&'a NodeInfo>{
    let mut scored: Vec<(u128, &NodeInfo)> = nodes.iter().map(|n| {
        let mut h = Hasher::new();
        h.update(key.as_bytes());
        h.update(n.node_id.as_bytes());

        let hash = h.finalize();
        let hash_bytes = hash.as_bytes();
        let mut score_bytes = [0u8; N_TOP_BYTES_FOR_SCORE];
        score_bytes.copy_from_slice(&hash_bytes[0..N_TOP_BYTES_FOR_SCORE]);

        (u128::from_be_bytes(score_bytes), n)
    }).collect();
    scored.sort_by(|a,b| b.0.cmp(&a.0));

    scored.into_iter().map(|(_,n)| n).collect()
}

use blake3::Hasher;

use crate::state::CoordinatorState;
use crate::node::{NodeInfo, NodeStatus};


const N_TOP_BYTES_FOR_SCORE: usize = 16;


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

pub fn alive_nodes_for_placement(ctx: &CoordinatorState) -> anyhow::Result<Vec<NodeInfo>> {
    let nodes = ctx.nodes.read().map_err(|e| anyhow::anyhow!("failed to acquire nodes read lock: {}", e))?;
    let alive_nodes = nodes.iter().filter_map(|(_,n)| {
        if n.info.status == NodeStatus::Alive {
            Some(n.info.clone())
        } else {
            None
        }
    }).collect();
    Ok(alive_nodes)
}

use anyhow::anyhow;
use blake3::Hasher;
use common::api_error::ApiError;
use rand::seq::IndexedRandom;

use crate::core::meta::Meta;
use crate::core::node::{NodeInfo, NodeStatus};
use crate::core::state::CoordinatorState;

const N_TOP_BYTES_FOR_SCORE: usize = 16;

pub fn rank_nodes<'a>(key: &str, nodes: &'a [NodeInfo]) -> Vec<&'a NodeInfo> {
    let mut scored: Vec<(u128, &NodeInfo)> = nodes
        .iter()
        .map(|n| {
            let mut h = Hasher::new();
            h.update(key.as_bytes());
            h.update(n.node_id.as_bytes());

            let hash = h.finalize();
            let hash_bytes = hash.as_bytes();
            let mut score_bytes = [0u8; N_TOP_BYTES_FOR_SCORE];
            score_bytes.copy_from_slice(&hash_bytes[0..N_TOP_BYTES_FOR_SCORE]);

            (u128::from_be_bytes(score_bytes), n)
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0));

    scored.into_iter().map(|(_, n)| n).collect()
}

pub fn choose_top_n_alive(nodes: &[NodeInfo], key: &str, n: usize) -> Vec<NodeInfo> {
    let alive_nodes = nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Alive)
        .cloned()
        .collect::<Vec<NodeInfo>>();
    let ranked_nodes = rank_nodes(key, &alive_nodes);
    ranked_nodes
        .into_iter()
        .take(n)
        .cloned()
        .collect::<Vec<NodeInfo>>()
}

pub fn get_volume_url_for_key(ctx: &CoordinatorState, meta: &Meta) -> Result<String, ApiError> {
    let nodes = ctx
        .nodes
        .read()
        .map_err(|e| anyhow::anyhow!("failed to acquire nodes read lock: {}", e))?;

    let alive_replicas: Vec<_> = meta
        .replicas
        .iter()
        .filter(|node_id| {
            nodes
                .get(*node_id)
                .map(|n| n.info.status == NodeStatus::Alive)
                .unwrap_or(false)
        })
        .collect();

    // Select randomly a replica to avoid saturating a single volume
    let node_id = alive_replicas
        .choose(&mut rand::rng())
        .ok_or_else(|| ApiError::NoReplicasAvailable)?;

    let node = nodes.get(*node_id).ok_or_else(|| ApiError::UnknownNode)?;

    Ok(node.info.internal_url.clone())
}

pub fn get_all_volume_urls_for_key(
    ctx: &CoordinatorState,
    meta: &Meta,
) -> Result<Vec<String>, ApiError> {
    let nodes = ctx
        .nodes
        .read()
        .map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?;

    let alive_replicas: Vec<_> = meta
        .replicas
        .iter()
        .filter(|node_id| {
            nodes
                .get(*node_id)
                .map(|n| n.info.status == NodeStatus::Alive)
                .unwrap_or(false)
        })
        .collect();

    let volume_urls: Vec<Result<String, anyhow::Error>> = alive_replicas
        .iter()
        .map(|node_id| {
            let node = nodes.get(*node_id).ok_or_else(|| ApiError::UnknownNode)?;
            Ok(node.info.internal_url.clone())
        })
        .collect();

    Ok(volume_urls
        .into_iter()
        .collect::<Result<Vec<String>, anyhow::Error>>()?)
}

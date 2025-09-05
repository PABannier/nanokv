use anyhow::anyhow;
use axum::extract::{Path, State};
use serde::Serialize;

use crate::core::state::CoordinatorState;
use common::api_error::ApiError;

#[cfg(test)]
use crate::core::placement::choose_top_n_alive;
#[cfg(test)]
use common::file_utils::sanitize_key;

#[derive(Serialize)]
pub struct PlacementResponse {
    pub replicas: Vec<String>,
}

/// GET /debug/placement/:key → {"replicas": ["nodeA","nodeB","nodeC"]} in HRW order.
/// 
/// This endpoint is only available in test builds and shows the deterministic
/// placement order for a given key according to HRW (Highest Random Weight) hashing.
#[cfg(test)]
pub async fn debug_placement(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
) -> Result<axum::Json<PlacementResponse>, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    
    // Get the replicas in HRW order (same logic as PUT)
    let nodes = ctx.nodes.read()
        .map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?
        .iter()
        .map(|(_,n)| n.info.clone())
        .collect::<Vec<_>>();
    let replicas = choose_top_n_alive(&nodes, &key_enc, ctx.n_replicas);
    
    let node_ids: Vec<String> = replicas.iter()
        .map(|replica| replica.node_id.clone())
        .collect();
    
    let response = PlacementResponse {
        replicas: node_ids,
    };
    
    Ok(axum::Json(response))
}

#[cfg(not(test))]
pub async fn debug_placement(
    _path: Path<String>,
    _state: State<CoordinatorState>,
) -> Result<axum::Json<PlacementResponse>, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!("Debug endpoints only available in test builds")))
}

use std::time::Instant;
use uuid::Uuid;
use axum::{
    body::Body,
    extract::{Path, State, Json},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use anyhow::anyhow;
use futures_util::TryStreamExt;

use common::schemas::{JoinRequest, HeartbeatRequest, PutResponse};
use common::time_utils::utc_now_ms;
use common::constants::NODE_KEY_PREFIX;
use common::api_error::ApiError;
use common::url_utils::sanitize_url;
use common::file_utils::{
    parse_content_length,
    sanitize_key,
    meta_key_for,
};

use crate::node::{NodeInfo, NodeStatus, NodeRuntime};
use crate::state::CoordinatorState;
use crate::meta::{Meta, TxState};
use crate::placement::{alive_nodes_for_placement, rank_nodes};


// PUT /:key
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    // Enforce write-once: conflict if Committed or Pending already exists
    if let Some(existing) = ctx.db.get::<Meta>(&meta_key)? {
        match existing.state {
            TxState::Committed => return Err(ApiError::KeyAlreadyExists),
            TxState::Pending => return Err(ApiError::KeyAlreadyExists),
            TxState::Tombstoned => return Err(ApiError::KeyAlreadyExists),
        }
    }

    // Optional content-length check (helps enforce max_size early)
    let content_length = parse_content_length(&headers);
    if let Some(len) = content_length {
        if len > ctx.max_size {
            return Err(ApiError::TooLarge);
        }
    }

    // Select a node for placement
    let alive_nodes = alive_nodes_for_placement(&ctx)?;

    let ranked_nodes = rank_nodes(&key_enc, &alive_nodes);
    let node = ranked_nodes.first().ok_or_else(|| ApiError::Any(anyhow!("no nodes available for placement")))?;

    // Declare an inflight upload
    let _permit = ctx.inflight.acquire().await.unwrap();
    let upload_id = Uuid::new_v4().to_string();

    // Record Pending in RocksDB
    ctx.db.put(&meta_key, &Meta::pending(upload_id.clone(), vec![node.internal_url.clone()]))?;

    // Stream to the volume node
    let stream = body
        .into_data_stream()
        .map_err(|e| ApiError::Any(anyhow!("failed to stream to node: {}", e)));
    let upstream_body = reqwest::Body::wrap_stream(stream);

    let vol_url = format!("{}/internal/object/{}", node.internal_url, key_enc);

    let mut req = ctx.http_client.put(&vol_url).header("X-Upload-Id", &upload_id);

    if let Some(len) = content_length {
        req = req.header("Content-Length", len.to_string());
    }

    let resp = req
        .body(upstream_body)
        .send()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to stream to node: {}", e)))?;

    if !resp.status().is_success() {
        // TODO: clear pending upload
        return Err(ApiError::Any(anyhow!("failed to stream to node. Volume replied: {}", resp.status())));
    }

    let put_resp: PutResponse = resp.json::<PutResponse>().await
        .map_err(|e| ApiError::Any(anyhow!("failed to parse volume put response: {}", e)))?;

    let replicas = vec![node.node_id.clone()];
    ctx.db.put(&meta_key, &Meta::committed(put_resp.size, put_resp.etag.clone(), replicas))?;

    // Build response headers
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", put_resp.etag)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&put_resp.size.to_string()).unwrap());

    Ok((StatusCode::CREATED, resp_headers))
}

// GET /:key
pub async fn get_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>
) -> Result<impl IntoResponse, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        Some(m) if m.state == TxState::Committed => m,
        Some(m) if m.state == TxState::Tombstoned => return Err(ApiError::KeyNotFound),
        _ => return Err(ApiError::KeyNotFound),
    };

    let node_id = meta.replicas.first().ok_or_else(|| ApiError::Any(anyhow!("no replicas found")))?;
    let vol_url = {
        let nodes = ctx.nodes.read().map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?;
        let node = nodes.get(node_id).ok_or_else(|| ApiError::Any(anyhow!("node not found")))?;
        format!("{}/blobs/{}", node.info.public_url, key_enc)
    };

    let req = ctx.http_client.get(&vol_url);
    let resp = req.send().await.map_err(|e| ApiError::Any(anyhow!("failed to send request to volume: {}", e)))?;

    if !resp.status().is_success() {
        return Err(ApiError::Any(anyhow!("failed to get object. Volume replied: {}", resp.status())));
    }

    // Convert the reqwest response body to axum body stream
    let stream = resp.bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let body = Body::from_stream(stream);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&meta.size.to_string()).unwrap());

    Ok((StatusCode::OK, resp_headers, body).into_response())
}

// DELETE /:key
pub async fn delete_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>
) -> Result<StatusCode, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let existing = match ctx.db.get::<Meta>(&meta_key)? {
        None => return Err(ApiError::KeyNotFound),
        Some(m) if m.state == TxState::Tombstoned => return Ok(StatusCode::NO_CONTENT),
        Some(m) if m.state == TxState::Pending => {
            // an in-flight upload exists; be conservative and reject delete
            return Err(ApiError::Any(anyhow!("cannot delete pending upload")));
        }
        Some(m) => m,
    };

    // Remove entry from KvDB
    ctx.db.put(&meta_key, &Meta::tombstoned())?;

    // Send a request to the volume node to delete the object
    let node_id = existing.replicas.first().ok_or_else(|| ApiError::Any(anyhow!("no replicas found")))?;
    let vol_url = {
        let nodes = ctx.nodes.read().map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?;
        let node = nodes.get(node_id).ok_or_else(|| ApiError::Any(anyhow!("node not found")))?;
        format!("{}/internal/delete/{}", node.info.internal_url, key_enc)
    };

    let req = ctx.http_client.delete(&vol_url);
    let resp = req.send().await.map_err(|e| ApiError::Any(anyhow!("failed to send request to volume: {}", e)))?;

    if !resp.status().is_success() {
        return Err(ApiError::Any(anyhow!("failed to delete object. Volume replied: {}", resp.status())));
    }

    Ok(StatusCode::NO_CONTENT)
}


// HEAD /:key
pub async fn head_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        None => return Err(ApiError::KeyNotFound),
        Some(m) if m.state == TxState::Tombstoned => return Err(ApiError::KeyNotFound),
        Some(m) => m,
    };

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("State", HeaderValue::from_str(&meta.state.to_string()).unwrap());
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&meta.size.to_string()).unwrap());

    Ok((StatusCode::OK, resp_headers))
}

// GET /admin/nodes
pub async fn list_nodes(
    State(ctx): State<CoordinatorState>
) -> Result<(StatusCode, impl IntoResponse), ApiError> {
    let nodes = ctx.nodes
        .read()
        .map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?
        .iter()
        .map(|(_,v)| v.info.clone())
        .collect::<Vec<_>>();

    Ok((StatusCode::OK, axum::Json(nodes)))
}

// POST /admin/join
pub async fn join_node(
    State(ctx): State<CoordinatorState>,
    Json(req): Json<JoinRequest>,
) -> Result<StatusCode, ApiError> {
    let now_ms = utc_now_ms();

    let mut nodes = match ctx.nodes.write() {
        Ok(nodes) => nodes,
        Err(e) => return Err(ApiError::Any(anyhow!("failed to acquire nodes lock: {}", e))),
    };

    let clean_public_url = sanitize_url(&req.public_url)?;
    let clean_internal_url = sanitize_url(&req.internal_url)?;

    let node_info = NodeInfo {
        node_id: req.node_id.clone(),
        public_url: clean_public_url,
        internal_url: clean_internal_url,
        subvols: req.subvols,
        last_heartbeat_ms: now_ms,
        capacity_bytes: req.capacity_bytes,
        used_bytes: req.used_bytes,
        status: NodeStatus::Alive,
        version: req.version,
    };
    let runtime = NodeRuntime { info: node_info.clone(), last_seen: Instant::now() };

    ctx.db.put(&format!("{}:{}", NODE_KEY_PREFIX, req.node_id), &node_info)?;
    nodes.insert(req.node_id, runtime);

    Ok(StatusCode::OK)
}

// POST /admin/heartbeat
pub async fn heartbeat(
    State(ctx): State<CoordinatorState>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<StatusCode, ApiError> {
    let mut nodes = match ctx.nodes.write() {
        Ok(nodes) => nodes,
        Err(e) => return Err(ApiError::Any(anyhow!("failed to acquire nodes lock: {}", e))),
    };

    let entry = nodes.get_mut(&req.node_id).ok_or_else(|| ApiError::UnknownNode)?;

    if let Some(u) = req.used_bytes { entry.info.used_bytes = Some(u); }
    if let Some(c) = req.capacity_bytes { entry.info.capacity_bytes = Some(c); }

    entry.info.last_heartbeat_ms = utc_now_ms();
    entry.info.status = NodeStatus::Alive;
    entry.last_seen = Instant::now();

    let node_key = format!("{}:{}", NODE_KEY_PREFIX, req.node_id);
    ctx.db.put(&node_key, &entry.info)?;

    Ok(StatusCode::OK)
}

use std::time::{Instant};
use axum::{
    body::Body,
    extract::{Path, State, Json},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use futures_util::future::try_join_all;
use anyhow::anyhow;

use common::schemas::{JoinRequest, HeartbeatRequest};
use common::time_utils::utc_now_ms;
use common::constants::NODE_KEY_PREFIX;
use common::api_error::ApiError;
use common::url_utils::sanitize_url;
use common::file_utils::{
    sanitize_key,
    meta_key_for,
};
use common::file_utils::parse_content_length;

use crate::core::op::{meta, prepare, guard::AbortGuard, write, pull, commit};
use crate::core::node::{NodeInfo, NodeStatus, NodeRuntime};
use crate::core::state::CoordinatorState;
use crate::core::meta::{Meta, TxState};
use crate::core::placement::{
    choose_top_n_alive,
    get_volume_url_for_key,
    get_all_volume_urls_for_key
};


// PUT /:key
/// A PUT request to create or replace an object, orchestrated in a 2 phase commit.
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, HeaderMap), ApiError> {
    // Sanity checks
    let key_enc = sanitize_key(&raw_key)?;

    let meta_key = meta_key_for(&key_enc);
    ensure_write_once(&ctx, &meta_key)?;

    content_length_check(&headers, ctx.max_size)?;

    // Find replicas
    let replicas = choose_top_n_alive(&ctx, &key_enc, ctx.n_replicas)?;
    if replicas.len() < ctx.n_replicas { return Err(ApiError::NoQuorum); }

    // Declare an inflight upload
    let _permit = ctx.inflight.acquire().await.unwrap();

    // Write pending transaction in DB
    let upload_id = meta::write_pending_meta(&ctx, &meta_key, &replicas)?;

    // Abort guard (abort on failure if dropped before disarmed)
    let mut guard = AbortGuard::new(&ctx, &replicas, upload_id.clone());

    // Prepare all replicas with a retry
    prepare::retry_prepare_all(&ctx, &replicas, &key_enc, &upload_id).await?;

    // Write to the head node (single-shot; long timeout). If this fails, abort.
    let head = replicas.first().unwrap();
    let (size, etag) = write::write_to_head_single_shot(&ctx, head, body, &upload_id).await?;

    // Pull all from head with a retry
    pull::retry_pull_all(&ctx, head, &replicas[1..], &upload_id, size, &etag).await?;

    // At this point, all replicas have the data in temporary files. We can disarm the
    // guard. If the commit fails, a verify or clean operation will clean up the temporary
    // files.
    guard.disarm();

    // Commit
    commit::retry_commit_all(&ctx, &replicas, &upload_id, &key_enc).await?;

    // Write committed transaction in DB
    meta::write_committed_meta(&ctx, &meta_key, size, etag.clone(), &replicas)?;

    // Build response headers
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&size.to_string()).unwrap());

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

    let volume_url = get_volume_url_for_key(&ctx, &meta)?;
    let volume_url_for_key = format!("{}/blobs/{}", volume_url, key_enc);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("Location", HeaderValue::from_str(&volume_url_for_key).unwrap());
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&meta.size.to_string()).unwrap());

    Ok((StatusCode::FOUND, resp_headers).into_response())
}

// DELETE /:key
async fn delete_object_on_volume(ctx: &CoordinatorState, vol_url: &str) -> Result<StatusCode, ApiError> {
    let req = ctx.http_client.delete(vol_url);

    let res = req.send()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to send request to volume: {}", e)))?;

    if !res.status().is_success() {
        return Err(ApiError::Any(anyhow!("failed to delete object. Volume replied: {}", res.status())));
    }

    Ok(StatusCode::NO_CONTENT)
}

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

    ctx.db.put(&meta_key, &Meta::tombstoned())?;

    let volume_urls = get_all_volume_urls_for_key(&ctx, &existing)?;

    let volume_urls_for_key = volume_urls
        .iter()
        .map(|url| format!("{}/internal/delete/{}", url, key_enc))
        .collect::<Vec<_>>();

    let results = try_join_all(
        volume_urls_for_key
        .iter()
        .map(|url| delete_object_on_volume(&ctx, url))
    ).await?;

    for code in results {
        if code != StatusCode::NO_CONTENT {
            return Err(ApiError::Any(anyhow!("failed to delete object. Volume replied: {}", code)));
        }
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

/// Utility functions

fn ensure_write_once(ctx: &CoordinatorState, meta_key: &str) -> Result<(), ApiError> {
    if let Some(existing) = ctx.db.get::<Meta>(&meta_key)? {
        match existing.state {
            TxState::Committed => return Err(ApiError::KeyAlreadyExists),
            TxState::Pending => return Err(ApiError::KeyAlreadyExists),
            TxState::Tombstoned => return Err(ApiError::KeyAlreadyExists),
        }
    }

    Ok(())
}

fn content_length_check(headers: &HeaderMap, max_size: u64) -> Result<(), ApiError> {
    let content_length = parse_content_length(&headers);
    if let Some(len) = content_length {
        if len > max_size {
            return Err(ApiError::TooLarge);
        }
    }

    Ok(())
}


use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Json, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use futures_util::future::try_join_all;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};
use tracing::Instrument;

use common::constants::NODE_KEY_PREFIX;
use common::error::ApiError;
use common::file_utils::parse_content_length;
use common::key_utils::{Key, meta_key_for};
use common::schemas::{HeartbeatRequest, JoinRequest};
use common::time_utils::utc_now_ms;
use common::url_utils::sanitize_url;

use crate::core::meta::{Meta, TxState};
use crate::core::node::{NodeInfo, NodeRuntime, NodeStatus};
use crate::core::op::{commit, guard::AbortGuard, meta, prepare, pull, write};
use crate::core::placement::{
    choose_top_n_alive, get_all_volume_urls_for_key, get_volume_url_for_key,
};
use crate::core::state::CoordinatorState;

// PUT /:key
/// A PUT request to create or replace an object, orchestrated in a 2 phase commit.
#[tracing::instrument(name="coord.put", skip(body, ctx), fields(key = raw_key))]
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key = Key::from_percent_encoded(&raw_key)?;
    let key_enc = key.enc();
    let meta_key = meta_key_for(key_enc);

    // Control plane work (placement, meta, prepare)
    let control_plane_permit = ctx.control_inflight.acquire().await.unwrap();

    // Sanity checks
    {
        let _sanity_check = tracing::info_span!("sanity_check").entered();
        ensure_write_once(&ctx, &meta_key)?;
        content_length_check(&headers, ctx.max_size)?;
    }

    // Find replicas
    let replicas = {
        let _choose_placement = tracing::info_span!("choose_placement").entered();

        let nodes = ctx
            .nodes
            .read()
            .map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?
            .values()
            .map(|n| n.info.clone())
            .collect::<Vec<_>>();

        choose_top_n_alive(&nodes, key_enc, ctx.n_replicas)
    };

    if replicas.len() < ctx.n_replicas {
        return Err(ApiError::NoQuorum);
    }

    // Write pending transaction in DB
    let _write_pending_meta = tracing::info_span!("write_pending_meta").entered();
    let upload_id = meta::write_pending_meta(&ctx.db, &meta_key, &replicas)?;
    drop(_write_pending_meta);

    // Abort guard (abort on failure if dropped before disarmed)
    let mut guard = AbortGuard::new(&ctx.http_client, &replicas, upload_id.clone());

    // Prepare all replicas with a retry
    prepare::retry_prepare_all(&ctx.http_client, &replicas, key_enc, &upload_id)
        .instrument(tracing::info_span!("prepare"))
        .await?;

    drop(control_plane_permit); // Release control plane permit

    // Acquire data inflight permit (alive for the entire duration of the data moving part)
    let data_permit = ctx.data_inflight.acquire().await.unwrap();

    // Write to the head node (single-shot; long timeout). If this fails, abort.
    let head = replicas.first().unwrap();

    let head_semaphore = ctx
        .per_node_inflight
        .read()
        .unwrap()
        .get(&head.node_id)
        .ok_or(ApiError::Any(anyhow!(
            "node {} not found in per_node_inflight",
            head.node_id
        )))?
        .clone();

    let head_permit = head_semaphore
        .acquire_owned()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to acquire per-node inflight permit: {}", e)))?;

    let (size, etag) = {
        let span = tracing::info_span!("write_to_head",
            node_id = %head.node_id,
            node_url = %head.internal_url
        );
        write::write_to_head_single_shot(&ctx.http_client, head, body, &upload_id)
            .instrument(span)
            .await?
    };

    drop(head_permit);

    // Acquire per-node inflight permits for all replicas
    let per_node_permits = {
        let _queued_span = tracing::info_span!("queued_per_node_all").entered();

        // Sort replicas by node_id for deterministic acquisition order (prevents deadlocks)
        let mut sorted_replicas = replicas.clone();
        sorted_replicas.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let per_node_timeout = Duration::from_secs(ctx.per_node_timeout);

        let futures: Result<Vec<_>, ApiError> = {
            let per_node_inflight = ctx.per_node_inflight.read().unwrap();
            sorted_replicas
                .iter()
                .map(|r| {
                    let semaphore = per_node_inflight
                        .get(&r.node_id)
                        .ok_or_else(|| {
                            ApiError::Any(anyhow!(
                                "node {} not found in per_node_inflight",
                                r.node_id
                            ))
                        })?
                        .clone();

                    // Wrap each acquisition in a timeout
                    Ok(async move {
                        timeout(per_node_timeout, semaphore.acquire_owned())
                            .await
                            .map_err(|_| ApiError::ServiceUnavailable {
                                retry_after: Some(5),
                                message: format!("Timeout acquiring permit for node {}", r.node_id),
                            })
                    })
                })
                .collect()
        };

        drop(_queued_span); // Drop span before await

        try_join_all(futures?).await?
    };

    // Pull all from head with a retry
    pull::retry_pull_all(
        &ctx.http_client,
        head,
        &replicas[1..],
        &upload_id,
        size,
        &etag,
    )
    .instrument(tracing::info_span!("pull"))
    .await?;

    for permit in per_node_permits {
        drop(permit);
    }
    drop(data_permit);

    // At this point, all replicas have the data in temporary files. We can disarm the
    // guard. If the commit fails, a verify or clean operation will clean up the temporary
    // files.
    guard.disarm();

    let commit_permit = ctx.control_inflight.acquire().await.unwrap();

    // Commit
    commit::retry_commit_all(&ctx.http_client, &replicas, &upload_id, key_enc)
        .instrument(tracing::info_span!("commit"))
        .await?;

    // Write committed transaction in DB
    meta::write_committed_meta(&ctx.db, &meta_key, size, etag.clone(), &replicas)?;

    drop(commit_permit);

    // Build response headers
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        "ETag",
        HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap(),
    );
    resp_headers.insert(
        "Content-Length",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );

    Ok((StatusCode::CREATED, resp_headers))
}

// GET /:key
#[tracing::instrument(name="coord.get", skip(ctx), fields(key = raw_key))]
pub async fn get_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
) -> Result<impl IntoResponse, ApiError> {
    let key = Key::from_percent_encoded(&raw_key)?;
    let key_enc = key.enc();
    let meta_key = meta_key_for(key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        Some(m) if m.state == TxState::Committed => m,
        Some(m) if m.state == TxState::Tombstoned => return Err(ApiError::KeyNotFound),
        _ => return Err(ApiError::KeyNotFound),
    };

    let volume_url = get_volume_url_for_key(&ctx, &meta)?;
    let volume_url_for_key = format!("{}/blobs/{}", volume_url, key_enc);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        "Location",
        HeaderValue::from_str(&volume_url_for_key).unwrap(),
    );
    resp_headers.insert(
        "ETag",
        HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap(),
    );
    resp_headers.insert(
        "Content-Length",
        HeaderValue::from_str(&meta.size.to_string()).unwrap(),
    );

    Ok((StatusCode::FOUND, resp_headers).into_response())
}

// DELETE /:key
async fn delete_object_on_volume(
    ctx: &CoordinatorState,
    vol_url: &str,
) -> Result<StatusCode, ApiError> {
    let req = ctx.http_client.delete(vol_url);

    let res = req
        .send()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to send request to volume: {}", e)))?;

    if !res.status().is_success() {
        return Err(ApiError::Any(anyhow!(
            "failed to delete object. Volume replied: {}",
            res.status()
        )));
    }

    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(name="coord.delete", skip(ctx), fields(key = raw_key))]
pub async fn delete_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
) -> Result<StatusCode, ApiError> {
    let key = Key::from_percent_encoded(&raw_key)?;
    let key_enc = key.enc();
    let meta_key = meta_key_for(key_enc);

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
            .map(|url| delete_object_on_volume(&ctx, url)),
    )
    .await?;

    for code in results {
        if code != StatusCode::NO_CONTENT {
            return Err(ApiError::Any(anyhow!(
                "failed to delete object. Volume replied: {}",
                code
            )));
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// HEAD /:key
#[tracing::instrument(name="coord.head", skip(ctx), fields(key = raw_key))]
pub async fn head_object(
    Path(raw_key): Path<String>,
    State(ctx): State<CoordinatorState>,
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key = Key::from_percent_encoded(&raw_key)?;
    let key_enc = key.enc();
    let meta_key = meta_key_for(key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        None => return Err(ApiError::KeyNotFound),
        Some(m) if m.state == TxState::Tombstoned => return Err(ApiError::KeyNotFound),
        Some(m) => m,
    };

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        "State",
        HeaderValue::from_str(&meta.state.to_string()).unwrap(),
    );
    resp_headers.insert(
        "ETag",
        HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap(),
    );
    resp_headers.insert(
        "Content-Length",
        HeaderValue::from_str(&meta.size.to_string()).unwrap(),
    );

    Ok((StatusCode::OK, resp_headers))
}

// GET /admin/nodes
#[tracing::instrument(name="coord.admin.list_nodes", skip(ctx))]
pub async fn list_nodes(
    State(ctx): State<CoordinatorState>,
) -> Result<(StatusCode, impl IntoResponse), ApiError> {
    let nodes = ctx
        .nodes
        .read()
        .map_err(|e| ApiError::Any(anyhow!("failed to acquire nodes read lock: {}", e)))?
        .values()
        .map(|v| v.info.clone())
        .collect::<Vec<_>>();

    Ok((StatusCode::OK, axum::Json(nodes)))
}

// POST /admin/join
#[tracing::instrument(name="coord.admin.join_node", skip(ctx), fields(node_id = %req.node_id, public_url = %req.public_url, internal_url = %req.internal_url))]
pub async fn join_node(
    State(ctx): State<CoordinatorState>,
    Json(req): Json<JoinRequest>,
) -> Result<StatusCode, ApiError> {
    let now_ms = utc_now_ms();

    let mut nodes = match ctx.nodes.write() {
        Ok(nodes) => nodes,
        Err(e) => {
            return Err(ApiError::Any(anyhow!(
                "failed to acquire nodes lock: {}",
                e
            )));
        }
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

    let runtime = NodeRuntime {
        info: node_info.clone(),
        last_seen: Instant::now(),
    };

    ctx.db
        .put(&format!("{}:{}", NODE_KEY_PREFIX, req.node_id), &node_info)?;

    nodes.insert(req.node_id.clone(), runtime);

    ctx.per_node_inflight.write().unwrap().insert(
        req.node_id,
        Arc::new(Semaphore::new(ctx.max_per_node_inflight)),
    );

    Ok(StatusCode::OK)
}

// POST /admin/heartbeat
#[tracing::instrument(name="coord.admin.heartbeat", skip(ctx), fields(node_id = %req.node_id))]
pub async fn heartbeat(
    State(ctx): State<CoordinatorState>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<StatusCode, ApiError> {
    let mut nodes = match ctx.nodes.write() {
        Ok(nodes) => nodes,
        Err(e) => {
            return Err(ApiError::Any(anyhow!(
                "failed to acquire nodes lock: {}",
                e
            )));
        }
    };

    let entry = nodes
        .get_mut(&req.node_id)
        .ok_or_else(|| ApiError::UnknownNode)?;

    if let Some(u) = req.used_bytes {
        entry.info.used_bytes = Some(u);
    }
    if let Some(c) = req.capacity_bytes {
        entry.info.capacity_bytes = Some(c);
    }

    entry.info.last_heartbeat_ms = utc_now_ms();
    entry.info.status = NodeStatus::Alive;
    entry.last_seen = Instant::now();

    let node_key = format!("{}:{}", NODE_KEY_PREFIX, req.node_id);
    ctx.db.put(&node_key, &entry.info)?;

    Ok(StatusCode::OK)
}

fn ensure_write_once(ctx: &CoordinatorState, meta_key: &str) -> Result<(), ApiError> {
    if let Some(existing) = ctx.db.get::<Meta>(meta_key)? {
        match existing.state {
            TxState::Committed => return Err(ApiError::KeyAlreadyExists),
            TxState::Pending => return Err(ApiError::KeyAlreadyExists),
            TxState::Tombstoned => return Err(ApiError::KeyAlreadyExists),
        }
    }

    Ok(())
}

fn content_length_check(headers: &HeaderMap, max_size: u64) -> Result<(), ApiError> {
    let content_length = parse_content_length(headers);
    if let Some(len) = content_length
        && len > max_size
    {
        return Err(ApiError::TooLarge);
    }

    Ok(())
}

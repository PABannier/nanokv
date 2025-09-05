use walkdir::WalkDir;
use std::{cmp::Ordering, time::{Duration, SystemTime}};
use tokio::{fs::{self, File, OpenOptions}};
use tokio_util::io::ReaderStream;
use serde::{Deserialize};
use axum::{
    body::Body,
    extract::{Path, State, Query, Json},
    http::StatusCode,
    response::IntoResponse,
};

use crate::state::VolumeState;
use crate::replicate::{PullRequest, CommitRequest, pull_from_head};
use common::{api_error::ApiError, constants::{BLOB_DIR_NAME, TMP_DIR_NAME}};
use common::schemas::{PutResponse, ListResponse, BlobHead, SweepTmpQuery, SweepTmpResponse};
use common::file_utils::{
    sanitize_key,
    fsync_dir,
    blob_path,
    tmp_path,
    stream_to_file_with_hash,
    file_hash,
    file_exists,
};

#[derive(Deserialize, Debug)]
pub struct PrepareRequest {
    pub key: String,  // Key is already encoded
    pub upload_id: String,
}

// POST /internal/prepare?key=?upload_id=
pub async fn prepare_handler(
    Query(req): Query<PrepareRequest>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    if ctx.fault_injector.should_fail_prepare() {
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: prepare failed")));
    }

    let final_path = blob_path(&ctx.data_root, &req.key);

    // Write-once at volume level (defensive)
    if file_exists(&final_path).await {
        return Err(ApiError::KeyAlreadyExists);
    }

    // Ensure tmp dir does not exist
    let tmp_path = tmp_path(&ctx.data_root, &req.upload_id);
    if file_exists(&tmp_path).await {
        return Err(ApiError::TmpDirExists);
    }

    // Create parent dirs
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Open temp file handle for streaming
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    Ok(StatusCode::OK)
}

// PUT /:upload_id (head only)
pub async fn write_handler(
    Path(upload_id): Path<String>,
    State(ctx): State<VolumeState>,
    body: Body,
) -> Result<(StatusCode, axum::Json<PutResponse>), ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    let tmp_path = tmp_path(&ctx.data_root, &upload_id);

    if !file_exists(&tmp_path).await {
        return Err(ApiError::TmpDirNotFound);
    }

    // Open temp file handle for streaming
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    // Stream to temp file, get size and etag
    let (size, etag) = stream_to_file_with_hash(body.into_data_stream(), &mut tmp_file).await?;
    tmp_file.sync_all().await?;  // make sure all buffered data is dumped onto disk (durable temp)

    // Build response headers
    let resp = PutResponse { etag, size };
    Ok((StatusCode::CREATED, axum::Json(resp)))
}

// GET /:upload_id (head only)
pub async fn read_handler(
    Path(upload_id): Path<String>,
    State(ctx): State<VolumeState>,
) -> Result<impl IntoResponse, ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    if ctx.fault_injector.should_fail_read_tmp() {
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: read tmp failed")));
    }

    let tmp_path = tmp_path(&ctx.data_root, &upload_id);

    if !file_exists(&tmp_path).await {
        return Err(ApiError::TmpDirNotFound);
    }

    let file = File::open(&tmp_path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok((StatusCode::OK, body).into_response())
}

// POST /pull?upload_id=?from= (follower only)
pub async fn pull_handler(
    Query(req): Query<PullRequest>,
    State(ctx): State<VolumeState>,
) -> Result<(StatusCode, Json<PutResponse>), ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    if ctx.fault_injector.should_fail_pull() {
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: pull failed")));
    }

    let tmp_path = tmp_path(&ctx.data_root, &req.upload_id);

    if !file_exists(&tmp_path).await {
        return Err(ApiError::TmpDirNotFound);
    }

    // Open temp file handle for streaming
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    // Check for mid-stream failure
    if ctx.fault_injector.should_fail_pull_mid_stream() {
        // Start the pull but fail partway through
        let _ = pull_from_head(&ctx, &req.from, &mut tmp_file).await;
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: pull failed mid-stream")));
    }

    // Make a request to the head node and stream the response to file
    let (size, etag) = pull_from_head(&ctx, &req.from, &mut tmp_file).await?;

    // Check for etag mismatch fault injection
    let final_etag = if ctx.fault_injector.should_fail_etag_mismatch() {
        "fault_injected_wrong_etag".to_string()
    } else {
        etag
    };

    if size != req.expected_size || final_etag != req.expected_etag {
        return Err(ApiError::ChecksumMismatch);
    }

    let resp = PutResponse { etag: final_etag, size };
    Ok((StatusCode::CREATED, axum::Json(resp)))
}


// POST /commit?upload_id=?key=
pub async fn commit_handler(
    Query(req): Query<CommitRequest>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    if ctx.fault_injector.should_fail_commit() {
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: commit failed")));
    }

    if ctx.fault_injector.should_timeout_commit() {
        // Simulate a long timeout
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        return Err(ApiError::Any(anyhow::anyhow!("Fault injection: commit timed out")));
    }

    let tmp_path = tmp_path(&ctx.data_root, &req.upload_id);
    let final_path = blob_path(&ctx.data_root, &req.key);

    if !file_exists(&tmp_path).await {
        return Err(ApiError::TmpDirNotFound);
    }

    if file_exists(&final_path).await {
        return Err(ApiError::KeyAlreadyExists);
    }

    let final_dir = final_path.parent().unwrap();
    fs::create_dir_all(final_dir).await?;
    fs::rename(&tmp_path, &final_path).await?;

    fsync_dir(final_dir).await?;

    Ok(StatusCode::OK)
}

// POST /abort?upload_id=
#[derive(Deserialize, Debug)]
pub struct AbortRequest {
    pub upload_id: String,
}

pub async fn abort_handler(
    Query(req): Query<AbortRequest>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    // Fault injection checks
    ctx.fault_injector.check_killed()?;
    ctx.fault_injector.wait_if_paused().await;
    ctx.fault_injector.apply_latency().await;

    let tmp_path = tmp_path(&ctx.data_root, &req.upload_id);
    fs::remove_file(&tmp_path).await?;
    Ok(StatusCode::OK)
}


// GET /:key
pub async fn get_handler(
    Path(raw_key): Path<String>,
    State(ctx): State<VolumeState>
) -> Result<impl IntoResponse, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let path = blob_path(&ctx.data_root, &key_enc);

    if !file_exists(&path).await {
        return Err(ApiError::KeyNotFound);
    }

    let file = File::open(&path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok((StatusCode::OK, body).into_response())
}


// DELETE /:key
pub async fn delete_handler(
    Path(raw_key): Path<String>,
    State(ctx): State<VolumeState>
) -> Result<StatusCode, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let path = blob_path(&ctx.data_root, &key_enc);

    fs::remove_file(&path).await?;

    Ok(StatusCode::NO_CONTENT)
}

// GET /admin/list?limit=?after=?
#[derive(Deserialize)]
pub struct ListRequest {
    pub limit: Option<usize>,
    pub after: Option<String>,  // percent-encoded cursor (exclusive)
}


const DEFAULT_PAGE_LIMIT: usize = 1000;
const MAX_PAGE_LIMIT: usize = 5000;

pub async fn admin_list_handler(
    State(ctx): State<VolumeState>,
    Query(req): Query<ListRequest>,
) -> Result<Json<ListResponse>, ApiError> {
    let limit = req.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
    let after_enc = req.after.unwrap_or_default();

    // Walk blobs/aa/bb/<key>, flatten into encoded keys
    // TODO: This is O(n) in the number of keys, for large store, we should consider a
    // small index.
    let root = ctx.data_root.join(BLOB_DIR_NAME);
    let mut keys: Vec<String> = Vec::with_capacity(limit + 1);
    let mut seen_after = after_enc.is_empty();

    for entry in WalkDir::new(&root).min_depth(3).max_depth(3) {
        let entry = match entry {
            Ok(e) if e.file_type().is_file() => e,
            _ => continue,
        };
        let key_enc = entry.file_name().to_string_lossy().to_string();

        // cursor (exclusive)
        if !after_enc.is_empty() && !seen_after {
            match key_enc.as_str().cmp(after_enc.as_str()) {
                Ordering::Less | Ordering::Equal => continue,
                Ordering::Greater => seen_after = true,
            }
        }

        keys.push(key_enc);
        if keys.len() == limit {
            break;
        }
    }

    let next_after = keys.last().cloned();
    Ok(Json(ListResponse { keys, next_after }))
}


// GET /admin/blob?key=...&deep=...
#[derive(Deserialize)]
pub struct BlobRequest { key: String, deep: Option<bool> }


pub async fn admin_blob_handler(
    State(ctx): State<VolumeState>,
    Query(req): Query<BlobRequest>,
) -> Result<Json<BlobHead>, ApiError> {
    let key_enc = sanitize_key(&req.key)?;
    let path = blob_path(&ctx.data_root, &key_enc);

    let exists = file_exists(&path).await;

    if !exists {
        return Ok(Json(BlobHead { exists, size: 0, etag: None }));
    }

    let size = fs::metadata(&path).await?.len();
    let deep = req.deep.unwrap_or(false);
    let etag = if deep {
        Some(file_hash(&path).await?)
    } else {
        None
    };

    Ok(Json(BlobHead { exists: true, size, etag }))
}

// POST /admin/sweep-tmp?sweep_age_secs=
pub async fn admin_sweep_tmp_handler(
    State(ctx): State<VolumeState>,
    Query(req): Query<SweepTmpQuery>,
) -> Result<Json<SweepTmpResponse>, ApiError> {
    let root = ctx.data_root.join(TMP_DIR_NAME);
    let now = SystemTime::now();
    let safe_age = Duration::from_secs(req.sweep_age_secs.unwrap_or(3600));
    let cutoff = now.checked_sub(safe_age).unwrap_or(now);

    let mut removed = 0u64;
    let mut kept = 0u64;

    if root.exists() {
        for entry in WalkDir::new(&root).min_depth(1).max_depth(1) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            if !entry.file_type().is_file() { continue; }
            let p = entry.into_path();
            let meta = match fs::metadata(&p).await { Ok(m) => m, Err(_) => continue };
            let mt = meta.modified().unwrap_or(now);
            if mt <= cutoff {
                let _ = tokio::fs::remove_file(&p).await;
                removed += 1;
            } else {
                kept += 1;
            }
        }
    }

    Ok(Json(SweepTmpResponse { removed, kept_recent: kept }))
}
use tokio::{
    fs::{self, File, OpenOptions}
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use anyhow::anyhow;

use crate::{error::ApiError, meta::{self, Meta, TxState}};
use crate::state::AppState;
use crate::file_utils::{
    parse_content_length,
    sanitize_key,
    blob_path,
    tmp_path,
    fsync_dir,
    stream_to_file_with_hash,
    file_exists,
    meta_key_for,
};


// PUT /:key
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>,
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
    if let Some(len) = parse_content_length(&headers) {
        if len > ctx.max_size {
            return Err(ApiError::TooLarge);
        }
    }

    // Single-file upload, streamed -> tmp, then atomic rename to final path
    let _permit = ctx.inflight.acquire().await.unwrap();
    let upload_id = Uuid::new_v4().to_string();

    // Record Pending in RocksDB
    ctx.db.put(&meta_key, &Meta::pending(upload_id.clone()))?;

    // Ensure parent dirs exist
    let tmp_path = tmp_path(&ctx.data_root, &upload_id);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    let (size, etag_hex) = stream_to_file_with_hash(body, &mut tmp_file, ctx.max_size).await?;
    tmp_file.sync_all().await?;  // make sure all buffered data is dumped onto disk (durable temp)

    // Ensure final dir exists
    let final_path = blob_path(&ctx.data_root, &key_enc);
    let final_dir = final_path.parent().unwrap();
    fs::create_dir_all(final_dir).await?;

    // Atomic rename then fsync the directory to persist the rename
    fs::rename(&tmp_path, &final_path).await?;
    fsync_dir(final_dir).await?;

    ctx.db.put(&meta_key, &Meta::committed(size, etag_hex.clone()))?;

    // Build response headers
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&size.to_string()).unwrap());

    Ok((StatusCode::CREATED, resp_headers))
}

// GET /:key
pub async fn get_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>
) -> Result<impl IntoResponse, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        Some(m) if m.state == TxState::Committed => m,
        Some(m) if m.state == TxState::Tombstoned => return Err(ApiError::KeyNotFound),
        _ => return Err(ApiError::KeyNotFound),
    };

    let path = blob_path(&ctx.data_root, &key_enc);
    if !file_exists(&path).await {
        return Err(ApiError::KeyNotFound);
    }

    let file = File::open(&path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&meta.size.to_string()).unwrap());

    Ok((StatusCode::OK, resp_headers, body).into_response())
}

// DELETE /:key
pub async fn delete_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>
) -> Result<StatusCode, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let existing = ctx.db.get::<Meta>(&meta_key)?;
    match existing {
        None => return Err(ApiError::KeyNotFound),
        Some(m) if m.state == TxState::Tombstoned => return Ok(StatusCode::NO_CONTENT),
        Some(m) if m.state == TxState::Pending => {
            // an in-flight upload exists; be conservative and reject delete
            return Err(ApiError::Any(anyhow!("cannot delete pending upload")));
        }
        Some(_m) => {}
    }

    ctx.db.put(&meta_key, &Meta::tombstoned())?;

    let path = blob_path(&ctx.data_root, &key_enc);
    fs::remove_file(&path).await?;

    Ok(StatusCode::NO_CONTENT)
}

// HEAD /:key
pub async fn head_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let meta_key = meta_key_for(&key_enc);

    let meta = match ctx.db.get::<Meta>(&meta_key)? {
        None => return Err(ApiError::KeyNotFound),
        Some(m) => m,
    };

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("State", HeaderValue::from_str(&meta.state.to_string()).unwrap());
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", &meta.etag_hex)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&meta.size.to_string()).unwrap());

    Ok((StatusCode::OK, resp_headers))
}

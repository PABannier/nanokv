use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncSeekExt,
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};

use crate::error::ApiError;
use crate::state::AppState;
use crate::file_utils::{
    parse_content_length,
    sanitize_key,
    blob_path,
    tmp_path,
    fsync_dir, 
    stream_to_file_with_hash,
    file_exists,
    hash_file,
};

// PUT /:key
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, HeaderMap), ApiError> {
    let key = sanitize_key(&raw_key)?;

    // Optional content-length check (helps enforce max_size early)
    if let Some(len) = parse_content_length(&headers) {
        if len > ctx.max_size {
            return Err(ApiError::TooLarge);
        }
    }

    // Enfore write-once by refusing to overwrite an existing blob
    let final_path = blob_path(&ctx.data_root, &key);
    if file_exists(&final_path).await {
        return Err(ApiError::KeyAlreadyExists);
    }

    // Single-file upload, streamed -> tmp, then atomic rename to final path
    let _permit = ctx.inflight.acquire().await.unwrap();
    let upload_id = Uuid::new_v4().to_string();
    let tmp_path = tmp_path(&ctx.data_root, &upload_id);

    // Ensure parent dirs exist
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    let (size, etag) = stream_to_file_with_hash(body, &mut tmp_file, ctx.max_size).await?;
    tmp_file.sync_all().await?;  // make sure all buffered data is dumped onto disk (durable temp)

    // Ensure final dir exists
    let final_dir = final_path.parent().unwrap();
    fs::create_dir_all(final_dir).await?;

    // Atomic rename then fsync the directory to persist the rename
    fs::rename(&tmp_path, &final_path).await?;
    fsync_dir(final_dir).await?;

    // TODO: where is the write to RocksDB?

    // Build response headers
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&size.to_string()).unwrap());

    Ok((StatusCode::CREATED, resp_headers))
}

// GET /:key
pub async fn get_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>
) -> Result<impl IntoResponse, ApiError> {
    let key = sanitize_key(&raw_key)?;
    let path = blob_path(&ctx.data_root, &key);

    if !file_exists(&path).await {
        return Err(ApiError::KeyNotFound);
    }

    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    // TODO: for now we compute the ETag on the fly. For very large files, you might want
    // to store this when writing or stream it separately.
    let etag = hash_file(&mut file).await?;
    file.rewind().await?;

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("ETag", HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap());
    resp_headers.insert("Content-Length", HeaderValue::from_str(&size.to_string()).unwrap());

    Ok((StatusCode::OK, resp_headers, body).into_response())
}

// DELETE /:key
pub async fn delete_object(
    Path(raw_key): Path<String>,
    State(ctx): State<AppState>
) -> Result<StatusCode, ApiError> {
    let key = sanitize_key(&raw_key)?;
    let path = blob_path(&ctx.data_root, &key);

    if !file_exists(&path).await {
        return Err(ApiError::KeyNotFound);
    }

    // Simple direct unlink in VO (TODO: tombstone)
    fs::remove_file(&path).await?;

    Ok(StatusCode::NO_CONTENT)
}
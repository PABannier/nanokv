use tokio::{
    fs::{self, File, OpenOptions}
};
use uuid::Uuid;
use tokio_util::io::ReaderStream;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::state::VolumeState;
use common::api_error::ApiError;
use common::schemas::PutResponse;
use common::file_utils::{
    sanitize_key,
    blob_path,
    tmp_path,
    fsync_dir,
    stream_to_file_with_hash,
    file_exists,
};

const UPLOAD_ID_HEADER_KEY: &str = "X-Upload-Id";

// PUT /:key
pub async fn put_object(
    Path(raw_key): Path<String>,
    State(ctx): State<VolumeState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, axum::Json<PutResponse>), ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let final_path = blob_path(&ctx.data_root, &key_enc);

    // Write-once at volume level (defensive)
    if file_exists(&final_path).await {
        return Err(ApiError::KeyAlreadyExists);
    }

    // Get upload-id from headers (optional)
    let upload_id = headers
        .get(UPLOAD_ID_HEADER_KEY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Ensure parent dirs exist
    let tmp_path = tmp_path(&ctx.data_root, &upload_id);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Open temp file handle for streaming
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    // Stream to temp file, get size and etag
    let (size, etag) = stream_to_file_with_hash(body, &mut tmp_file).await?;
    tmp_file.sync_all().await?;  // make sure all buffered data is dumped onto disk (durable temp)

    // Commit the file
    let final_dir = final_path.parent().unwrap();
    fs::create_dir_all(final_dir).await?;
    fs::rename(&tmp_path, &final_path).await?;
    fsync_dir(final_dir).await?;

    // Build response headers
    let resp = PutResponse { etag, size };
    Ok((StatusCode::CREATED, axum::Json(resp)))
}


// GET /:key
pub async fn get_object(
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
pub async fn delete_object(
    Path(raw_key): Path<String>,
    State(ctx): State<VolumeState>
) -> Result<StatusCode, ApiError> {
    let key_enc = sanitize_key(&raw_key)?;
    let path = blob_path(&ctx.data_root, &key_enc);

    fs::remove_file(&path).await?;

    Ok(StatusCode::NO_CONTENT)
}

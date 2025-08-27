use std::{
    io,
    path::{Path, PathBuf},
};
use bytes::Bytes;
use tokio::fs::File;
use axum::body::Body;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use axum::http::HeaderMap;
use tracing::error;
use blake3;
use anyhow;
use percent_encoding::{self, percent_encode, NON_ALPHANUMERIC};
use futures_util::StreamExt;

use crate::error::ApiError;

const MAX_KEY_LEN: usize = 2048;

const BLOB_DIR_NAME: &str = "blobs";
const TMP_DIR_NAME: &str = "tmp";
const GC_DIR_NAME: &str = "gc";


pub fn parse_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

pub fn sanitize_key(raw: &str) -> Result<String, ApiError> {
    if raw.is_empty() || raw.len() > MAX_KEY_LEN {
        return Err(ApiError::Any(anyhow::anyhow!("invalid key length")));
    }

    if raw.contains("..") {
        return Err(ApiError::Any(anyhow::anyhow!("invalid key")));
    }

    // Percent-encode to keep filenames safe
    let enc = percent_encode(raw.as_bytes(), NON_ALPHANUMERIC).to_string();
    Ok(enc)
}

fn shard_dirs(key: &str) -> (String, String) {
    let hash = blake3::hash(key.as_bytes());
    let bytes = hash.as_bytes();
    (format!("{:02x}", bytes[0]), format!("{:02x}", bytes[1]))
}

pub fn blob_path(root: &Path, key: &str) -> PathBuf {
    let (a, b) = shard_dirs(key);
    root.join(BLOB_DIR_NAME).join(a).join(b).join(key)
}

pub fn tmp_path(root: &Path, upload_id: &str) -> PathBuf {
    root.join(TMP_DIR_NAME).join(upload_id)
}

pub async fn init_dirs(root: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(root.join(BLOB_DIR_NAME)).await?;
    fs::create_dir_all(root.join(TMP_DIR_NAME)).await?;
    fs::create_dir_all(root.join(GC_DIR_NAME)).await?;

    Ok(())
}

pub async fn file_exists(path: &Path) -> bool {
    fs::metadata(path).await.map(|m| m.is_file()).unwrap_or(false)
}

pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    let dirf = std::fs::File::open(dir)?;
    dirf.sync_all()?;
    Ok(())
}

pub async fn stream_to_file_with_hash(
    body: Body,
    file: &mut File,
    max_size: u64,
) -> Result<(u64, String), ApiError> {
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;

    let mut stream = body.into_data_stream();

    while let Some(next) = stream.next().await {
        let chunk: Bytes = next.map_err(|e| {
            error!("body error: {e}");
            ApiError::Any(anyhow::anyhow!("bad request body"))
        })?;
        
        total = total
            .checked_add(chunk.len() as u64)
            .ok_or(ApiError::TooLarge)?;

        if total > max_size {
            return Err(ApiError::TooLarge);
        }

        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }

    file.flush().await?;
    
    let etag = hasher.finalize().to_hex().to_string();

    Ok((total, etag))
}

pub async fn hash_file(file: &mut File) -> Result<String, ApiError> {
    file.rewind().await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[0..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
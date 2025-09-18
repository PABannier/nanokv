use anyhow;
use axum::http::HeaderMap;
use blake3;
use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::{fs, io::AsyncWriteExt};
use tracing::error;

use crate::constants::{BLOB_DIR_NAME, GC_DIR_NAME, TMP_DIR_NAME};
use crate::error::ApiError;

pub fn parse_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
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
    fs::metadata(path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
}

pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    let dirf = std::fs::File::open(dir)?;
    dirf.sync_all()?;
    Ok(())
}

pub async fn file_hash(path: &Path) -> io::Result<String> {
    let mut f = File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub async fn stream_to_file_with_hash<S, E>(
    mut stream: S,
    file: &mut File,
) -> Result<(u64, String), ApiError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;

    // Use a larger buffer for better performance
    const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);

    while let Some(next) = stream.next().await {
        let chunk: Bytes = next.map_err(|e| {
            error!("stream error: {e}");
            ApiError::Any(anyhow::anyhow!("stream error"))
        })?;

        total = total
            .checked_add(chunk.len() as u64)
            .ok_or(ApiError::TooLarge)?;

        hasher.update(&chunk);

        // Buffer chunks to reduce write syscalls
        buffer.extend_from_slice(&chunk);

        // Write buffer when it's full or chunk is large
        if buffer.len() >= BUFFER_SIZE || chunk.len() >= BUFFER_SIZE / 2 {
            file.write_all(&buffer).await?;
            buffer.clear();
        }
    }

    // Write remaining buffered data
    if !buffer.is_empty() {
        file.write_all(&buffer).await?;
    }

    // Single flush at the end instead of per-chunk
    file.flush().await?;

    let etag = hasher.finalize().to_hex().to_string();

    Ok((total, etag))
}

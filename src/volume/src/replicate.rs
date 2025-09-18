use anyhow::anyhow;
use serde::Deserialize;
use tokio::fs::File;

use common::error::ApiError;
use common::file_utils::stream_to_file_with_hash;

use crate::state::{DurabilityLevel, VolumeState};

#[derive(Deserialize, Debug)]
pub struct PullRequest {
    pub upload_id: String,
    pub from: String,
    pub expected_size: u64,
    pub expected_etag: String,
}

#[derive(Deserialize, Debug)]
pub struct CommitRequest {
    pub upload_id: String,
    pub key: String,
}

pub async fn pull_from_head(
    ctx: &VolumeState,
    from: &str,
    tmp_file: &mut File,
) -> Result<(u64, String), ApiError> {
    let req = ctx.http_client.get(from);
    let resp = req
        .send()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to pull from head: {}", e)))?;

    if !resp.status().is_success() {
        return Err(ApiError::Any(anyhow!(
            "failed to pull from head: {}",
            resp.status()
        )));
    }

    let stream = resp.bytes_stream();
    let (size, etag) = stream_to_file_with_hash(stream, tmp_file).await?;

    // Conditional sync based on durability level
    match ctx.durability_level {
        DurabilityLevel::Immediate => {
            tmp_file.sync_all().await?;
        }
        DurabilityLevel::OS => {
            // Skip sync, rely on OS
        }
    }

    Ok((size, etag))
}

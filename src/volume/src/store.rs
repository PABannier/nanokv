use nix::sys::statvfs::statvfs;
use std::path::Path;

use crate::state::DurabilityLevel;
use common::error::ApiError;
use common::file_utils::fsync_dir;
use tokio::fs::File;

pub fn disk_usage(root: &Path) -> anyhow::Result<(Option<u64>, Option<u64>)> {
    let v = statvfs(root)?;
    let cap = v.blocks() as u64 * v.fragment_size() as u64;
    let free = v.blocks_available() as u64 * v.fragment_size() as u64;
    let used = cap.saturating_sub(free);
    Ok((Some(used), Some(cap)))
}

pub async fn conditional_sync_file(
    file: &mut File,
    durability: &DurabilityLevel,
) -> Result<(), ApiError> {
    match durability {
        DurabilityLevel::Immediate => {
            file.sync_all().await?;
        }
        DurabilityLevel::OS => {
            // Skip sync, rely on OS
        }
    }
    Ok(())
}

pub async fn conditional_sync_dir(
    dir: &std::path::Path,
    durability: &DurabilityLevel,
) -> Result<(), ApiError> {
    match durability {
        DurabilityLevel::Immediate => {
            fsync_dir(dir).await?;
        }
        DurabilityLevel::OS => {
            // Skip sync, rely on OS
        }
    }
    Ok(())
}

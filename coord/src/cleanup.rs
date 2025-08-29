use std::time::Duration;
use std::path::Path;
use std::collections::HashSet;
use time::OffsetDateTime;
use tracing::info;

use common::file_utils::tmp_path;
use common::constants::{META_KEY_PREFIX, TMP_DIR_NAME};

use crate::meta::{Meta, TxState, KvDb};

pub async fn startup_cleanup(data_root: &Path, db: &KvDb, grace: Duration) -> anyhow::Result<()> {
    let now_ms = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let cutoff_ms = now_ms - (grace.as_millis() as i128);

    let mut cleaned_meta = 0usize;
    let mut cleaned_tmp = 0usize;

    for kv in db.iter() {
        let (k, v) = kv?;

        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let meta: Meta = serde_json::from_slice(&v)?;

        // only consider pending older than grace period
        if matches!(meta.state, TxState::Pending) && meta.created_ms < cutoff_ms {
            if let Some(upload_id) = meta.upload_id.as_ref() {
                let tpath = tmp_path(data_root, upload_id);
                if file_exists_sync(&tpath) {
                    let _ = tokio::fs::remove_file(tpath).await;
                    cleaned_tmp += 1;
                }
            }
            db.delete(std::str::from_utf8(&k)?)?;
            cleaned_meta += 1;
        }
    }

    info!("startup_cleanup: removed {cleaned_meta} stale Pending metas, {cleaned_tmp} tmp files");
    Ok(())
}

pub async fn sweep_tmp_orphans(data_root: &Path, db: &KvDb) -> anyhow::Result<()> {
    // Build a set of live Pending upload_ids from DB
    let mut pending_ids: HashSet<String> = HashSet::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let meta: Meta = serde_json::from_slice(&v)?;
        if matches!(meta.state, TxState::Pending) {
            if let Some(upload_id) = meta.upload_id {
                pending_ids.insert(upload_id);
            }
        }
    }

    let tmp_dir = data_root.join(TMP_DIR_NAME);
    let mut removed = 0usize;

    if tmp_dir.exists() {
        let mut rd = tokio::fs::read_dir(tmp_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();  // upload_id
            if !pending_ids.contains(&name) {
                let path = entry.path();
                let _ = tokio::fs::remove_file(&path).await;
                removed += 1;
            }
        }
    }

    info!("sweep_tmp_orphans: removed {removed} tmp files");
    Ok(())
}

fn file_exists_sync(path: &Path) -> bool {
    std::fs::metadata(path).map(|m| m.is_file()).unwrap_or(false)
}

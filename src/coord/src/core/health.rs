use std::time::Duration;
use std::path::Path;
use std::collections::HashSet;
use tracing::{info, error};
use tokio::sync::watch;

use std::time::Instant;

use crate::core::node::NodeStatus;
use crate::core::state::CoordinatorState;

use common::file_utils::tmp_path;
use common::constants::{META_KEY_PREFIX, TMP_DIR_NAME, NODE_KEY_PREFIX};
use common::time_utils::utc_now_ms;

use crate::core::meta::{Meta, TxState, KvDb};

pub async fn startup_cleanup(data_root: &Path, db: &KvDb, grace: Duration) -> anyhow::Result<()> {
    let now_ms = utc_now_ms();
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
        if matches!(meta.state, TxState::Pending)
            && let Some(upload_id) = meta.upload_id {
                pending_ids.insert(upload_id);
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

pub async fn node_status_sweeper(
    state: CoordinatorState, 
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = shutdown.changed() => { if *shutdown.borrow() { break; }}
        }

        let mut nodes = match state.nodes.write() {
            Ok(nodes) => nodes,
            Err(e) => {
                error!("failed to acquire nodes lock: {}", e);
                continue;
            }
        };

        let now = Instant::now();

        for node in nodes.values_mut() {
            let elapsed = now.saturating_duration_since(node.last_seen);
            let new_status = if elapsed <= Duration::from_secs(state.hb_alive_secs) {
                NodeStatus::Alive
            } else if elapsed <= Duration::from_secs(state.hb_down_secs) {
                NodeStatus::Suspect
            } else {
                NodeStatus::Down
            };
            if new_status != node.info.status {
                node.info.status = new_status;
                state.db.put(&format!("{}:{}", NODE_KEY_PREFIX, node.info.node_id), &node.info)?;
            }
        }
    }

    info!("node status sweeper stopped");

    Ok(())
}
use std::time::Duration;
use tracing::{info, warn};

use common::schemas::HeartbeatRequest;

use crate::state::VolumeState;
use crate::store::disk_usage;

pub async fn heartbeat_loop(
    state: VolumeState,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>
) -> anyhow::Result<()> {
    let url = format!("{}/admin/heartbeat", state.coordinator_url);
    let mut tick = tokio::time::interval(interval);
    let mut backoff = Duration::from_secs(1);

    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
        }

        let (used, cap) = match disk_usage(&state.data_root) {
            Ok(v) => v,
            Err(e) => {
                warn!("disk_usage error: {e:#}");
                (None, None)
            }
        };

        let hb = HeartbeatRequest {
            node_id: state.node_id.clone(),
            used_bytes: used,
            capacity_bytes: cap,
        };

        let req = state.http_client.post(&url).json(&hb);

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                backoff = interval; // reset backoff on success
            }
            Ok(resp) => {
                warn!("heartbeat non-200: {}", resp.status());
            }
            Err(e) => {
                warn!("heartbeat error: {e}");
            }
        }

        // simple backoff on repeated failures (cap at 30s)
        if backoff > interval {
            tokio::time::sleep(backoff).await;
            backoff = (backoff.mul_f32(1.5)).min(Duration::from_secs(30));
        }
    }

    info!("heartbeat loop stopped");

    Ok(())
}
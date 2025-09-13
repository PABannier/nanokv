use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

use std::time::Instant;

use crate::core::node::NodeStatus;
use crate::core::state::CoordinatorState;

use common::constants::NODE_KEY_PREFIX;

pub async fn node_status_sweeper(
    state: CoordinatorState,
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
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
                state.db.put(
                    &format!("{}:{}", NODE_KEY_PREFIX, node.info.node_id),
                    &node.info,
                )?;
            }
        }
    }

    info!("node status sweeper stopped");

    Ok(())
}

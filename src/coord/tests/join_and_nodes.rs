use std::time::Duration;

use reqwest::Client;
use tokio::time::sleep;

mod common;
use ::common::time_utils;
use common::*;
use coord::core::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_and_nodes_listing() -> anyhow::Result<()> {
    // Start coordinator
    let coord = TestCoordinator::new().await?;
    let client = Client::new();

    // Start volume server (no heartbeat yet)
    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;

    // POST /admin/join from the volume
    volume.join_coordinator().await?;

    // Assert: GET /admin/nodes returns single node with matching fields
    let nodes = list_nodes(&client, coord.url()).await?;
    assert_eq!(nodes.len(), 1);

    let node = &nodes[0];
    assert_eq!(node.node_id, "vol-1");
    assert_eq!(node.subvols, 1);
    assert_eq!(node.version, Some("test".to_string()));
    assert_eq!(node.status, NodeStatus::Alive);

    // Check that last_heartbeat_ms is recent (within 5 seconds)
    let now_ms = time_utils::utc_now_ms();
    let heartbeat_age_ms = now_ms - node.last_heartbeat_ms;
    assert!(
        heartbeat_age_ms < 5000,
        "heartbeat too old: {}ms",
        heartbeat_age_ms
    );

    // Start heartbeat loop (interval ~500ms)
    volume.start_heartbeat(500)?;

    // Sleep ~2 intervals and assert status remains Alive
    sleep(Duration::from_millis(1000)).await;
    let nodes = list_nodes(&client, coord.url()).await?;
    assert_eq!(nodes[0].status, NodeStatus::Alive);

    // Stop heartbeat loop
    volume.stop_heartbeat();

    // Wait > Down threshold (3000ms) and assert node becomes Down
    wait_until(5000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes[0].status == NodeStatus::Down)
    })
    .await?;

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_heartbeat_status_transitions() -> anyhow::Result<()> {
    // Use shorter timeouts for faster testing
    let coord = TestCoordinator::with_config(1500, 3000, 500).await?;
    let client = Client::new();

    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;

    // Start heartbeat loop -> assert Alive after a couple intervals
    volume.start_heartbeat(500)?;
    sleep(Duration::from_millis(1000)).await;

    let nodes = list_nodes(&client, coord.url()).await?;
    assert_eq!(nodes[0].status, NodeStatus::Alive);

    // Pause heartbeat (stop the loop)
    volume.stop_heartbeat();

    // After Alive window elapses but before Down window, assert Suspect
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes[0].status == NodeStatus::Suspect)
    })
    .await?;

    // After Down window elapses, assert Down
    wait_until(5000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes[0].status == NodeStatus::Down)
    })
    .await?;

    // Restart heartbeat loop -> assert node returns to Alive
    volume.start_heartbeat(500)?;
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes[0].status == NodeStatus::Alive)
    })
    .await?;

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

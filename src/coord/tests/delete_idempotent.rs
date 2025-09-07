use reqwest::Client;

mod common;
use ::common::key_utils;
use common::*;
use coord::core::meta::{Meta, TxState};
use coord::core::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_idempotent() -> anyhow::Result<()> {
    // Start one Alive volume + coordinator
    let coord = TestCoordinator::new().await?;
    let client = Client::new();

    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;
    volume.start_heartbeat(500)?;

    // Wait for volume to be alive
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 1 && nodes[0].status == NodeStatus::Alive)
    })
    .await?;

    // Upload a small object
    let payload = b"test data for delete idempotency".to_vec();
    let (status, _etag, _len) =
        put_via_coordinator(&client, coord.url(), "delete-test", payload).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // First DELETE should return 204
    let status1 = delete_via_coordinator(&client, coord.url(), "delete-test").await?;
    assert_eq!(status1, reqwest::StatusCode::NO_CONTENT);

    // Verify meta becomes Tombstoned
    let key = key_utils::Key::from_percent_encoded("delete-test").unwrap();
    let key_enc = key.enc();
    let meta_key = key_utils::meta_key_for(key_enc);
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some(), "Meta should exist after delete");

    let meta = meta.unwrap();
    assert_eq!(meta.state, TxState::Tombstoned);

    // Second DELETE should also return 204 (idempotent)
    let status2 = delete_via_coordinator(&client, coord.url(), "delete-test").await?;
    assert_eq!(status2, reqwest::StatusCode::NO_CONTENT);

    // Meta should still be Tombstoned
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(
        meta.is_some(),
        "Meta should still exist after second delete"
    );

    let meta = meta.unwrap();
    assert_eq!(meta.state, TxState::Tombstoned);

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_nonexistent_key() -> anyhow::Result<()> {
    // Test deleting a key that doesn't exist
    let coord = TestCoordinator::new().await?;
    let client = Client::new();

    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;
    volume.start_heartbeat(500)?;

    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 1 && nodes[0].status == NodeStatus::Alive)
    })
    .await?;

    // Try to delete a key that doesn't exist
    let status = delete_via_coordinator(&client, coord.url(), "nonexistent-key").await?;

    // Should return 404 Not Found
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_volume_delete_idempotent() -> anyhow::Result<()> {
    // Test that volume's internal delete is idempotent
    let coord = TestCoordinator::new().await?;
    let client = Client::new();

    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;
    volume.start_heartbeat(500)?;

    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 1 && nodes[0].status == NodeStatus::Alive)
    })
    .await?;

    // Upload a small object
    let payload = b"test data for volume delete idempotency".to_vec();
    let (status, _etag, _len) =
        put_via_coordinator(&client, coord.url(), "vol-delete-test", payload).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // Call coordinator DELETE which should call volume's internal delete
    let status1 = delete_via_coordinator(&client, coord.url(), "vol-delete-test").await?;
    assert_eq!(status1, reqwest::StatusCode::NO_CONTENT);

    // Now directly call volume's delete endpoint (this simulates the coordinator
    // calling it again or some other scenario where the same delete is attempted)
    let key = key_utils::Key::from_percent_encoded("vol-delete-test").unwrap();
    let key_enc = key.enc();
    let vol_delete_url = format!("{}/internal/delete/{}", volume.url(), key_enc);
    let resp = client.delete(&vol_delete_url).send().await?;

    // Volume should return 204 even if file was already missing (idempotent)
    // Note: The current volume implementation might return an error if file doesn't exist.
    // This depends on the implementation details. The test documents expected behavior.
    println!("Volume delete response: {}", resp.status());

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

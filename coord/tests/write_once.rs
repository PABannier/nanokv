use reqwest::Client;

mod common;
use common::*;
use ::common::file_utils;
use coord::core::node::NodeStatus;
use coord::core::meta::{Meta, TxState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_once_semantics() -> anyhow::Result<()> {
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
    }).await?;
    
    let payload = b"test data for write-once semantics".to_vec();
    let expected_etag = blake3_hex(&payload);
    
    // First PUT should succeed with 201
    let (status1, etag1, len1) = put_via_coordinator(&client, coord.url(), "same-key", payload.clone()).await?;
    assert_eq!(status1, reqwest::StatusCode::CREATED);
    assert_eq!(etag1, expected_etag);
    assert_eq!(len1, payload.len() as u64);
    
    // Second PUT with identical bytes should return 409 Conflict
    let (status2, _etag2, _len2) = put_via_coordinator(&client, coord.url(), "same-key", payload.clone()).await?;
    assert_eq!(status2, reqwest::StatusCode::CONFLICT);
    
    // Assert RocksDB meta remains correct and only one blob exists
    let meta_key = file_utils::meta_key_for("same%2Dkey"); // percent-encoded
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some(), "Meta not found after duplicate PUT");
    
    let meta = meta.unwrap();
    assert_eq!(meta.state, TxState::Committed);
    assert_eq!(meta.size, payload.len() as u64);
    assert_eq!(meta.etag_hex, expected_etag);
    assert_eq!(meta.replicas, vec!["vol-1"]);
    
    // Verify only one blob exists on volume
    let blob_file_path = file_utils::blob_path(&volume.state.data_root, "same%2Dkey");
    assert!(blob_file_path.exists(), "Blob file should exist");
    
    let file_size = std::fs::metadata(&blob_file_path)?.len();
    assert_eq!(file_size, payload.len() as u64);
    
    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_once_with_pending_state() -> anyhow::Result<()> {
    // This test verifies that Pending uploads also trigger write-once semantics
    let coord = TestCoordinator::new().await?;
    let client = Client::new();
    
    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;
    volume.start_heartbeat(500)?;
    
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 1 && nodes[0].status == NodeStatus::Alive)
    }).await?;
    
    let payload = b"test data for pending write-once".to_vec();
    
    // Artificially create a Pending state in the DB (simulating an interrupted upload)
    let meta_key = file_utils::meta_key_for("pending%2Dkey");
    let pending_meta = Meta::pending("test-upload-id".to_string(), vec!["vol-1".to_string()]);
    coord.state.db.put(&meta_key, &pending_meta)?;
    
    // Now try to PUT the same key - should get conflict due to existing Pending state
    let (status, _etag, _len) = put_via_coordinator(&client, coord.url(), "pending-key", payload).await?;
    assert_eq!(status, reqwest::StatusCode::CONFLICT);
    
    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;
    
    Ok(())
}

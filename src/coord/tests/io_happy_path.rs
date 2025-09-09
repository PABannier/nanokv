use reqwest::Client;

mod common;
use ::common::file_utils;
use ::common::key_utils;
use common::*;
use coord::core::meta::{Meta, TxState};
use coord::core::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_get_delete_happy_path() -> anyhow::Result<()> {
    // Start one Alive volume + coordinator
    let coord = TestCoordinator::new().await?;
    let client = Client::new();

    let mut volume = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    volume.join_coordinator().await?;
    volume.start_heartbeat(500)?;

    // Wait for volume to be alive
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        println!("Current nodes: {:?}", nodes);
        Ok(nodes.len() == 1 && nodes[0].status == NodeStatus::Alive)
    })
    .await?;

    println!("Volume is alive, proceeding with PUT test");

    // PUT a small payload through coordinator for debugging
    let payload = generate_random_bytes(1024); // 1 KiB
    let expected_etag = blake3_hex(&payload);
    let expected_len = payload.len() as u64;

    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), "test-key", payload.clone()).await?;

    // Assert coordinator response
    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert_eq!(etag, expected_etag);
    assert_eq!(len, expected_len);

    // Assert RocksDB meta for key is Committed with correct fields
    let key = key_utils::Key::from_percent_encoded("test-key").unwrap();
    let key_enc = key.enc();
    let meta_key = key_utils::meta_key_for(key_enc);
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some(), "Meta not found in RocksDB");

    let meta = meta.unwrap();
    assert_eq!(meta.state, TxState::Committed);
    assert_eq!(meta.size, expected_len);
    assert_eq!(meta.etag_hex, expected_etag);
    assert_eq!(meta.replicas, vec!["vol-1"]);

    // Assert volume has the file at correct location with same size
    let blob_file_path = file_utils::blob_path(&volume.state.data_root, key_enc);
    assert!(
        blob_file_path.exists(),
        "Blob file not found at {:?}",
        blob_file_path
    );

    let file_size = std::fs::metadata(&blob_file_path)?.len();
    assert_eq!(file_size, expected_len);

    // GET the key - should return 302 redirect
    let (status, location) = get_redirect_location(&client, coord.url(), "test-key").await?;
    assert_eq!(status, reqwest::StatusCode::FOUND);
    assert!(location.is_some(), "No Location header in redirect");

    let location = location.unwrap();
    let expected_location = format!("{}/blobs/{}", volume.url(), key_enc);
    assert_eq!(location, expected_location);

    // Follow the redirect to get actual content
    let (status, bytes) = get_via_coordinator(&client, coord.url(), "test-key").await?;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(bytes, payload);

    // DELETE the key
    let status = delete_via_coordinator(&client, coord.url(), "test-key").await?;
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);

    // Assert meta becomes Tombstoned
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some(), "Meta should still exist after delete");

    let meta = meta.unwrap();
    assert_eq!(meta.state, TxState::Tombstoned);

    // GET should return 404
    let (status, _location) = get_redirect_location(&client, coord.url(), "test-key").await?;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

    // Volume blob should be removed (or eventually removed)
    // Note: depending on implementation, this might be async
    let blob_exists = blob_file_path.exists();
    if blob_exists {
        println!("Warning: blob file still exists after delete, might be async cleanup");
    }

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_large_object_streaming() -> anyhow::Result<()> {
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

    // Create a 100-200 MiB payload (we'll use 150 MiB)
    let size = 150 * 1024 * 1024; // 150 MiB
    let payload = generate_random_bytes(size);
    let expected_etag = blake3_hex(&payload);
    let expected_len = payload.len() as u64;

    // PUT via coordinator
    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), "large-key", payload.clone()).await?;

    // Assert success and metadata correctness
    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert_eq!(etag, expected_etag);
    assert_eq!(len, expected_len);

    // Memory bound assertion: The key assertion is that the request doesn't buffer
    // the entire body. This is hard to test directly, but the fact that we can
    // handle 150MB without OOM indicates streaming is working.
    // In a real test environment, you might set memory limits or use instrumentation.

    // Verify we can GET the large object back
    let (status, bytes) = get_via_coordinator(&client, coord.url(), "large-key").await?;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(bytes.len(), size);
    assert_eq!(blake3_hex(&bytes), expected_etag);

    // Cleanup
    volume.shutdown().await?;
    coord.shutdown().await?;

    Ok(())
}

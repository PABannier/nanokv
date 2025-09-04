use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prepare_multiple_retries() -> anyhow::Result<()> {
    // Test that coordinator can handle multiple transient failures
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-multiple-prepare-retries";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Try to inject multiple failures on different followers
    // This simulates a scenario where network issues cause multiple prepare attempts to fail
    for (i, replica_node) in expected_replicas[1..].iter().enumerate() {
        if let Some(volume) = volumes.iter().find(|v| v.state.node_id == *replica_node) {
            let fail_url = format!("{}/admin/fail/prepare?count=2", volume.url());
            let _ = client.post(&fail_url).send().await;
            println!("Attempted to inject 2 prepare failures on replica {}: {}", i+1, replica_node);
        }
    }

    let payload = generate_random_bytes(2048);
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should trigger multiple prepare retries");

    let start_time = std::time::Instant::now();
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    // Should still succeed despite multiple failures
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed despite multiple prepare failures");
    assert_eq!(etag, expected_etag, "ETag should match");

    println!("PUT with multiple prepare retries completed in {:?}", elapsed);

    // Verify final state
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have the file");

    println!("Multiple prepare retry test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prepare_failure_on_head_node() -> anyhow::Result<()> {
    // Test prepare failure on the head node (first replica)
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-head-prepare-retry";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    
    // Inject failure on head node (first replica)
    let head_node = &expected_replicas[0];
    if let Some(head_volume) = volumes.iter().find(|v| v.state.node_id == *head_node) {
        let fail_url = format!("{}/admin/fail/prepare?once=true", head_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected prepare failure on head node: {}", head_node);
    }

    let payload = generate_random_bytes(512);
    let expected_etag = blake3_hex(&payload);

    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;

    // Should succeed after retry
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed after head prepare retry");
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify all replicas have the data
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have the file");

    println!("Head node prepare retry test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

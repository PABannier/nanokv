use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pull_checksum_mismatch_abort() -> anyhow::Result<()> {
    // Start coordinator with N=3 replicas
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-checksum-mismatch";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    println!("Expected replicas for key '{}': {:?}", key, expected_replicas);

    // Inject etag mismatch on one follower during pull
    let follower_node = &expected_replicas[1]; // F1 = second replica
    let follower_volume = volumes.iter()
        .find(|v| v.state.node_id == *follower_node)
        .expect("Should find follower volume");

    // Inject etag mismatch using assumed test endpoint
    let fail_url = format!("{}/admin/fail/etag_mismatch?once=true", follower_volume.url());
    let fail_resp = client.post(&fail_url).send().await;
    
    if let Ok(resp) = fail_resp {
        if resp.status().is_success() {
            println!("Injected etag mismatch on follower: {}", follower_node);
        } else {
            println!("Warning: Could not inject etag mismatch, test may not behave as expected");
        }
    } else {
        println!("Warning: Etag mismatch fault injection endpoint not available");
    }

    // PUT with small payload
    let payload = generate_random_bytes(1024); // 1 KiB
    let _expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should trigger checksum mismatch and abort");

    let start_time = std::time::Instant::now();
    let (status, _etag, _len) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Assert: PUT returns 5xx due to checksum mismatch
    assert!(status.is_server_error(), "PUT should return 5xx due to checksum mismatch, got: {}", status);
    println!("PUT correctly failed with status: {} in {:?}", status, elapsed);

    // Assert: No replica has a final file (all tmp cleaned by abort guard/sweep)
    // It's ok if a tmp remains briefly; assert blob absence
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 0, "No volume should have the final blob file after abort");

    // Assert: Meta is not Committed; Pending either removed or will be cleaned by startup cleanup
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be Committed after checksum mismatch");
        println!("Meta state after abort: {:?}", meta.state);
    } else {
        println!("Meta was cleaned up after abort");
    }

    // Verify GET returns appropriate error
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert!(get_status.is_client_error() || get_status.is_server_error(), 
            "GET should return error after aborted write, got: {}", get_status);

    println!("Checksum mismatch abort test successful - transaction properly aborted");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_size_mismatch_abort() -> anyhow::Result<()> {
    // Test that size mismatches also trigger abort
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-size-mismatch";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Try to inject a size mismatch (this might be harder to simulate)
    // For now, we'll use the etag_mismatch endpoint as a proxy
    let follower_node = &expected_replicas[2]; // F2 = third replica
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        let fail_url = format!("{}/admin/fail/etag_mismatch?once=true", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected mismatch on follower: {}", follower_node);
    }

    let payload = generate_random_bytes(2048); // 2 KiB

    println!("Starting PUT that should trigger size/checksum mismatch");

    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;

    // Should fail with server error
    assert!(status.is_server_error(), "PUT should fail due to mismatch, got: {}", status);

    // Verify no final files exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 0, "No volume should have final file after size mismatch");

    // Verify meta is not committed
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be committed after mismatch");
    }

    println!("Size mismatch abort test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_followers_checksum_mismatch() -> anyhow::Result<()> {
    // Test checksum mismatch on multiple followers
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-multi-checksum-mismatch";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject checksum mismatch on both followers
    for replica_node in &expected_replicas[1..] { // Skip head, inject on followers
        if let Some(volume) = volumes.iter().find(|v| v.state.node_id == *replica_node) {
            let fail_url = format!("{}/admin/fail/etag_mismatch?once=true", volume.url());
            let _ = client.post(&fail_url).send().await;
            println!("Injected checksum mismatch on follower: {}", replica_node);
        }
    }

    let payload = generate_random_bytes(4096); // 4 KiB

    println!("Starting PUT with multiple checksum mismatches");

    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;

    // Should fail - even one checksum mismatch should abort the entire transaction
    assert!(status.is_server_error(), "PUT should fail with multiple checksum mismatches, got: {}", status);

    // Verify cleanup
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 0, "No volume should have final file after multiple mismatches");

    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be committed");
    }

    println!("Multiple checksum mismatch test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checksum_mismatch_with_large_payload() -> anyhow::Result<()> {
    // Test that checksum mismatch works correctly even with large payloads
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-large-checksum-mismatch";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject checksum mismatch on one follower
    let follower_node = &expected_replicas[1];
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        let fail_url = format!("{}/admin/fail/etag_mismatch?once=true", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected checksum mismatch on follower: {}", follower_node);
    }

    // Use a larger payload to test streaming behavior with checksum failure
    let payload_size = 20 * 1024 * 1024; // 20 MiB
    let payload = generate_random_bytes(payload_size);

    println!("Starting PUT with {}MB payload that should fail checksum", payload_size / (1024*1024));

    let start_time = std::time::Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    // Should fail despite large payload
    assert!(status.is_server_error(), "Large payload PUT should fail due to checksum mismatch, got: {}", status);
    println!("Large payload PUT correctly failed in {:?}", elapsed);

    // Verify no final files exist (important for large files)
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 0, "No volume should have large file after checksum mismatch");

    // Verify meta state
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be committed for large file");
    }

    println!("Large payload checksum mismatch test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_successful_write_after_checksum_mismatch_cleanup() -> anyhow::Result<()> {
    // Test that after a checksum mismatch abort, subsequent writes to the same key work
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-retry-after-mismatch";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // First attempt: inject checksum mismatch
    let follower_node = &expected_replicas[1];
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        let fail_url = format!("{}/admin/fail/etag_mismatch?once=true", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected checksum mismatch for first attempt");
    }

    let payload1 = generate_random_bytes(1024);
    let (status1, _, _) = put_via_coordinator(&client, coord.url(), key, payload1).await?;
    assert!(status1.is_server_error(), "First PUT should fail due to checksum mismatch");

    println!("First PUT failed as expected, attempting second PUT");

    // Second attempt: should succeed (no fault injection)
    let payload2 = generate_random_bytes(2048);
    let expected_etag2 = blake3_hex(&payload2);
    
    // Wait a bit for cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let (status2, etag2, _) = put_via_coordinator(&client, coord.url(), key, payload2.clone()).await?;
    
    // Second attempt should succeed
    assert_eq!(status2, reqwest::StatusCode::CREATED, "Second PUT should succeed after cleanup");
    assert_eq!(etag2, expected_etag2, "Second PUT should have correct etag");

    // Verify final state
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist after successful write");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed after successful retry");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have file after successful retry");

    // Verify we can read the correct data
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(response_bytes, payload2, "Should read the data from successful write");

    println!("Successful write after checksum mismatch cleanup test passed");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

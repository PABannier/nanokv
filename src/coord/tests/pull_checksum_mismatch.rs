use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;


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
        let fail_url = format!(
            "{}/admin/fail/etag_mismatch?once=true",
            follower_volume.url()
        );
        let _ = client.post(&fail_url).send().await;
        println!("Injected mismatch on follower: {}", follower_node);
    }

    let payload = generate_random_bytes(2048); // 2 KiB

    println!("Starting PUT that should trigger size/checksum mismatch");

    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;

    // Should fail with unprocessable entity
    assert!(
        status == reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "PUT should fail due to mismatch, got: {}",
        status
    );

    // Verify no final files exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No volume should have final file after size mismatch"
    );

    // Verify meta is not committed
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be committed after mismatch"
        );
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
    for replica_node in &expected_replicas[1..] {
        // Skip head, inject on followers
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
    assert!(
        status == reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "PUT should fail with multiple checksum mismatches, got: {}",
        status
    );

    // Verify cleanup
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No volume should have final file after multiple mismatches"
    );

    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be committed"
        );
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
        let fail_url = format!(
            "{}/admin/fail/etag_mismatch?once=true",
            follower_volume.url()
        );
        let _ = client.post(&fail_url).send().await;
        println!("Injected checksum mismatch on follower: {}", follower_node);
    }

    // Use a larger payload to test streaming behavior with checksum failure
    let payload_size = 20 * 1024 * 1024; // 20 MiB
    let payload = generate_random_bytes(payload_size);

    println!(
        "Starting PUT with {}MB payload that should fail checksum",
        payload_size / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    // Should fail despite large payload
    assert!(
        status == reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "Large payload PUT should fail due to checksum mismatch, got: {}",
        status
    );
    println!("Large payload PUT correctly failed in {:?}", elapsed);

    // Verify no final files exist (important for large files)
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No volume should have large file after checksum mismatch"
    );

    // Verify meta state
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be committed for large file"
        );
    }

    println!("Large payload checksum mismatch test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

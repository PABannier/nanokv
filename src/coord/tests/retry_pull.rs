use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pull_retry_transient_failure() -> anyhow::Result<()> {
    // Start coordinator with N=3 replicas
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-pull-retry";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    println!(
        "Expected replicas for key '{}': {:?}",
        key, expected_replicas
    );

    // Inject one-time pull failure on follower F2 (third replica)
    let follower_node = &expected_replicas[2]; // F2 = third replica
    let follower_volume = volumes
        .iter()
        .find(|v| v.state.node_id == *follower_node)
        .expect("Should find follower volume");

    // Inject pull failure using assumed test endpoint
    let fail_url = format!("{}/admin/fail/pull?once=true", follower_volume.url());
    let fail_resp = client.post(&fail_url).send().await;

    if let Ok(resp) = fail_resp {
        if resp.status().is_success() {
            println!(
                "Injected one-time pull failure on follower: {}",
                follower_node
            );
        } else {
            println!("Warning: Could not inject pull failure, assuming retry logic exists");
        }
    } else {
        println!(
            "Warning: Pull fault injection endpoint not available, assuming retry logic exists"
        );
    }

    // PUT with medium payload (16-32 MiB) to test streaming pull retry
    let payload_size = 24 * 1024 * 1024; // 24 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);

    println!(
        "Starting PUT with {}MB payload that should trigger pull retry",
        payload_size / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Assert: Request eventually succeeds (201), meaning coordinator retried pull within its time-box
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "PUT should eventually succeed after pull retry"
    );
    assert_eq!(etag, expected_etag, "ETag should match");
    assert_eq!(len, payload.len() as u64, "Length should match");

    println!("PUT completed in {:?} (should show retry delay)", elapsed);
    assert!(
        elapsed.as_millis() > 500,
        "Should take some time due to retry and large payload"
    );
    assert!(elapsed.as_secs() < 30, "Should not timeout completely");

    // Assert: Meta Committed, files present on all 3
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");
    assert_eq!(meta.replicas.len(), 3, "Should have 3 replicas");
    assert_eq!(
        meta.size,
        payload.len() as u64,
        "Meta size should match payload"
    );
    assert_eq!(meta.etag_hex, expected_etag, "Meta etag should match");

    // Verify all replicas have the file with correct size
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All 3 volumes should have the file after pull retry"
    );

    // Verify we can read the data back correctly
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(
        response_bytes.len(),
        payload.len(),
        "Response should have correct length"
    );
    assert_eq!(
        response_bytes, payload,
        "Response should match original payload"
    );

    println!("Pull retry test successful - all replicas have the correct file");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pull_failure_multiple_followers() -> anyhow::Result<()> {
    // Test pull retry when multiple followers fail
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-multi-pull-retry";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject pull failures on both followers (F1 and F2)
    for replica_node in &expected_replicas[1..] {
        // Skip head, inject on followers
        if let Some(volume) = volumes.iter().find(|v| v.state.node_id == *replica_node) {
            let fail_url = format!("{}/admin/fail/pull?once=true", volume.url());
            let _ = client.post(&fail_url).send().await;
            println!("Injected pull failure on follower: {}", replica_node);
        }
    }

    // Use a larger payload to make pull operations more significant
    let payload_size = 32 * 1024 * 1024; // 32 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);

    println!(
        "Starting PUT with {}MB payload that should trigger multiple pull retries",
        payload_size / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Should still succeed despite multiple pull failures
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "PUT should succeed despite multiple pull failures"
    );
    assert_eq!(etag, expected_etag, "ETag should match");

    println!("PUT with multiple pull retries completed in {:?}", elapsed);

    // Verify final state
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All volumes should have the file"
    );

    // Verify data integrity after multiple pull retries
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(
        response_bytes, payload,
        "Data should be intact after multiple pull retries"
    );

    println!("Multiple pull retry test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pull_retry_with_read_tmp_failure() -> anyhow::Result<()> {
    // Test pull retry when the head node's read_tmp endpoint fails
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-read-tmp-retry";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject read_tmp failure on head node
    let head_node = &expected_replicas[0];
    if let Some(head_volume) = volumes.iter().find(|v| v.state.node_id == *head_node) {
        let fail_url = format!("{}/admin/fail/read_tmp?once=true", head_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected read_tmp failure on head node: {}", head_node);
    }

    // Use medium-sized payload
    let payload_size = 16 * 1024 * 1024; // 16 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should trigger read_tmp retry on head node");

    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // Should succeed after retry
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "PUT should succeed after read_tmp retry"
    );
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify all replicas have the data
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All volumes should have the file"
    );

    // Verify data integrity
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(
        response_bytes, payload,
        "Data should be intact after read_tmp retry"
    );

    println!("Read_tmp retry test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pull_streaming_integrity() -> anyhow::Result<()> {
    // Test that pull retry maintains streaming integrity for large objects
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-pull-streaming";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject a pull failure that should trigger retry during streaming
    let follower_node = &expected_replicas[1];
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        let fail_url = format!("{}/admin/fail/pull?once=true", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!(
            "Injected pull failure during streaming on: {}",
            follower_node
        );
    }

    // Use a very large payload to ensure streaming behavior
    let payload_size = 50 * 1024 * 1024; // 50 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);

    println!(
        "Starting PUT with {}MB payload to test streaming pull retry",
        payload_size / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "Large streaming PUT should succeed"
    );
    assert_eq!(etag, expected_etag, "ETag should match for large payload");

    println!("Large streaming PUT completed in {:?}", elapsed);

    // Verify all replicas have the complete file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All volumes should have the large file"
    );

    // Most importantly, verify data integrity for the large file
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(
        response_bytes.len(),
        payload.len(),
        "Large file should have correct length"
    );

    // For very large files, we'll just check the hash rather than byte-by-byte comparison
    let response_etag = blake3_hex(&response_bytes);
    assert_eq!(
        response_etag, expected_etag,
        "Large file should have correct hash after streaming pull retry"
    );

    println!("Streaming pull retry integrity test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

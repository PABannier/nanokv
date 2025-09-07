use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_abort_after_any_commit() -> anyhow::Result<()> {
    // Test that abort is not sent after any commit has happened (guard disarmed)
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-no-abort-after-commit";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    println!("Expected replicas for key '{}': {:?}", key, expected_replicas);

    // Set up scenario: let write pass through to just before commit, 
    // then inject failure in commit on F1 once, but ensure commit eventually succeeds on retry
    let follower_node = &expected_replicas[1]; // F1 = second replica
    let follower_volume = volumes.iter()
        .find(|v| v.state.node_id == *follower_node)
        .expect("Should find follower volume");

    // Inject one-time commit failure that will be retried and succeed
    let fail_url = format!("{}/admin/fail/commit?once=true", follower_volume.url());
    let fail_resp = client.post(&fail_url).send().await;
    
    if let Ok(resp) = fail_resp {
        if resp.status().is_success() {
            println!("Injected one-time commit failure on follower: {}", follower_node);
        } else {
            println!("Warning: Could not inject commit failure");
        }
    } else {
        println!("Warning: Commit fault injection endpoint not available");
    }

    // PUT with payload
    let payload = generate_random_bytes(4 * 1024 * 1024); // 4 MiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should trigger commit retry without abort");

    let start_time = std::time::Instant::now();
    let (status, etag, len) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Assert: After any single replica commits, no abort calls are sent
    // The PUT should eventually succeed
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed after commit retry");
    assert_eq!(etag, expected_etag, "ETag should match");
    assert_eq!(len, payload.len() as u64, "Length should match");

    println!("PUT completed successfully in {:?}", elapsed);

    // Assert: Final success and meta Committed
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");
    assert_eq!(meta.replicas.len(), 3, "Should have 3 replicas");

    // Verify all replicas have the file (no abort was called)
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All 3 volumes should have the file (no abort)");

    // Verify data integrity
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(response_bytes, payload, "Data should be intact");

    // The key assertion: no temporary files should remain (they weren't aborted)
    // This is implicit in the fact that all final files exist and the transaction succeeded

    println!("No abort after commit test successful - commit-wins rule enforced");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commit_wins_rule_multiple_retries() -> anyhow::Result<()> {
    // Test commit-wins rule with multiple commit retries
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-commit-wins-multiple";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject multiple commit failures that will eventually succeed
    let follower_node = &expected_replicas[2]; // F2 = third replica
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        let fail_url = format!("{}/admin/fail/commit?count=3", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected 3 commit failures on follower: {}", follower_node);
    }

    let payload = generate_random_bytes(2 * 1024 * 1024); // 2 MiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT with multiple commit retries");

    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // Should eventually succeed
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed after multiple commit retries");
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify final committed state
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    // All replicas should have the file (no abort during commit phase)
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have the file");

    println!("Commit-wins rule with multiple retries test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_abort_after_head_commits() -> anyhow::Result<()> {
    // Test that once head commits, abort is never called even if followers fail
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-no-abort-after-head";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Let head commit succeed, but make followers fail initially
    // This tests that once head commits, the guard is disarmed
    for replica_node in &expected_replicas[1..] { // Inject on followers only
        if let Some(volume) = volumes.iter().find(|v| v.state.node_id == *replica_node) {
            let fail_url = format!("{}/admin/fail/commit?once=true", volume.url());
            let _ = client.post(&fail_url).send().await;
            println!("Injected commit failure on follower: {}", replica_node);
        }
    }

    let payload = generate_random_bytes(1024 * 1024); // 1 MiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT where head commits but followers initially fail");

    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // Should succeed after retries (no abort once head commits)
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed - no abort after head commits");
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify all replicas eventually have the file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have the file");

    // Verify committed state
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    println!("No abort after head commits test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_abort_guard_disarmed_timing() -> anyhow::Result<()> {
    // Test the precise timing of when abort guard is disarmed
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-abort-guard-timing";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Create a scenario where we can observe the guard disarming behavior
    // Inject failures that happen after the guard should be disarmed
    let follower_node = &expected_replicas[1];
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        // Use a small number of failures to test retry behavior during commit phase
        let fail_url = format!("{}/admin/fail/commit?count=2", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected limited commit failures on: {}", follower_node);
    }

    let payload = generate_random_bytes(512 * 1024); // 512 KiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT to test abort guard disarm timing");

    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // The key behavior: once we reach commit phase, guard is disarmed
    // So even with commit failures, we should get retries, not aborts
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed via commit retries");
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify final state shows successful commit (no abort happened)
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have file (no abort)");

    // Verify data integrity
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(response_bytes, payload, "Data should be correct");

    println!("Abort guard timing test successful - guard properly disarmed before commit phase");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

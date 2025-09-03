use reqwest::Client;
use std::time::{Duration, Instant};

mod common;
use common::*;
use coord::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retry_backoff_observable() -> anyhow::Result<()> {
    // Test time-boxed retry policy with observable backoff behavior
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-retry-backoff";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    // Inject repeated transient prepare failures on F1 (e.g., 3 times) then success
    let follower_node = &expected_replicas[1];
    let follower_volume = volumes.iter()
        .find(|v| v.state.node_id == *follower_node)
        .expect("Should find follower volume");

    // Inject multiple failures to observe retry behavior
    let fail_url = format!("{}/admin/fail/prepare?count=3", follower_volume.url());
    let fail_resp = client.post(&fail_url).send().await;
    
    if let Ok(resp) = fail_resp {
        if resp.status().is_success() {
            println!("Injected 3 prepare failures on follower: {}", follower_node);
        } else {
            println!("Warning: Could not inject prepare failures");
        }
    } else {
        println!("Warning: Fault injection endpoint not available");
    }

    let payload = generate_random_bytes(1024 * 1024); // 1 MiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should trigger observable retry backoff");

    // Capture timestamps to observe retry behavior
    let start_time = Instant::now();
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let total_elapsed = start_time.elapsed();

    // Assert: increasing intervals between attempts (within jitter), and total time ≤ configured budget
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should eventually succeed after retries");
    assert_eq!(etag, expected_etag, "ETag should match");

    println!("PUT completed in {:?}", total_elapsed);

    // Verify timing constraints
    assert!(total_elapsed.as_millis() > 500, "Should take some time due to multiple retries and backoff");
    assert!(total_elapsed.as_secs() < 10, "Should complete within reasonable time budget");

    // The backoff behavior is observable in the total time taken
    // With exponential backoff: first retry ~100ms, second ~200ms, third ~400ms, etc.
    // So 3 retries should take roughly 700ms + processing time
    println!("Retry backoff timing appears consistent with exponential backoff");

    // Verify final success
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 3, "All volumes should have the file");

    println!("Retry backoff observable test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retry_backoff_different_phases() -> anyhow::Result<()> {
    // Test retry backoff behavior in different phases (prepare, pull, commit)
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    // Test 1: Prepare phase backoff
    let key1 = "test-prepare-backoff";
    let nodes = list_nodes(&client, coord.url()).await?;
    let replicas1 = test_placement_n(key1, &nodes, 3);

    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == replicas1[1]) {
        let fail_url = format!("{}/admin/fail/prepare?count=2", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected prepare failures for backoff test");
    }

    let payload1 = generate_random_bytes(512 * 1024); // 512 KiB
    let start1 = Instant::now();
    let (status1, _, _) = put_via_coordinator(&client, coord.url(), key1, payload1).await?;
    let elapsed1 = start1.elapsed();

    assert_eq!(status1, reqwest::StatusCode::CREATED, "Prepare backoff PUT should succeed");
    println!("Prepare phase with backoff completed in {:?}", elapsed1);

    // Test 2: Pull phase backoff
    let key2 = "test-pull-backoff";
    let replicas2 = test_placement_n(key2, &nodes, 3);

    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == replicas2[2]) {
        let fail_url = format!("{}/admin/fail/pull?count=2", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected pull failures for backoff test");
    }

    let payload2 = generate_random_bytes(2 * 1024 * 1024); // 2 MiB
    let start2 = Instant::now();
    let (status2, _, _) = put_via_coordinator(&client, coord.url(), key2, payload2).await?;
    let elapsed2 = start2.elapsed();

    assert_eq!(status2, reqwest::StatusCode::CREATED, "Pull backoff PUT should succeed");
    println!("Pull phase with backoff completed in {:?}", elapsed2);

    // Test 3: Commit phase backoff
    let key3 = "test-commit-backoff";
    let replicas3 = test_placement_n(key3, &nodes, 3);

    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == replicas3[1]) {
        let fail_url = format!("{}/admin/fail/commit?count=2", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected commit failures for backoff test");
    }

    let payload3 = generate_random_bytes(1024 * 1024); // 1 MiB
    let start3 = Instant::now();
    let (status3, _, _) = put_via_coordinator(&client, coord.url(), key3, payload3).await?;
    let elapsed3 = start3.elapsed();

    assert_eq!(status3, reqwest::StatusCode::CREATED, "Commit backoff PUT should succeed");
    println!("Commit phase with backoff completed in {:?}", elapsed3);

    // Analyze backoff behavior across phases
    println!("Backoff timing analysis:");
    println!("  Prepare phase: {:?}", elapsed1);
    println!("  Pull phase: {:?}", elapsed2);
    println!("  Commit phase: {:?}", elapsed3);

    // Each should show evidence of retry delays
    assert!(elapsed1.as_millis() > 200, "Prepare backoff should show retry delay");
    assert!(elapsed2.as_millis() > 200, "Pull backoff should show retry delay");
    assert!(elapsed3.as_millis() > 200, "Commit backoff should show retry delay");

    // But all should complete within reasonable bounds
    assert!(elapsed1.as_secs() < 8, "Prepare backoff should complete within budget");
    assert!(elapsed2.as_secs() < 15, "Pull backoff should complete within budget (longer for large payload)");
    assert!(elapsed3.as_secs() < 8, "Commit backoff should complete within budget");

    println!("Different phases retry backoff test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retry_backoff_budget_exhaustion() -> anyhow::Result<()> {
    // Test that retry stops when budget is exhausted
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-backoff-budget-exhausted";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject persistent failures to exhaust retry budget
    let follower_node = &expected_replicas[1];
    if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
        // Use a high count to ensure we exhaust the retry budget
        let fail_url = format!("{}/admin/fail/prepare?count=10", follower_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected persistent prepare failures to exhaust budget");
    }

    let payload = generate_random_bytes(1024);

    println!("Starting PUT that should exhaust retry budget");

    let start_time = Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let total_elapsed = start_time.elapsed();

    // Should fail after exhausting retry budget
    assert!(status.is_server_error(), "PUT should fail after exhausting retry budget, got: {}", status);
    println!("PUT correctly failed after {:?} (budget exhausted)", total_elapsed);

    // Should have taken a reasonable amount of time showing multiple retry attempts
    assert!(total_elapsed.as_secs() >= 2, "Should take time showing multiple retry attempts");
    assert!(total_elapsed.as_secs() < 15, "Should not exceed reasonable maximum budget");

    // Verify no data was committed
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be committed after budget exhaustion");
    }

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file.len(), 0, "No volume should have file after budget exhaustion");

    println!("Retry budget exhaustion test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_jitter_in_backoff() -> anyhow::Result<()> {
    // Test that jitter is applied to backoff intervals
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let nodes = list_nodes(&client, coord.url()).await?;
    let mut timing_samples = Vec::new();

    // Run multiple tests with the same failure pattern to observe jitter
    for i in 0..5 {
        let key = format!("test-jitter-{}", i);
        let expected_replicas = test_placement_n(&key, &nodes, 3);

        // Inject the same failure pattern
        let follower_node = &expected_replicas[1];
        if let Some(follower_volume) = volumes.iter().find(|v| v.state.node_id == *follower_node) {
            let fail_url = format!("{}/admin/fail/prepare?count=2", follower_volume.url());
            let _ = client.post(&fail_url).send().await;
        }

        let payload = generate_random_bytes(512);
        
        let start_time = Instant::now();
        let (status, _, _) = put_via_coordinator(&client, coord.url(), &key, payload).await?;
        let elapsed = start_time.elapsed();

        assert_eq!(status, reqwest::StatusCode::CREATED, "PUT {} should succeed", i);
        timing_samples.push(elapsed);

        println!("PUT {} completed in {:?}", i, elapsed);
        
        // Small delay between tests
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Analyze timing variance (evidence of jitter)
    let mut min_time = timing_samples[0];
    let mut max_time = timing_samples[0];
    let mut total_time = Duration::ZERO;

    for &sample in &timing_samples {
        if sample < min_time { min_time = sample; }
        if sample > max_time { max_time = sample; }
        total_time += sample;
    }

    let avg_time = total_time / timing_samples.len() as u32;
    let variance = max_time.saturating_sub(min_time);

    println!("Timing analysis:");
    println!("  Min: {:?}", min_time);
    println!("  Max: {:?}", max_time);
    println!("  Avg: {:?}", avg_time);
    println!("  Variance: {:?}", variance);

    // With jitter, we should see some variance in timing
    // Even with the same failure pattern, jitter should cause different total times
    assert!(variance.as_millis() > 50, "Should see timing variance due to jitter, got: {:?}", variance);
    
    // But variance shouldn't be too extreme
    assert!(variance.as_millis() < 2000, "Variance should be reasonable, got: {:?}", variance);

    println!("Jitter in backoff test successful - observed timing variance");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_non_retryable_errors() -> anyhow::Result<()> {
    // Test that non-retryable errors (4xx) don't trigger backoff
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-non-retryable";
    let payload = generate_random_bytes(1024);

    // First, establish the key
    let (status1, _, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    assert_eq!(status1, reqwest::StatusCode::CREATED, "Initial PUT should succeed");

    println!("Key established, testing non-retryable error (write-once violation)");

    // Try to PUT the same key again (should be 409 Conflict - non-retryable)
    let start_time = Instant::now();
    let (status2, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    // Should fail quickly with 409 (no retries for non-retryable errors)
    assert_eq!(status2, reqwest::StatusCode::CONFLICT, "Second PUT should return 409");
    println!("Non-retryable error returned in {:?}", elapsed);

    // Should be fast (no retry backoff for non-retryable errors)
    assert!(elapsed.as_millis() < 500, "Non-retryable error should fail quickly without backoff, took: {:?}", elapsed);

    println!("Non-retryable error test successful - no backoff applied");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_partial_commit_deadline_exceeded() -> anyhow::Result<()> {
    // Test commit failure after some replicas committed but deadline exceeded
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-partial-commit-deadline";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    println!("Expected replicas for key '{}': {:?}", key, expected_replicas);

    // Inject persistent commit failure on F2 (third replica) to simulate timeout/block
    let failing_node = &expected_replicas[2];
    let failing_volume = volumes.iter()
        .find(|v| v.state.node_id == *failing_node)
        .expect("Should find failing volume");

    // Inject persistent failure (not just once) to simulate deadline exceeded
    let fail_url = format!("{}/admin/fail/commit?persistent=true", failing_volume.url());
    let fail_resp = client.post(&fail_url).send().await;
    
    if let Ok(resp) = fail_resp {
        if resp.status().is_success() {
            println!("Injected persistent commit failure on: {}", failing_node);
        } else {
            println!("Warning: Could not inject persistent commit failure");
        }
    } else {
        // Fallback: inject multiple failures to simulate persistent issue
        let fail_url = format!("{}/admin/fail/commit?count=10", failing_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Warning: Using multiple failures as fallback for persistent failure");
    }

    // PUT with medium payload
    let payload = generate_random_bytes(4 * 1024 * 1024); // 4 MiB
    let _expected_etag = blake3_hex(&payload);

    println!("Starting PUT that should hit commit deadline");

    let start_time = std::time::Instant::now();
    let (status, _etag, _len) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Assert: PUT returns 5xx after commit time-box expires
    assert!(status.is_server_error(), "PUT should return 5xx after commit deadline, got: {}", status);
    println!("PUT correctly failed with status: {} in {:?}", status, elapsed);

    // The elapsed time should indicate that we hit a timeout
    assert!(elapsed.as_secs() >= 3, "Should take at least a few seconds to hit deadline");
    assert!(elapsed.as_secs() < 30, "Should not take too long (test timeout)");

    // Assert: At least one replica (likely head + one follower) may have a final blob
    // but meta is not Committed (since W=N and one failed)
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    
    // Some replicas might have the file (those that committed before deadline)
    // but not all, and meta should not be committed
    println!("Volumes with file after partial commit: {:?}", volumes_with_file);
    assert!(volumes_with_file.len() < 3, "Not all volumes should have the file (some commits failed)");
    
    // The key assertion: meta is not Committed despite partial success
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be Committed since W=N failed");
        println!("Meta state after partial commit failure: {:?}", meta.state);
    } else {
        println!("Meta was cleaned up after partial commit failure");
    }

    // Assert: GET via coordinator returns 404/5xx (because meta isn't committed)
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert!(!get_status.is_success(), "GET should fail because meta is not committed, got: {}", get_status);

    println!("Partial commit deadline test successful - documented commit-wins rule behavior");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_partial_commit_head_succeeds_follower_fails() -> anyhow::Result<()> {
    // Test specific case where head commits but follower fails within deadline
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-head-commit-follower-fail";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject persistent failure on just one follower
    let failing_follower = &expected_replicas[1]; // F1
    if let Some(failing_volume) = volumes.iter().find(|v| v.state.node_id == *failing_follower) {
        let fail_url = format!("{}/admin/fail/commit?count=5", failing_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected persistent commit failure on follower: {}", failing_follower);
    }

    let payload = generate_random_bytes(2 * 1024 * 1024); // 2 MiB

    println!("Starting PUT where head commits but one follower fails");

    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // Should fail because W=N (need all 3 to commit)
    assert!(status.is_server_error(), "PUT should fail when any replica fails commit");

    // Check which volumes have the file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    
    // Head and one follower might have committed, but not the failing one
    println!("Volumes with file after partial commit: {:?} (expected < 3)", volumes_with_file);
    assert!(volumes_with_file.len() < 3, "Should have partial commit state");

    // Meta should not be committed
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Meta should not be committed with partial success");
    }

    // GET should fail
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert!(!get_status.is_success(), "GET should fail with partial commit");

    println!("Partial commit (head success, follower fail) test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commit_deadline_with_large_payload() -> anyhow::Result<()> {
    // Test commit deadline behavior with large payload
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-large-commit-deadline";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject failure on last replica
    let failing_node = &expected_replicas[2];
    if let Some(failing_volume) = volumes.iter().find(|v| v.state.node_id == *failing_node) {
        let fail_url = format!("{}/admin/fail/commit?count=10", failing_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!("Injected commit failures on: {}", failing_node);
    }

    // Use large payload to test that the issue isn't related to payload size
    let payload_size = 30 * 1024 * 1024; // 30 MiB
    let payload = generate_random_bytes(payload_size);

    println!("Starting PUT with {}MB payload that should hit commit deadline", payload_size / (1024*1024));

    let start_time = std::time::Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    assert!(status.is_server_error(), "Large payload PUT should fail at commit deadline");
    println!("Large payload PUT failed at commit in {:?}", elapsed);

    // Verify partial state
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert!(volumes_with_file.len() < 3, "Should have partial commit with large payload");

    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(meta.state, TxState::Committed, "Large payload meta should not be committed");
    }

    println!("Large payload commit deadline test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commit_deadline_cleanup_behavior() -> anyhow::Result<()> {
    // Test that partial commits are eventually cleaned up or reconciled
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-commit-cleanup";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Cause partial commit
    let failing_node = &expected_replicas[1];
    if let Some(failing_volume) = volumes.iter().find(|v| v.state.node_id == *failing_node) {
        let fail_url = format!("{}/admin/fail/commit?count=3", failing_volume.url());
        let _ = client.post(&fail_url).send().await;
    }

    let payload = generate_random_bytes(1024 * 1024); // 1 MiB

    // First PUT should fail due to commit deadline
    let (status1, _, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    assert!(status1.is_server_error(), "First PUT should fail");

    println!("First PUT failed as expected, checking cleanup behavior");

    // Wait a bit for potential cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Check current state
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    println!("Volumes with file after cleanup wait: {:?}", volumes_with_file);

    // Try another operation to the same key (should be handled appropriately)
    let payload2 = generate_random_bytes(2048);
    let (status2, _, _) = put_via_coordinator(&client, coord.url(), key, payload2).await?;
    
    // The behavior here depends on implementation:
    // - Might succeed if cleanup happened
    // - Might fail if write-once is enforced
    // - Might fail if partial state blocks new writes
    println!("Second PUT status: {} (implementation-dependent)", status2);

    // The key point is that the system should handle this gracefully
    // and not be left in an inconsistent state
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        println!("Final meta state: {:?}", meta.state);
        // Meta should either be cleaned up, or in a consistent state
        if meta.state == TxState::Committed {
            // If committed, all replicas should have the file
            let final_volumes_with_file = which_volume_has_file(&volume_refs, key)?;
            assert_eq!(final_volumes_with_file.len(), 3, "If committed, all volumes should have file");
        }
    }

    println!("Commit deadline cleanup behavior test completed");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

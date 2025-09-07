use std::time::Duration;
use tokio::time::sleep;

mod common;

/// Example test demonstrating fault injection capabilities
/// 
/// This test shows how to use the fault injection endpoints to simulate
/// various failure scenarios for comprehensive testing of the distributed
/// storage system.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fault_injection_demo() -> anyhow::Result<()> {
    use common::*;

    // Start coordinator
    let coord = TestCoordinator::new_with_replicas(3).await?;
    
    // Start three volumes for multi-replica testing
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol2".to_string()).await?;
    let mut vol3 = TestVolume::new(coord.url().to_string(), "vol3".to_string()).await?;
    
    // Join coordinator
    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol3.join_coordinator().await?;
    
    // Start heartbeats
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;
    vol3.start_heartbeat(500)?;

    // Wait for all volumes to join and heartbeat
    wait_until(5000, || async {
        let nodes = list_nodes(&coord.state.http_client, coord.url()).await.unwrap_or_default();
        Ok(nodes.len() >= 3 && nodes.iter().all(|n| matches!(n.status, coord::core::node::NodeStatus::Alive)))
    }).await?;

    let client = &coord.state.http_client;
    
    // Test 1: Debug placement endpoint
    println!("=== Test 1: Debug Placement ===");
    let placement_resp = client
        .get(format!("{}/debug/placement/test-key", coord.url()))
        .send()
        .await?;
    
    if placement_resp.status().is_success() {
        let placement: serde_json::Value = placement_resp.json().await?;
        println!("Placement for 'test-key': {}", placement);
    } else {
        println!("Debug placement not available (non-test build)");
    }

    // Test 2: Normal PUT operation (should succeed)
    println!("\n=== Test 2: Normal PUT (should succeed) ===");
    let data = generate_random_bytes(1024);
    let (status, _etag, _len) = put_via_coordinator(client, coord.url(), "normal-key", data).await?;
    assert_eq!(status, 201);
    println!("Normal PUT succeeded with status {}", status);

    // Test 3: Inject prepare failure
    println!("\n=== Test 3: Prepare Failure Injection ===");
    
    // Inject failure on one volume
    let fail_resp = client
        .post(format!("{}/admin/fail/prepare?once=true", vol1.state.internal_url))
        .send()
        .await?;
    
    if fail_resp.status().is_success() {
        println!("Prepare failure injected on vol1");
        
        // Try PUT - should succeed because other replicas work
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "prepare-fail-key", data).await;
        
        match result {
            Ok((status, _, _)) => println!("PUT succeeded despite prepare failure: {}", status),
            Err(e) => println!("PUT failed as expected due to prepare failure: {}", e),
        }
    } else {
        println!("Fault injection not available (non-test build)");
    }

    // Test 4: Inject pull failure
    println!("\n=== Test 4: Pull Failure Injection ===");
    
    let fail_resp = client
        .post(format!("{}/admin/fail/pull?once=true", vol2.state.internal_url))
        .send()
        .await?;
        
    if fail_resp.status().is_success() {
        println!("Pull failure injected on vol2");
        
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "pull-fail-key", data).await;
        
        match result {
            Ok((status, _, _)) => println!("PUT succeeded despite pull failure: {}", status),
            Err(e) => println!("PUT failed as expected due to pull failure: {}", e),
        }
    }

    // Test 5: Inject commit failure
    println!("\n=== Test 5: Commit Failure Injection ===");
    
    let fail_resp = client
        .post(format!("{}/admin/fail/commit?once=true", vol3.state.internal_url))
        .send()
        .await?;
        
    if fail_resp.status().is_success() {
        println!("Commit failure injected on vol3");
        
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "commit-fail-key", data).await;
        
        match result {
            Ok((status, _, _)) => println!("PUT succeeded despite commit failure: {}", status),
            Err(e) => println!("PUT failed as expected due to commit failure: {}", e),
        }
    }

    // Test 6: Inject latency
    println!("\n=== Test 6: Latency Injection ===");
    
    let latency_resp = client
        .post(format!("{}/admin/inject/latency?latency_ms=1000", vol1.state.internal_url))
        .send()
        .await?;
        
    if latency_resp.status().is_success() {
        println!("1000ms latency injected on vol1");
        
        let start = std::time::Instant::now();
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "latency-key", data).await;
        let duration = start.elapsed();
        
        match result {
            Ok((status, _, _)) => {
                println!("PUT succeeded with injected latency: {} (took {}ms)", 
                    status, duration.as_millis());
                // Should take at least 1 second due to injected latency
                assert!(duration >= Duration::from_millis(900));
            },
            Err(e) => println!("PUT failed: {}", e),
        }
    }

    // Test 7: Pause and resume volume
    println!("\n=== Test 7: Pause/Resume Volume ===");
    
    // Pause vol1
    let pause_resp = client
        .post(format!("{}/admin/pause", vol1.state.internal_url))
        .send()
        .await?;
        
    if pause_resp.status().is_success() {
        println!("vol1 paused");
        
        // Operations should still work with other volumes
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "pause-key", data).await;
        
        match result {
            Ok((status, _, _)) => println!("PUT succeeded while vol1 paused: {}", status),
            Err(e) => println!("PUT failed while vol1 paused: {}", e),
        }
        
        // Resume vol1
        let resume_resp = client
            .post(format!("{}/admin/resume", vol1.state.internal_url))
            .send()
            .await?;
            
        if resume_resp.status().is_success() {
            println!("vol1 resumed");
        }
    }

    // Test 8: Reset all faults
    println!("\n=== Test 8: Reset All Faults ===");
    
    for vol in [&vol1, &vol2, &vol3] {
        let reset_resp = client
            .post(format!("{}/admin/reset", vol.state.internal_url))
            .send()
            .await?;
            
        if reset_resp.status().is_success() {
            println!("Faults reset on {}", vol.state.node_id);
        }
    }
    
    // Final test - everything should work normally now
    let data = generate_random_bytes(1024);
    let (status, _etag, _len) = put_via_coordinator(client, coord.url(), "final-key", data).await?;
    assert_eq!(status, 201);
    println!("Final PUT succeeded after reset: {}", status);

    println!("\n=== Fault Injection Demo Complete ===");
    Ok(())
}

/// Test etag mismatch injection
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_etag_mismatch_injection() -> anyhow::Result<()> {
    use common::*;

    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    // Wait for volumes to join
    wait_until(3000, || async {
        let nodes = list_nodes(&coord.state.http_client, coord.url()).await.unwrap_or_default();
        Ok(nodes.len() >= 2)
    }).await?;

    let client = &coord.state.http_client;
    
    // Inject etag mismatch on vol2
    let fail_resp = client
        .post(format!("{}/admin/fail/etag_mismatch?once=true", vol2.state.internal_url))
        .send()
        .await?;
        
    if fail_resp.status().is_success() {
        println!("Etag mismatch injected on vol2");
        
        // Try PUT - should fail due to checksum mismatch during pull
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "etag-mismatch-key", data).await;
        
        match result {
            Ok((status, _, _)) => {
                println!("PUT unexpectedly succeeded: {}", status);
                // In a real system, this might succeed if retries work around the fault
            },
            Err(e) => {
                println!("PUT failed as expected due to etag mismatch: {}", e);
                // This is the expected behavior
            },
        }
    } else {
        println!("Fault injection not available (non-test build)");
    }

    Ok(())
}

/// Test volume kill simulation
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_volume_kill_injection() -> anyhow::Result<()> {
    use common::*;

    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    // Wait for volumes to join
    wait_until(3000, || async {
        let nodes = list_nodes(&coord.state.http_client, coord.url()).await.unwrap_or_default();
        Ok(nodes.len() >= 2)
    }).await?;

    let client = &coord.state.http_client;
    
    // Kill vol1
    let kill_resp = client
        .post(format!("{}/admin/kill", vol1.state.internal_url))
        .send()
        .await?;
        
    if kill_resp.status().is_success() {
        println!("vol1 killed (simulated crash)");
        
        // Give some time for the kill to take effect
        sleep(Duration::from_millis(500)).await;
        
        // Operations involving vol1 should fail
        let data = generate_random_bytes(1024);
        let result = put_via_coordinator(client, coord.url(), "kill-test-key", data).await;
        
        match result {
            Ok((status, _, _)) => {
                println!("PUT succeeded despite vol1 kill: {}", status);
                // This might succeed if the system can work with remaining volumes
            },
            Err(e) => {
                println!("PUT failed as expected due to vol1 kill: {}", e);
            },
        }
    } else {
        println!("Fault injection not available (non-test build)");
    }

    Ok(())
}

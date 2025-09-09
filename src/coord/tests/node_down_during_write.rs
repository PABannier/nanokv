use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;
use coord::core::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_follower_down_during_write_abort() -> anyhow::Result<()> {
    // Test follower node down during write -> abort
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-follower-down-write";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    println!(
        "Expected replicas for key '{}': {:?}",
        key, expected_replicas
    );

    // Create a larger payload to ensure the write takes some time
    let payload_size = 20 * 1024 * 1024; // 20 MiB
    let payload = generate_random_bytes(payload_size);

    // Identify the follower we'll kill (F2 = third replica)
    let follower_to_kill = &expected_replicas[2];
    let follower_index = volumes
        .iter()
        .position(|v| v.state.node_id == *follower_to_kill)
        .expect("Should find follower volume");

    println!("Will kill follower {} during write", follower_to_kill);

    // Start the PUT operation in a background task
    let client_clone = client.clone();
    let coord_url = coord.url().to_string();
    let key_clone = key.to_string();
    let payload_clone = payload.clone();

    let put_task = tokio::spawn(async move {
        put_via_coordinator(&client_clone, &coord_url, &key_clone, payload_clone).await
    });

    // Wait a short time to let the PUT start and get to the pull phase
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Kill the follower server (stop its server and heartbeats)
    let follower_volume = volumes.remove(follower_index);
    println!("Killing follower volume: {}", follower_to_kill);
    follower_volume.shutdown().await?;

    // Wait for the coordinator to detect the node as down
    // This might take a bit due to heartbeat intervals
    wait_until(10000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        let down_node = nodes.iter().find(|n| n.node_id == *follower_to_kill);
        Ok(down_node.is_none_or(|n| n.status != NodeStatus::Alive))
    })
    .await
    .unwrap_or_else(|_| {
        println!("Warning: Node may not have been marked down yet");
    });

    // Wait for the PUT to complete
    let put_result = put_task.await?;
    let (status, _etag, _len) = put_result?;

    // Assert: PUT fails and aborts; no final blob exists on any node; meta not Committed
    assert!(
        status.is_server_error(),
        "PUT should fail when follower goes down, got: {}",
        status
    );
    println!("PUT correctly failed with status: {}", status);

    // Check that no final files exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No remaining volume should have final blob after abort"
    );

    // Check meta state
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be Committed after node failure"
        );
        println!("Meta state after node failure: {:?}", meta.state);
    } else {
        println!("Meta was cleaned up after abort");
    }

    // Verify GET fails
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert!(
        !get_status.is_success(),
        "GET should fail after aborted write"
    );

    println!("Follower down during write test successful - transaction aborted");

    // Cleanup remaining volumes
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_head_down_during_write_abort() -> anyhow::Result<()> {
    // Test head node down during write -> abort
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-head-down-write";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Create a large payload
    let payload_size = 25 * 1024 * 1024; // 25 MiB
    let payload = generate_random_bytes(payload_size);

    // Identify the head node to kill
    let head_to_kill = &expected_replicas[0];
    let head_index = volumes
        .iter()
        .position(|v| v.state.node_id == *head_to_kill)
        .expect("Should find head volume");

    println!("Will kill head node {} during write", head_to_kill);

    // Start PUT operation
    let client_clone = client.clone();
    let coord_url = coord.url().to_string();
    let key_clone = key.to_string();
    let payload_clone = payload.clone();

    let put_task = tokio::spawn(async move {
        put_via_coordinator(&client_clone, &coord_url, &key_clone, payload_clone).await
    });

    // Wait for write to start, then kill head during write phase
    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

    // Kill the head node
    let head_volume = volumes.remove(head_index);
    println!("Killing head volume: {}", head_to_kill);
    head_volume.shutdown().await?;

    // Wait for PUT to complete
    let put_result = put_task.await?;
    let (status, _, _) = put_result?;

    // Assert: failure and abort; no finals anywhere; tmp files may remain and be cleaned later
    assert!(
        status.is_server_error(),
        "PUT should fail when head goes down, got: {}",
        status
    );
    println!("PUT correctly failed with status: {}", status);

    // Check no final files exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No remaining volume should have final blob"
    );

    // Check meta state
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be Committed after head failure"
        );
    }

    println!("Head down during write test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_node_pause_during_pull_timeout() -> anyhow::Result<()> {
    // Test node pause/slow response during pull causing timeout beyond retry budget
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-node-pause-pull";
    let nodes = list_nodes(&client, coord.url()).await?;
    let expected_replicas = test_placement_n(key, &nodes, 3);

    // Inject read_tmp failure/timeout on head node to force pull timeouts
    let head_node = &expected_replicas[0];
    if let Some(head_volume) = volumes.iter().find(|v| v.state.node_id == *head_node) {
        // Use many failures to ensure retry budget is exhausted
        let fail_url = format!("{}/admin/fail/read_tmp?count=50", head_volume.url());
        let _ = client.post(&fail_url).send().await;
        println!(
            "Injected persistent read_tmp failures on head: {}",
            head_node
        );
    }

    let payload = generate_random_bytes(15 * 1024 * 1024); // 15 MiB

    println!("Starting PUT that should timeout during pull phase");

    let start_time = std::time::Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    // Should fail due to timeout beyond retry budget
    assert!(
        status.is_server_error(),
        "PUT should fail due to pull timeout, got: {}",
        status
    );
    println!("PUT failed due to pull timeout in {:?}", elapsed);

    // Should have taken some time due to retries
    assert!(
        elapsed.as_secs() >= 2,
        "Should take time due to retry attempts"
    );

    // No final files should exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No volume should have final file after timeout"
    );

    // Meta should not be committed
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be committed after timeout"
        );
    }

    println!("Node pause during pull timeout test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_nodes_down_no_quorum() -> anyhow::Result<()> {
    // Test multiple nodes down causing no quorum for write
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-no-quorum";

    // Kill 2 out of 3 nodes to lose quorum
    let vol1 = volumes.remove(0);
    let vol2 = volumes.remove(0);

    println!("Killing 2 volumes to lose quorum");
    vol1.shutdown().await?;
    vol2.shutdown().await?;

    // Wait for coordinator to detect nodes as down
    wait_until(8000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        let alive_count = nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Alive)
            .count();
        Ok(alive_count < 3) // Less than required replicas
    })
    .await
    .unwrap_or_else(|_| {
        println!("Warning: Nodes may not be marked down yet");
    });

    let payload = generate_random_bytes(1024);

    println!("Attempting PUT with insufficient quorum");

    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;

    // Should fail due to no quorum
    assert!(
        status.is_server_error(),
        "PUT should fail with no quorum, got: {}",
        status
    );
    println!("PUT correctly failed with no quorum: {}", status);

    // No files should exist
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No volume should have file with no quorum"
    );

    // No committed meta
    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Meta should not be committed with no quorum"
        );
    }

    println!("No quorum test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_node_recovery_after_failure() -> anyhow::Result<()> {
    // Test that system can recover and handle writes after node failure
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    // First write should fail due to node going down
    let key1 = "test-recovery-fail";
    let nodes = list_nodes(&client, coord.url()).await?;
    let _expected_replicas = test_placement_n(key1, &nodes, 3);

    let payload1 = generate_random_bytes(5 * 1024 * 1024); // 5 MiB

    // Kill a follower during the first write
    let follower_index = 1; // Kill second volume
    let killed_volume = volumes.remove(follower_index);
    println!("Killing volume for recovery test");
    killed_volume.shutdown().await?;

    // First PUT should fail
    let (status1, _, _) = put_via_coordinator(&client, coord.url(), key1, payload1).await?;
    assert!(
        status1.is_server_error(),
        "First PUT should fail with node down"
    );

    // Now start a replacement volume
    let mut new_volume =
        TestVolume::new(coord.url().to_string(), "vol-replacement".to_string()).await?;
    new_volume.join_coordinator().await?;
    new_volume.start_heartbeat(500)?;
    volumes.push(new_volume);

    // Wait for new volume to be alive
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    // Second write should succeed with recovered quorum
    let key2 = "test-recovery-success";
    let payload2 = generate_random_bytes(2 * 1024 * 1024); // 2 MiB
    let expected_etag2 = blake3_hex(&payload2);

    println!("Attempting write after recovery");

    let (status2, etag2, _) =
        put_via_coordinator(&client, coord.url(), key2, payload2.clone()).await?;

    // Should succeed
    assert_eq!(
        status2,
        reqwest::StatusCode::CREATED,
        "PUT should succeed after recovery"
    );
    assert_eq!(etag2, expected_etag2, "ETag should match");

    // Verify successful write
    let meta2 = meta_of(&coord.state.db, key2)?.expect("Meta should exist after recovery");
    assert_eq!(meta2.state, TxState::Committed, "Meta should be committed");

    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key2)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All volumes should have file after recovery"
    );

    // Verify data integrity
    let redirect_client = create_redirect_client()?;
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key2).await?;
    assert_eq!(
        response_bytes, payload2,
        "Data should be correct after recovery"
    );

    println!("Node recovery test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

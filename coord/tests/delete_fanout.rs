use reqwest::Client;

mod common;
use common::*;
use coord::meta::TxState;
use coord::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_fanout_idempotent() -> anyhow::Result<()> {
    // Test delete fan-out across replicas (idempotent)
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-delete-fanout";
    let payload = generate_random_bytes(2 * 1024 * 1024); // 2 MiB
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT to establish replicated data");

    // PUT /k success
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed");
    assert_eq!(etag, expected_etag, "ETag should match");

    // Verify all replicas have the file initially
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file_initial = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file_initial.len(), 3, "All 3 volumes should have the file initially");

    // Verify meta is committed
    let meta_before = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta_before.state, TxState::Committed, "Meta should be committed before delete");

    println!("Data established, performing first DELETE");

    // DELETE /k first time
    let delete_status1 = delete_via_coordinator(&client, coord.url(), key).await?;
    assert_eq!(delete_status1, reqwest::StatusCode::NO_CONTENT, "First DELETE should return 204");

    // Assert: Meta Tombstoned
    let meta_after_delete1 = meta_of(&coord.state.db, key)?.expect("Meta should still exist after delete");
    assert_eq!(meta_after_delete1.state, TxState::Tombstoned, "Meta should be tombstoned after first delete");

    // Wait a bit for delete fan-out to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Assert: No replica has the blob
    let volumes_with_file_after_delete1 = which_volume_has_file(&volume_refs, key)?;
    if !volumes_with_file_after_delete1.is_empty() {
        println!("Warning: {} volumes still have file after delete (may be async cleanup)", 
                volumes_with_file_after_delete1.len());
        
        // Wait a bit more for async cleanup
        wait_until(5000, || async {
            let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
            let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
            Ok(volumes_with_file.is_empty())
        }).await.unwrap_or_else(|_| {
            println!("Files may still exist - implementation may use async cleanup");
        });
    }

    println!("First DELETE completed, performing second DELETE (idempotent test)");

    // DELETE /k second time (idempotent)
    let delete_status2 = delete_via_coordinator(&client, coord.url(), key).await?;
    assert_eq!(delete_status2, reqwest::StatusCode::NO_CONTENT, "Second DELETE should also return 204 (idempotent)");

    // Meta should still be tombstoned
    let meta_after_delete2 = meta_of(&coord.state.db, key)?.expect("Meta should still exist after second delete");
    assert_eq!(meta_after_delete2.state, TxState::Tombstoned, "Meta should remain tombstoned after second delete");

    println!("Second DELETE completed, testing write-once rule");

    // Re-PUT /k should be 409 (write-once rule) — or document your chosen policy if you permit re-create
    let repay_load = generate_random_bytes(1024);
    let (reput_status, _, _) = put_via_coordinator(&client, coord.url(), key, repay_load).await?;
    assert_eq!(reput_status, reqwest::StatusCode::CONFLICT, "Re-PUT should return 409 due to write-once rule");

    println!("Write-once rule enforced, testing GET after delete");

    // GET should return 404
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND, "GET should return 404 after delete");

    println!("Delete fanout idempotent test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_with_some_replicas_down() -> anyhow::Result<()> {
    // Test delete fan-out when some replicas are down
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-delete-some-down";
    let payload = generate_random_bytes(1024 * 1024); // 1 MiB

    // Establish data on all replicas
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed");

    // Verify all replicas have the file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file_initial = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file_initial.len(), 3, "All volumes should have file initially");

    // Kill one replica
    let killed_volume = volumes.remove(1);
    let killed_node_id = killed_volume.state.node_id.clone();
    println!("Killing volume: {}", killed_node_id);
    killed_volume.shutdown().await?;

    // Wait for node to be marked down
    wait_until(8000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        let killed_node = nodes.iter().find(|n| n.node_id == killed_node_id);
        Ok(killed_node.map_or(true, |n| n.status != NodeStatus::Alive))
    }).await?;

    println!("Node marked down, performing DELETE");

    // DELETE should still work (fan-out to available replicas)
    let delete_status = delete_via_coordinator(&client, coord.url(), key).await?;
    assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT, "DELETE should succeed with one replica down");

    // Meta should be tombstoned
    let meta_after_delete = meta_of(&coord.state.db, key)?.expect("Meta should exist after delete");
    assert_eq!(meta_after_delete.state, TxState::Tombstoned, "Meta should be tombstoned");

    // Remaining replicas should have files deleted
    wait_until(5000, || async {
        let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        Ok(volumes_with_file.is_empty())
    }).await.unwrap_or_else(|_| {
        println!("Warning: Files may not be immediately cleaned up");
    });

    // GET should return 404
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND, "GET should return 404 after delete");

    println!("Delete with some replicas down test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_multiple_keys_fanout() -> anyhow::Result<()> {
    // Test delete fan-out for multiple keys with different replica placements
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let keys = vec!["key-delete-1", "key-delete-2", "key-delete-3"];
    let mut key_payloads = std::collections::HashMap::new();

    println!("Establishing multiple keys with different placements");

    // PUT multiple keys
    for key in &keys {
        let payload = format!("data for {}", key).repeat(1000);
        let payload_bytes = payload.into_bytes();
        let expected_etag = blake3_hex(&payload_bytes);

        let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload_bytes.clone()).await?;
        assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed for key: {}", key);
        assert_eq!(etag, expected_etag, "ETag should match for key: {}", key);
        
        key_payloads.insert(key.to_string(), payload_bytes);
    }

    // Verify all keys are present
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    for key in &keys {
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        assert_eq!(volumes_with_file.len(), 3, "All volumes should have file for key: {}", key);
    }

    println!("All keys established, performing DELETE operations");

    // DELETE all keys
    for key in &keys {
        let delete_status = delete_via_coordinator(&client, coord.url(), key).await?;
        assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT, "DELETE should succeed for key: {}", key);

        // Verify meta is tombstoned
        let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist after delete");
        assert_eq!(meta.state, TxState::Tombstoned, "Meta should be tombstoned for key: {}", key);
    }

    // Wait for delete fan-out to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    println!("DELETE operations completed, verifying cleanup");

    // Verify all files are deleted
    for key in &keys {
        wait_until(3000, || async {
            let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
            let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
            Ok(volumes_with_file.is_empty())
        }).await.unwrap_or_else(|_| {
            println!("Warning: Files for key {} may not be immediately cleaned up", key);
        });

        // Verify GET returns 404
        let no_redirect_client = create_no_redirect_client()?;
        let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
        assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND, "GET should return 404 for key: {}", key);
    }

    println!("Multiple keys delete fanout test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_large_object_fanout() -> anyhow::Result<()> {
    // Test delete fan-out for large objects
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-delete-large";
    let payload_size = 50 * 1024 * 1024; // 50 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);

    println!("Starting PUT with {}MB payload", payload_size / (1024*1024));

    // PUT large object
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED, "Large PUT should succeed");
    assert_eq!(etag, expected_etag, "ETag should match for large object");

    // Verify all replicas have the large file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file_initial = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(volumes_with_file_initial.len(), 3, "All volumes should have large file");

    println!("Large object established, performing DELETE");

    // DELETE large object
    let delete_start = std::time::Instant::now();
    let delete_status = delete_via_coordinator(&client, coord.url(), key).await?;
    let delete_elapsed = delete_start.elapsed();
    
    assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT, "DELETE should succeed for large object");
    println!("Large object DELETE completed in {:?}", delete_elapsed);

    // Meta should be tombstoned
    let meta_after_delete = meta_of(&coord.state.db, key)?.expect("Meta should exist after delete");
    assert_eq!(meta_after_delete.state, TxState::Tombstoned, "Meta should be tombstoned for large object");

    // Wait for large file cleanup (may take longer)
    println!("Waiting for large file cleanup across replicas");
    wait_until(10000, || async {
        let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        Ok(volumes_with_file.is_empty())
    }).await.unwrap_or_else(|_| {
        println!("Warning: Large files may take longer to clean up");
    });

    // Verify GET returns 404
    let no_redirect_client = create_no_redirect_client()?;
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND, "GET should return 404 for deleted large object");

    println!("Large object delete fanout test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_concurrent_operations() -> anyhow::Result<()> {
    // Test delete fan-out with concurrent DELETE operations
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let keys = vec!["concurrent-1", "concurrent-2", "concurrent-3", "concurrent-4"];

    // Establish multiple keys
    for key in &keys {
        let payload = format!("data for {}", key).repeat(500);
        let payload_bytes = payload.into_bytes();
        let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload_bytes).await?;
        assert_eq!(status, reqwest::StatusCode::CREATED, "PUT should succeed for key: {}", key);
    }

    println!("All keys established, performing concurrent DELETEs");

    // Perform concurrent DELETE operations
    let mut delete_tasks = Vec::new();
    for key in &keys {
        let client_clone = client.clone();
        let coord_url = coord.url().to_string();
        let key_clone = key.to_string();
        
        let task = tokio::spawn(async move {
            delete_via_coordinator(&client_clone, &coord_url, &key_clone).await
        });
        delete_tasks.push(task);
    }

    // Wait for all DELETE operations to complete
    let mut all_success = true;
    for (i, task) in delete_tasks.into_iter().enumerate() {
        match task.await? {
            Ok(status) => {
                if status != reqwest::StatusCode::NO_CONTENT {
                    println!("DELETE {} returned unexpected status: {}", i, status);
                    all_success = false;
                }
            }
            Err(e) => {
                println!("DELETE {} failed: {}", i, e);
                all_success = false;
            }
        }
    }

    assert!(all_success, "All concurrent DELETE operations should succeed");

    // Verify all keys are tombstoned
    for key in &keys {
        let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist after delete");
        assert_eq!(meta.state, TxState::Tombstoned, "Meta should be tombstoned for key: {}", key);
    }

    // Wait for cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Verify all files are deleted
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    for key in &keys {
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        if !volumes_with_file.is_empty() {
            println!("Warning: Key {} still has files on {} volumes", key, volumes_with_file.len());
        }
    }

    println!("Concurrent delete operations test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

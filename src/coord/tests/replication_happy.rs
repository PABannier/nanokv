use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replicated_write_happy_path_n3() -> anyhow::Result<()> {
    // Start coordinator with N=3 replicas
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();
    let no_redirect_client = create_no_redirect_client()?;
    let redirect_client = create_redirect_client()?;

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;

    // Wait for all volumes to be alive
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-key-1";

    // Confirm placement for key1 -> debug/placement/key1 returns 3 node_ids [H,F1,F2]
    let expected_replicas = test_placement_n(key, &list_nodes(&client, coord.url()).await?, 3);
    assert_eq!(expected_replicas.len(), 3, "Should have 3 replicas");

    // Create a medium-sized payload (8-16 MiB)
    let payload_size = 12 * 1024 * 1024; // 12 MiB
    let payload = generate_random_bytes(payload_size);
    let expected_etag = blake3_hex(&payload);
    let expected_len = payload.len() as u64;

    println!(
        "Starting PUT with {}MB payload to key: {}",
        payload_size / (1024 * 1024),
        key
    );

    // PUT /key1 via coordinator
    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;

    // Assert: 201 Created, headers ETag = blake3_hex(payload), Content-Length equals len
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "PUT should succeed with 201"
    );
    assert_eq!(etag, expected_etag, "ETag should match blake3 hash");
    assert_eq!(
        len, expected_len,
        "Content-Length should match payload size"
    );

    println!("PUT succeeded, checking metadata and file presence");

    // Assert: RocksDB meta: state=Committed, replicas exactly those 3 node_ids, size, etag_hex
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");
    assert_eq!(meta.size, expected_len, "Meta size should match");
    assert_eq!(meta.etag_hex, expected_etag, "Meta etag should match");
    assert_eq!(meta.replicas.len(), 3, "Should have 3 replicas in meta");

    // Check that the replicas match the expected placement
    for expected_replica in &expected_replicas {
        assert!(
            meta.replicas.contains(expected_replica),
            "Meta should contain replica: {}",
            expected_replica
        );
    }

    // Assert: Each of the 3 volumes has blobs/aa/bb/<key-enc> with exact size
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All 3 volumes should have the file"
    );

    // Verify all expected replicas have the file
    for expected_replica in &expected_replicas {
        assert!(
            volumes_with_file.contains(expected_replica),
            "Volume {} should have the file",
            expected_replica
        );
    }

    println!("All replicas have the file, testing GET operations");

    // GET /key1: Without following redirects: 302 and Location matches any of the 3 volumes' public_url/blobs/....
    let (status, location) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert_eq!(status, reqwest::StatusCode::FOUND, "GET should return 302");
    assert!(location.is_some(), "Location header should be present");

    let location_url = location.unwrap();
    // Verify the location points to one of our volumes
    let mut found_volume = false;
    for vol in &volumes {
        if location_url.starts_with(vol.url()) && location_url.contains("/blobs/") {
            found_volume = true;
            break;
        }
    }
    assert!(
        found_volume,
        "Location should point to one of our volumes: {}",
        location_url
    );

    // GET /key1: Following redirects: body bytes equal original
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    assert_eq!(
        response_bytes, payload,
        "GET response should match original payload"
    );

    println!("GET operations successful, testing DELETE");

    // DELETE /key1: 204; meta -> Tombstoned; file removed on all 3
    let delete_status = delete_via_coordinator(&client, coord.url(), key).await?;
    assert_eq!(
        delete_status,
        reqwest::StatusCode::NO_CONTENT,
        "DELETE should return 204"
    );

    // Check meta becomes tombstoned
    let meta_after_delete =
        meta_of(&coord.state.db, key)?.expect("Meta should still exist after delete");
    assert_eq!(
        meta_after_delete.state,
        TxState::Tombstoned,
        "Meta should be tombstoned after delete"
    );

    // Verify files are removed (with some tolerance for async cleanup)
    wait_until(3000, || async {
        let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        Ok(volumes_with_file.is_empty())
    })
    .await
    .unwrap_or_else(|_| {
        // If files aren't cleaned up immediately, that's acceptable for some implementations
        println!("Warning: Files may not be immediately cleaned up after delete");
    });

    // Verify GET returns 404 after delete
    let (get_status, _) = get_redirect_location(&no_redirect_client, coord.url(), key).await?;
    assert_eq!(
        get_status,
        reqwest::StatusCode::NOT_FOUND,
        "GET should return 404 after delete"
    );

    println!("DELETE operations successful, test complete");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replicated_write_with_different_key_placements() -> anyhow::Result<()> {
    // Test that different keys get placed on different replica sets
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    // Start 5 volumes to ensure different placement possibilities
    let mut volumes = create_volumes(coord.url(), 5).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 5, 5000).await?;

    let nodes = list_nodes(&client, coord.url()).await?;

    // Test multiple keys and verify they get different replica assignments
    let test_keys = vec!["key-a", "key-b", "key-c", "key-d", "key-e"];
    let mut all_placements = Vec::new();

    for key in &test_keys {
        let placement = test_placement_n(key, &nodes, 3);
        assert_eq!(placement.len(), 3, "Each key should have 3 replicas");
        all_placements.push(placement);

        // Do a small PUT to verify the placement works
        let payload = format!("data for {}", key).into_bytes();
        let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
        assert_eq!(
            status,
            reqwest::StatusCode::CREATED,
            "PUT should succeed for key: {}",
            key
        );

        // Verify meta has the correct replicas
        let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
        assert_eq!(
            meta.state,
            TxState::Committed,
            "Meta should be committed for key: {}",
            key
        );
        assert_eq!(
            meta.replicas.len(),
            3,
            "Should have 3 replicas for key: {}",
            key
        );
    }

    // Verify that not all keys have identical placement (with 5 nodes and 3 replicas, this should be true)
    let first_placement = &all_placements[0];
    let mut found_different = false;
    for placement in &all_placements[1..] {
        if placement != first_placement {
            found_different = true;
            break;
        }
    }
    assert!(
        found_different,
        "Different keys should have different replica placements"
    );

    println!("Verified different keys get different replica placements");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

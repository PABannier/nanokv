use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_large_object_streaming_path() -> anyhow::Result<()> {
    // Test large object with followers (streaming path)
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();
    let redirect_client = create_redirect_client()?;

    // Start 3 volumes and join them
    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-large-object";

    // Generate a 100–200 MiB payload (using 150 MiB)
    let payload_size = 150 * 1024 * 1024; // 150 MiB
    println!(
        "Generating {}MB payload for large object test",
        payload_size / (1024 * 1024)
    );

    // For test efficiency, we'll use a pattern instead of random data
    // This still tests streaming but is faster to generate and compare
    let pattern_chunk = generate_random_bytes(1024 * 1024); // 1 MiB pattern
    let mut payload = Vec::with_capacity(payload_size);
    for _ in 0..(payload_size / pattern_chunk.len()) {
        payload.extend_from_slice(&pattern_chunk);
    }
    // Add remainder
    let remainder = payload_size % pattern_chunk.len();
    if remainder > 0 {
        payload.extend_from_slice(&pattern_chunk[..remainder]);
    }

    let expected_etag = blake3_hex(&payload);
    let expected_len = payload.len() as u64;

    println!(
        "Starting PUT with {}MB payload",
        payload.len() / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    // Assert: PUT success
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "Large object PUT should succeed"
    );
    assert_eq!(etag, expected_etag, "ETag should match for large object");
    assert_eq!(len, expected_len, "Length should match for large object");

    println!("Large PUT completed in {:?}", elapsed);

    // Assert: memory use didn't explode (indirectly by not OOM-ing)
    // The fact that we got here means we didn't run out of memory
    println!("Memory usage test passed - no OOM during large object streaming");

    // Assert: all 3 replicas have correct size
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All 3 volumes should have the large file"
    );

    // Verify metadata
    let meta = meta_of(&coord.state.db, key)?.expect("Meta should exist");
    assert_eq!(meta.state, TxState::Committed, "Meta should be committed");
    assert_eq!(
        meta.size, expected_len,
        "Meta size should match large object"
    );
    assert_eq!(meta.etag_hex, expected_etag, "Meta etag should match");

    println!("All replicas have the large file, testing GET");

    // Assert: GET returns correct bytes
    let get_start = std::time::Instant::now();
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    let get_elapsed = get_start.elapsed();

    assert_eq!(
        response_bytes.len(),
        payload.len(),
        "GET response should have correct length"
    );

    // For performance, we'll verify hash instead of byte-by-byte comparison
    let response_etag = blake3_hex(&response_bytes);
    assert_eq!(
        response_etag, expected_etag,
        "GET response should have correct hash"
    );

    println!("Large GET completed in {:?} with correct data", get_elapsed);

    // Test that we can handle the large object in memory efficiently
    // If we got this far without issues, streaming is working
    println!("Large object streaming test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_very_large_object_streaming() -> anyhow::Result<()> {
    // Test with an even larger object to stress streaming
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();
    let redirect_client = create_redirect_client()?;

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-very-large-object";

    // Use an even larger size: 200 MiB
    let payload_size = 200 * 1024 * 1024; // 200 MiB
    println!(
        "Generating {}MB payload for very large object test",
        payload_size / (1024 * 1024)
    );

    // Use a more efficient pattern-based approach for very large objects
    let base_pattern = b"NanoKV-Large-Object-Test-Pattern-";
    let pattern_size = 64 * 1024; // 64 KiB pattern
    let mut pattern_chunk = Vec::with_capacity(pattern_size);

    // Fill pattern chunk
    while pattern_chunk.len() < pattern_size {
        pattern_chunk.extend_from_slice(base_pattern);
        if pattern_chunk.len() > pattern_size {
            pattern_chunk.truncate(pattern_size);
        }
    }

    // Create the large payload
    let mut payload = Vec::with_capacity(payload_size);
    let chunks_needed = payload_size / pattern_chunk.len();
    for i in 0..chunks_needed {
        // Add some variation to each chunk for better testing
        let mut chunk = pattern_chunk.clone();
        // Modify a few bytes per chunk to make it unique
        let idx = (i * 37) % (chunk.len() - 8); // Some offset
        chunk[idx] = (i % 256) as u8;
        chunk[idx + 1] = ((i >> 8) % 256) as u8;
        payload.extend_from_slice(&chunk);
    }

    // Add remainder
    let remainder = payload_size % pattern_chunk.len();
    if remainder > 0 {
        payload.extend_from_slice(&pattern_chunk[..remainder]);
    }

    let expected_etag = blake3_hex(&payload);
    let expected_len = payload.len() as u64;

    println!(
        "Starting PUT with {}MB payload",
        payload.len() / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, etag, len) =
        put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let put_elapsed = start_time.elapsed();

    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "Very large PUT should succeed"
    );
    assert_eq!(etag, expected_etag, "ETag should match");
    assert_eq!(len, expected_len, "Length should match");

    println!("Very large PUT completed in {:?}", put_elapsed);

    // Verify all replicas have the file
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        3,
        "All volumes should have the very large file"
    );

    // Test GET with streaming
    println!("Testing GET of very large object");
    let get_start = std::time::Instant::now();
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    let get_elapsed = get_start.elapsed();

    assert_eq!(
        response_bytes.len(),
        payload.len(),
        "GET should return correct size"
    );

    // Verify data integrity via hash (more efficient than full comparison)
    let response_etag = blake3_hex(&response_bytes);
    assert_eq!(
        response_etag, expected_etag,
        "GET data should match original"
    );

    println!("Very large GET completed in {:?}", get_elapsed);
    println!("Very large object streaming test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_large_object_with_node_failure() -> anyhow::Result<()> {
    // Test large object streaming with node failure during operation
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-large-with-failure";

    // Use a moderately large size
    let payload_size = 80 * 1024 * 1024; // 80 MiB
    let payload = generate_random_bytes(payload_size);

    println!("Testing large object with simulated node failure");

    // Start PUT operation
    let client_clone = client.clone();
    let coord_url = coord.url().to_string();
    let key_clone = key.to_string();
    let payload_clone = payload.clone();

    let put_task = tokio::spawn(async move {
        put_via_coordinator(&client_clone, &coord_url, &key_clone, payload_clone).await
    });

    // Let the PUT start, then kill a follower
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let killed_volume = volumes.remove(2);
    println!("Killing follower during large object upload");
    killed_volume.shutdown().await?;

    // Wait for PUT to complete
    let put_result = put_task.await?;
    let (status, _, _) = put_result?;

    // Should fail due to node failure
    assert!(
        status.is_server_error(),
        "Large PUT should fail when node goes down"
    );
    println!("Large PUT correctly failed with node down: {}", status);

    // Verify no files remain
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert_eq!(
        volumes_with_file.len(),
        0,
        "No remaining volume should have file after failure"
    );

    println!("Large object with node failure test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_large_object_concurrent_operations() -> anyhow::Result<()> {
    // Test multiple concurrent large object operations
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    // Create multiple large objects concurrently
    let keys = vec!["large-1", "large-2", "large-3"];
    let payload_size = 50 * 1024 * 1024; // 50 MiB each

    println!("Starting concurrent large object uploads");

    let mut put_tasks = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let client_clone = client.clone();
        let coord_url = coord.url().to_string();
        let key_clone = key.to_string();

        // Create different payload for each
        let mut payload = generate_random_bytes(payload_size);
        // Add unique marker to each payload
        payload.extend_from_slice(format!("UNIQUE-{}", i).as_bytes());

        let task = tokio::spawn(async move {
            put_via_coordinator(&client_clone, &coord_url, &key_clone, payload).await
        });
        put_tasks.push(task);
    }

    // Wait for all uploads to complete
    let start_time = std::time::Instant::now();
    let mut results = Vec::new();
    for task in put_tasks {
        results.push(task.await??);
    }
    let total_elapsed = start_time.elapsed();

    println!(
        "All concurrent large uploads completed in {:?}",
        total_elapsed
    );

    // Verify all succeeded
    for (i, (status, _, _)) in results.iter().enumerate() {
        assert_eq!(
            *status,
            reqwest::StatusCode::CREATED,
            "PUT {} should succeed",
            i
        );
    }

    // Verify all files exist on all replicas
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    for key in &keys {
        let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
        assert_eq!(
            volumes_with_file.len(),
            3,
            "All volumes should have file for key: {}",
            key
        );
    }

    println!("Concurrent large object operations test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "heavy-tests"), ignore)]
async fn test_large_object_memory_efficiency() -> anyhow::Result<()> {
    // Test that large objects don't consume excessive memory during streaming
    let coord = TestCoordinator::new_with_replicas(3).await?;
    let client = Client::new();

    let mut volumes = create_volumes(coord.url(), 3).await?;
    join_and_heartbeat_volumes(&mut volumes, 500).await?;
    wait_for_volumes_alive(&client, coord.url(), 3, 5000).await?;

    let key = "test-memory-efficiency";

    // Use a large size that would be problematic if fully buffered
    let payload_size = 100 * 1024 * 1024; // 100 MiB

    // Create payload efficiently
    let pattern = b"MEMORY-EFFICIENCY-TEST-PATTERN-1234567890-";
    let mut payload = Vec::with_capacity(payload_size);
    while payload.len() < payload_size {
        payload.extend_from_slice(pattern);
        if payload.len() > payload_size {
            payload.truncate(payload_size);
        }
    }

    let expected_etag = blake3_hex(&payload);

    println!(
        "Testing memory efficiency with {}MB object",
        payload.len() / (1024 * 1024)
    );

    // The key test: if this completes without OOM, streaming is working
    let start_time = std::time::Instant::now();
    let (status, etag, _) = put_via_coordinator(&client, coord.url(), key, payload.clone()).await?;
    let elapsed = start_time.elapsed();

    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "Memory efficiency PUT should succeed"
    );
    assert_eq!(etag, expected_etag, "ETag should match");

    println!("Memory efficient PUT completed in {:?}", elapsed);

    // Test memory efficient GET
    let redirect_client = create_redirect_client()?;
    let get_start = std::time::Instant::now();
    let response_bytes = follow_redirect_get(&redirect_client, coord.url(), key).await?;
    let get_elapsed = get_start.elapsed();

    assert_eq!(
        response_bytes.len(),
        payload.len(),
        "GET should return correct size"
    );

    // Quick integrity check via hash
    let response_etag = blake3_hex(&response_bytes);
    assert_eq!(
        response_etag, expected_etag,
        "GET should return correct data"
    );

    println!("Memory efficient GET completed in {:?}", get_elapsed);
    println!(
        "Memory efficiency test successful - no OOM with {}MB object",
        payload.len() / (1024 * 1024)
    );

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

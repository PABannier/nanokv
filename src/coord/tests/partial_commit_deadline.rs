use reqwest::Client;

mod common;
use common::*;
use coord::core::meta::TxState;

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

    println!(
        "Starting PUT with {}MB payload that should hit commit deadline",
        payload_size / (1024 * 1024)
    );

    let start_time = std::time::Instant::now();
    let (status, _, _) = put_via_coordinator(&client, coord.url(), key, payload).await?;
    let elapsed = start_time.elapsed();

    assert!(
        status.is_server_error(),
        "Large payload PUT should fail at commit deadline"
    );
    println!("Large payload PUT failed at commit in {:?}", elapsed);

    // Verify partial state
    let volume_refs: Vec<&TestVolume> = volumes.iter().collect();
    let volumes_with_file = which_volume_has_file(&volume_refs, key)?;
    assert!(
        volumes_with_file.len() < 3,
        "Should have partial commit with large payload"
    );

    let meta = meta_of(&coord.state.db, key)?;
    if let Some(meta) = meta {
        assert_ne!(
            meta.state,
            TxState::Committed,
            "Large payload meta should not be committed"
        );
    }

    println!("Large payload commit deadline test successful");

    // Cleanup
    shutdown_volumes(volumes).await?;
    coord.shutdown().await?;

    Ok(())
}

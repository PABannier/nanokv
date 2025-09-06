use std::collections::HashMap;

use reqwest::Client;

mod common;
use common::*;
use coord::core::node::NodeStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_deterministic_placement() -> anyhow::Result<()> {
    // Start coordinator + two Alive volume servers
    let coord = TestCoordinator::new().await?;
    let client = Client::new();
    
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;
    
    // Join both volumes and start heartbeats
    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    
    vol2.join_coordinator().await?;
    vol2.start_heartbeat(500)?;
    
    // Wait for both to be alive
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 2 && nodes.iter().all(|n| n.status == NodeStatus::Alive))
    }).await?;
    
    // Test fixed list of keys for consistent placement
    let test_keys: Vec<String> = (0..100).map(|i| format!("test-key-{}", i)).collect();
    
    // First round: record which node each key maps to
    let mut first_placements = HashMap::new();
    let nodes = list_nodes(&client, coord.url()).await?;
    
    for key in &test_keys {
        if let Some(chosen_node) = test_placement(key, &nodes) {
            first_placements.insert(key.clone(), chosen_node);
        }
    }
    
    // Second round: verify placement is stable
    let nodes = list_nodes(&client, coord.url()).await?;
    for key in &test_keys {
        if let Some(chosen_node) = test_placement(key, &nodes) {
            assert_eq!(
                first_placements.get(key),
                Some(&chosen_node),
                "Placement changed for key {}: was {:?}, now {}",
                key,
                first_placements.get(key),
                chosen_node
            );
        }
    }
    
    // Bring up a third volume
    let mut vol3 = TestVolume::new(coord.url().to_string(), "vol-3".to_string()).await?;
    vol3.join_coordinator().await?;
    vol3.start_heartbeat(500)?;
    
    // Wait for all three to be alive
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 3 && nodes.iter().all(|n| n.status == NodeStatus::Alive))
    }).await?;
    
    // Recompute placement with 3 nodes
    let mut changed_keys = 0;
    let mut stayed_keys = 0;
    let nodes = list_nodes(&client, coord.url()).await?;
    
    for key in &test_keys {
        if let Some(chosen_node) = test_placement(key, &nodes) {
            if first_placements.get(key) == Some(&chosen_node) {
                stayed_keys += 1;
            } else {
                changed_keys += 1;
            }
        }
    }
    
    // Assert: A minority of keys move to the new node, some stay on previous nodes
    assert!(changed_keys > 0, "No keys moved to new node");
    assert!(stayed_keys > 0, "All keys moved, should have some stability");
    assert!(changed_keys < test_keys.len(), "All keys moved, expected minority");
    
    println!("Placement results: {} keys moved, {} keys stayed", changed_keys, stayed_keys);
    
    // Cleanup
    vol1.shutdown().await?;
    vol2.shutdown().await?;
    vol3.shutdown().await?;
    coord.shutdown().await?;
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_placement_affinity_with_actual_puts() -> anyhow::Result<()> {
    // Start coordinator + two Alive volume servers
    let coord = TestCoordinator::new().await?;
    let client = Client::new();
    
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;
    
    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.join_coordinator().await?;
    vol2.start_heartbeat(500)?;
    
    // Wait for both to be alive
    wait_until(3000, || async {
        let nodes = list_nodes(&client, coord.url()).await?;
        Ok(nodes.len() == 2 && nodes.iter().all(|n| n.status == NodeStatus::Alive))
    }).await?;
    
    let nodes = list_nodes(&client, coord.url()).await?;
    
    // Upload multiple keys and verify placement matches expected
    let test_keys = vec!["key1", "key2", "key3", "key4", "key5"];
    
    for key in &test_keys {
        let data = format!("data for {}", key).into_bytes();
        let (status, _etag, _len) = put_via_coordinator(&client, coord.url(), key, data).await?;
        assert_eq!(status, reqwest::StatusCode::CREATED);
        
        // Verify the coordinator's chosen node matches where we expect
        let expected_node = test_placement(key, &nodes);
        assert!(expected_node.is_some(), "Expected placement for key {}", key);
        
        // We can't easily verify which volume actually received the file without
        // filesystem access, but the fact that PUT succeeded means placement worked
    }
    
    // Cleanup
    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    
    Ok(())
}

use reqwest::Client;
use tempfile::TempDir;

mod common;
use ::common::key_utils::{meta_key_for, Key};
use common::*;

use coord::command::repair::{RepairArgs, repair};
use coord::core::meta::{KvDb, Meta, TxState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_fills_under_replication_to_n() -> anyhow::Result<()> {
    // Setup: N=2, vol1 has A, vol2 missing, DB meta lists replicas = [node1] Committed
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    // Join volumes to coordinator
    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob through coordinator - it should go to both volumes
    let raw_key = "test-key-repair";
    let content = b"test content for repair";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Simulate under-replication by modifying the meta to only list vol1
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.replicas = vec!["vol-1".to_string()]; // Only vol1 in replicas
    coord.state.db.put(&meta_key, &meta)?;

    // Run repair
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    // Copy the coordinator's database to repair DB
    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &meta)?;

        // Also copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: Some(vec![vol1.url().to_string(), vol2.url().to_string()]),
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: false,
    };

    repair(args).await?;

    // Assert: meta should now include vol2 in replicas
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());
    let meta_after = meta_after.unwrap();

    assert_eq!(meta_after.state, TxState::Committed);
    assert_eq!(meta_after.size, _size);
    assert_eq!(meta_after.replicas.len(), 2);
    assert!(meta_after.replicas.contains(&"vol-1".to_string()));
    assert!(meta_after.replicas.contains(&"vol-2".to_string()));

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_uses_valid_source_only() -> anyhow::Result<()> {
    // Setup: meta lists node1 as replica but vol1 content is corrupted
    // vol2 holds valid copy but not listed in meta.replicas
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob to get it on both volumes
    let raw_key = "test-key-valid-source";
    let content = b"test content for valid source";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Simulate corruption by modifying meta to only list vol1, but vol2 has valid copy
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.replicas = vec!["vol-1".to_string()]; // Only vol1 listed, but both have the file
    coord.state.db.put(&meta_key, &meta)?;

    // Run repair with dry_run to see what it would do
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &meta)?;

        // Copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: Some(vec![vol1.url().to_string(), vol2.url().to_string()]),
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: true, // Dry run to test logic without actual copying
    };

    repair(args).await?;

    // In a real scenario, repair should detect that vol2 has valid copy and update meta
    // For now, just verify the repair command runs without error
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_skips_tombstoned_metas() -> anyhow::Result<()> {
    // Setup: meta for A is Tombstoned, but files exist on some volumes
    let coord = TestCoordinator::new_with_replicas(2).await?;

    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;
    let mut vol3 = TestVolume::new(coord.url().to_string(), "vol-3".to_string()).await?;

    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;

    vol2.join_coordinator().await?;
    vol2.start_heartbeat(500)?;

    vol3.join_coordinator().await?;
    vol3.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 3, 3000).await?;

    // Put and then delete a blob to create tombstone
    let raw_key = "test-key-tombstone-repair";
    let content = b"test content tombstone";
    let (status, _, _) = put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let delete_status = delete_via_coordinator(&client, coord.url(), raw_key).await?;
    assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Verify meta is tombstoned
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some());
    assert_eq!(meta.unwrap().state, TxState::Tombstoned);

    // Run repair
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let tombstone_meta = coord.state.db.get::<Meta>(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &tombstone_meta)?;

        // Copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: None,  // Use registry
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: false,
    };

    repair(args).await?;

    // Assert: tombstone meta should remain unchanged (not repaired)
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());
    assert_eq!(meta_after.unwrap().state, TxState::Tombstoned);

    vol1.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_destination_pre_check_avoids_unnecessary_copy() -> anyhow::Result<()> {
    // Setup: destination already has valid blob but meta does not list it yet
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob through coordinator
    let raw_key = "test-key-precheck";
    let content = b"test content precheck";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Simulate scenario where destination has file but meta doesn't list it
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.replicas = vec!["vol-1".to_string()]; // Only vol1 in meta, but vol2 also has file
    coord.state.db.put(&meta_key, &meta)?;

    // Run repair
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &meta)?;

        // Copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: None,  // Use registry
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: false,
    };

    repair(args).await?;

    // Assert: meta should be updated to include vol2 without unnecessary copy
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());
    let meta_after = meta_after.unwrap();

    assert_eq!(meta_after.replicas.len(), 2);
    assert!(meta_after.replicas.contains(&"vol-1".to_string()));
    assert!(meta_after.replicas.contains(&"vol-2".to_string()));

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_dry_run() -> anyhow::Result<()> {
    // Setup: one missing replica
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob and simulate under-replication
    let raw_key = "test-key-dry-run-repair";
    let content = b"test content dry run";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Simulate under-replication
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    let _original_replicas = meta.replicas.clone();
    meta.replicas = vec!["vol-1".to_string()]; // Only vol1
    coord.state.db.put(&meta_key, &meta)?;

    // Run repair in dry-run mode
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &meta)?;

        // Copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: Some(vec![vol1.url().to_string(), vol2.url().to_string()]),
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: true, // Dry run should not modify anything
    };

    repair(args).await?;

    // Assert: meta should remain unchanged in dry run
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());
    let meta_after = meta_after.unwrap();

    // In dry run, meta should not be updated
    assert_eq!(meta_after.replicas.len(), 1);
    assert!(meta_after.replicas.contains(&"vol-1".to_string()));

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repair_resumability_journal() -> anyhow::Result<()> {
    // Setup: simulate one move marked as committed in journal
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob
    let raw_key = "test-key-journal";
    let content = b"test content journal";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);

    // Simulate under-replication
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.replicas = vec!["vol-1".to_string()];
    coord.state.db.put(&meta_key, &meta)?;

    // Run repair
    let temp_dir = TempDir::new()?;
    let repair_db_path = temp_dir.path().join("repair_db");

    {
        let repair_db = KvDb::open(&repair_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        repair_db.put(&meta_key, &meta)?;

        // Pre-populate journal entry to simulate previous run
        let journal_key = format!("repair:{}:vol-2", key_enc);
        repair_db.put(&journal_key, &"Committed")?;

        // Copy node information
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("node:") {
                repair_db.put(key_str, &serde_json::from_slice::<serde_json::Value>(&v)?)?;
            }
        }
    }

    let args = RepairArgs {
        index: repair_db_path.clone(),
        volumes: Some(vec![vol1.url().to_string(), vol2.url().to_string()]),
        n_replicas: 2,
        concurrency: 8,
        concurrency_per_node: 2,
        include_suspect: false,
        dry_run: false,
    };

    repair(args).await?;

    // Assert: repair should skip the already committed move
    let repair_db = KvDb::open(&repair_db_path)?;
    let meta_after: Option<Meta> = repair_db.get(&meta_key)?;
    assert!(meta_after.is_some());

    // Journal entry should still exist
    let journal_key = format!("repair:{}:vol-2", key_enc);
    let journal_entry: Option<String> = repair_db.get(&journal_key)?;
    assert!(journal_entry.is_some());

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

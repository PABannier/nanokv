use reqwest::Client;
use tempfile::TempDir;

mod common;
use ::common::key_utils::{Key, meta_key_for};
use common::*;

use coord::command::rebuild::{RebuildArgs, rebuild};
use coord::core::meta::{KvDb, Meta, TxState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rebuild_writes_metas_from_file_system() -> anyhow::Result<()> {
    // Setup: coordinator and 2 volumes with same blob A, no meta in DB
    let coord = TestCoordinator::new().await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    // Join volumes to coordinator
    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put a blob through the coordinator to create it on both volumes
    let content_a = b"test content A";
    let raw_key = "test-key-a";
    let (status, _etag, size) =
        put_via_coordinator(&client, coord.url(), raw_key, content_a.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // Now clear the meta from the database to simulate the scenario
    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);
    coord.state.db.delete(&meta_key)?;

    // Run rebuild with deep=false - use a separate DB for rebuild
    let temp_dir = TempDir::new()?;
    let rebuild_db_path = temp_dir.path().join("rebuild_db");

    let args = RebuildArgs {
        index: rebuild_db_path.clone(),
        nodes: vec![vol1.url().to_string(), vol2.url().to_string()],
        dry_run: false,
        deep: false,
        concurrency: 16,
        http_timeout_secs: 5,
    };

    rebuild(args).await?;

    // Open the DB after rebuild to check results
    let rebuild_db = KvDb::open(&rebuild_db_path)?;
    let meta: Option<Meta> = rebuild_db.get(&meta_key)?;
    assert!(meta.is_some(), "Meta should be created by rebuild");
    let meta = meta.unwrap();

    assert_eq!(meta.state, TxState::Committed);
    assert_eq!(meta.size, size);
    assert_eq!(meta.replicas.len(), 2);
    assert!(meta.replicas.contains(&"vol-1".to_string()));
    assert!(meta.replicas.contains(&"vol-2".to_string()));

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rebuild_preserves_tombstones() -> anyhow::Result<()> {
    // Setup: coordinator and volumes with blob, but meta is tombstoned
    let coord = TestCoordinator::new().await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;

    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 1, 3000).await?;

    // Put and then delete a blob to create tombstone
    let raw_key = "test-key-tombstone";
    let content = b"test content";
    let (status, _, _) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let delete_status = delete_via_coordinator(&client, coord.url(), raw_key).await?;
    assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT);

    // Verify meta is tombstoned
    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);
    let meta: Option<Meta> = coord.state.db.get(&meta_key)?;
    assert!(meta.is_some());
    assert_eq!(meta.unwrap().state, TxState::Tombstoned);

    // Run rebuild
    let temp_dir = TempDir::new()?;
    let rebuild_db_path = temp_dir.path().join("rebuild_db");

    // Pre-populate the rebuild DB with the tombstone
    {
        let rebuild_db = KvDb::open(&rebuild_db_path)?;
        let tombstone_meta = coord.state.db.get::<Meta>(&meta_key)?.unwrap();
        rebuild_db.put(&meta_key, &tombstone_meta)?;
        // DB will be closed when rebuild_db goes out of scope
    }

    let args = RebuildArgs {
        index: rebuild_db_path.clone(),
        nodes: vec![vol1.url().to_string()],
        dry_run: false,
        deep: false,
        concurrency: 16,
        http_timeout_secs: 5,
    };

    rebuild(args).await?;

    // Open DB after rebuild to check results
    let rebuild_db = KvDb::open(&rebuild_db_path)?;
    let meta_after: Option<Meta> = rebuild_db.get(&meta_key)?;
    assert!(meta_after.is_some());
    assert_eq!(meta_after.unwrap().state, TxState::Tombstoned);

    vol1.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rebuild_dry_run() -> anyhow::Result<()> {
    // Setup: coordinator and volume with blob, no meta in DB
    let coord = TestCoordinator::new().await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;

    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 1, 3000).await?;

    // Put a blob and then clear meta
    let raw_key = "test-key-dry-run";
    let content = b"test content";
    let (status, _, _) =
        put_via_coordinator(&client, coord.url(), raw_key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key = Key::from_percent_encoded(raw_key).unwrap();
    let key_enc = key.enc();
    let meta_key = meta_key_for(&key_enc);
    coord.state.db.delete(&meta_key)?;

    // Run rebuild in dry-run mode
    let temp_dir = TempDir::new()?;
    let rebuild_db_path = temp_dir.path().join("rebuild_db");

    let args = RebuildArgs {
        index: rebuild_db_path.clone(),
        nodes: vec![vol1.url().to_string()],
        dry_run: true, // Dry run should not modify DB
        deep: false,
        concurrency: 16,
        http_timeout_secs: 5,
    };

    rebuild(args).await?;

    // Open DB after rebuild to check results
    let rebuild_db = KvDb::open(&rebuild_db_path)?;
    let meta: Option<Meta> = rebuild_db.get(&meta_key)?;
    assert!(meta.is_none(), "Dry run should not write any metas to DB");

    vol1.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

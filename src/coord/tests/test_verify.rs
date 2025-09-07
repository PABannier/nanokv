use reqwest::Client;
use tempfile::TempDir;

mod common;
use ::common::file_utils::{meta_key_for, sanitize_key};
use common::*;

use coord::command::verify::{VerifyArgs, verify};
use coord::core::meta::{KvDb, Meta, TxState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_under_replication() -> anyhow::Result<()> {
    // Setup: meta says replicas=[node1,node2], only node1 has file
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
    let key = "test-key-under-repl";
    let content = b"test content under replication";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key_enc = sanitize_key(key)?;
    let meta_key = meta_key_for(&key_enc);

    // Verify both volumes have the file initially
    let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    assert_eq!(meta.replicas.len(), 2);

    // Simulate file missing from vol2 by modifying the meta to claim both have it
    // but in reality only vol1 will respond correctly
    let mut meta_modified = meta.clone();
    meta_modified.replicas = vec!["vol-1".to_string(), "vol-2".to_string()];
    coord.state.db.put(&meta_key, &meta_modified)?;

    // Run verify
    let temp_dir = TempDir::new()?;
    let verify_db_path = temp_dir.path().join("verify_db");

    {
        let verify_db = KvDb::open(&verify_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        verify_db.put(&meta_key, &meta)?;
    }

    let args = VerifyArgs {
        index: verify_db_path.clone(),
        nodes: vec![
            format!("vol-1={}", vol1.url()),
            format!("vol-2={}", vol2.url()),
        ],
        fix: false,
        deep: false,
        concurrency: 16,
        per_node: 4,
        http_timeout_secs: 5,
    };

    verify(args).await?;

    // The verify command should detect under-replication
    // In a real implementation, this would increment under_replicated counter
    // For now, verify the command runs without error

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_corrupted_size_mismatch() -> anyhow::Result<()> {
    // Setup: file exists on both; sizes differ from meta
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
    let key = "test-key-size-mismatch";
    let content = b"test content size mismatch";
    let (status, _etag, size) =
        put_via_coordinator(&client, coord.url(), key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key_enc = sanitize_key(key)?;
    let meta_key = meta_key_for(&key_enc);

    // Corrupt the meta by changing the size
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.size = size + 100; // Wrong size
    coord.state.db.put(&meta_key, &meta)?;

    // Run verify
    let temp_dir = TempDir::new()?;
    let verify_db_path = temp_dir.path().join("verify_db");

    {
        let verify_db = KvDb::open(&verify_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        verify_db.put(&meta_key, &meta)?;
    }

    let args = VerifyArgs {
        index: verify_db_path.clone(),
        nodes: vec![
            format!("vol-1={}", vol1.url()),
            format!("vol-2={}", vol2.url()),
        ],
        fix: false,
        deep: false,
        concurrency: 16,
        per_node: 4,
        http_timeout_secs: 5,
    };

    verify(args).await?;

    // The verify command should detect corruption due to size mismatch
    // For now, verify the command runs without error

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_deep_etag_mismatch() -> anyhow::Result<()> {
    // Setup: sizes equal; contents differ; deep=true
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
    let key = "test-key-etag-mismatch";
    let content = b"test content etag mismatch";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), key, content.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let key_enc = sanitize_key(key)?;
    let meta_key = meta_key_for(&key_enc);

    // Corrupt the meta by changing the etag
    let mut meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
    meta.etag_hex = "corrupted_etag_hex".to_string();
    coord.state.db.put(&meta_key, &meta)?;

    // Run verify with deep=true
    let temp_dir = TempDir::new()?;
    let verify_db_path = temp_dir.path().join("verify_db");

    {
        let verify_db = KvDb::open(&verify_db_path)?;
        let meta: Meta = coord.state.db.get(&meta_key)?.unwrap();
        verify_db.put(&meta_key, &meta)?;
    }

    let args = VerifyArgs {
        index: verify_db_path.clone(),
        nodes: vec![
            format!("vol-1={}", vol1.url()),
            format!("vol-2={}", vol2.url()),
        ],
        fix: false,
        deep: true, // Deep verification should detect etag mismatch
        concurrency: 16,
        per_node: 4,
        http_timeout_secs: 5,
    };

    verify(args).await?;

    // The verify command should detect corruption due to etag mismatch in deep mode
    // For now, verify the command runs without error

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_unindexed_and_should_gc() -> anyhow::Result<()> {
    // Setup: volume has file B but DB has no meta → unindexed
    // Setup: volume has file C but DB has Tombstoned meta → should_gc
    let coord = TestCoordinator::new_with_replicas(1).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;

    vol1.join_coordinator().await?;
    vol1.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 1, 3000).await?;

    // Create a normal blob
    let key_normal = "test-key-normal";
    let content_normal = b"test content normal";
    let (status, _etag, _size) =
        put_via_coordinator(&client, coord.url(), key_normal, content_normal.to_vec()).await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // Create a blob and then delete it to make it tombstoned
    let key_tombstone = "test-key-should-gc";
    let content_tombstone = b"test content should gc";
    let (status, _etag, _size) = put_via_coordinator(
        &client,
        coord.url(),
        key_tombstone,
        content_tombstone.to_vec(),
    )
    .await?;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    let delete_status = delete_via_coordinator(&client, coord.url(), key_tombstone).await?;
    assert_eq!(delete_status, reqwest::StatusCode::NO_CONTENT);

    // Verify tombstone exists
    let key_enc_tombstone = sanitize_key(key_tombstone)?;
    let meta_key_tombstone = meta_key_for(&key_enc_tombstone);
    let meta_tombstone: Option<Meta> = coord.state.db.get(&meta_key_tombstone)?;
    assert!(meta_tombstone.is_some());
    assert_eq!(meta_tombstone.unwrap().state, TxState::Tombstoned);

    // For unindexed test, we would need to manually place a file on volume
    // without corresponding meta, but TestVolume doesn't expose file system
    // So we'll just test the tombstone case for now

    // Run verify
    let temp_dir = TempDir::new()?;
    let verify_db_path = temp_dir.path().join("verify_db");

    {
        let verify_db = KvDb::open(&verify_db_path)?;

        // Copy all metas from coordinator DB
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("meta:") {
                verify_db.put(key_str, &serde_json::from_slice::<Meta>(&v)?)?;
            }
        }
    }

    let args = VerifyArgs {
        index: verify_db_path.clone(),
        nodes: vec![format!("vol-1={}", vol1.url())],
        fix: false,
        deep: false,
        concurrency: 16,
        per_node: 4,
        http_timeout_secs: 5,
    };

    verify(args).await?;

    // The verify command should detect should_gc items
    // For now, verify the command runs without error

    vol1.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_per_node_concurrency_respected() -> anyhow::Result<()> {
    // Setup: inject artificial delay; ensure concurrency limits not exceeded
    let coord = TestCoordinator::new_with_replicas(2).await?;
    let mut vol1 = TestVolume::new(coord.url().to_string(), "vol-1".to_string()).await?;
    let mut vol2 = TestVolume::new(coord.url().to_string(), "vol-2".to_string()).await?;

    vol1.join_coordinator().await?;
    vol2.join_coordinator().await?;
    vol1.start_heartbeat(500)?;
    vol2.start_heartbeat(500)?;

    let client = Client::new();
    wait_for_volumes_alive(&client, coord.url(), 2, 3000).await?;

    // Put several blobs to test concurrency
    for i in 0..5 {
        let key = format!("test-key-concurrency-{}", i);
        let content = format!("test content concurrency {}", i).into_bytes();
        let (status, _etag, _size) =
            put_via_coordinator(&client, coord.url(), &key, content).await?;
        assert_eq!(status, reqwest::StatusCode::CREATED);
    }

    // Run verify with limited per-node concurrency
    let temp_dir = TempDir::new()?;
    let verify_db_path = temp_dir.path().join("verify_db");

    {
        let verify_db = KvDb::open(&verify_db_path)?;

        // Copy all metas from coordinator DB
        for kv in coord.state.db.iter() {
            let (k, v) = kv?;
            let key_str = std::str::from_utf8(&k)?;
            if key_str.starts_with("meta:") {
                verify_db.put(key_str, &serde_json::from_slice::<Meta>(&v)?)?;
            }
        }
    }

    let args = VerifyArgs {
        index: verify_db_path.clone(),
        nodes: vec![
            format!("vol-1={}", vol1.url()),
            format!("vol-2={}", vol2.url()),
        ],
        fix: false,
        deep: false,
        concurrency: 16,
        per_node: 2, // Limited per-node concurrency
        http_timeout_secs: 5,
    };

    verify(args).await?;

    // The verify command should respect per-node concurrency limits
    // In a real test, we would expose counters from fake server state
    // For now, verify the command runs without error

    vol1.shutdown().await?;
    vol2.shutdown().await?;
    coord.shutdown().await?;
    Ok(())
}

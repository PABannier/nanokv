use nanokv::file_utils::{sanitize_key, blob_path, tmp_path, meta_key_for};
use nanokv::meta::{Meta, TxState};
use nanokv::kvdb::KvDb;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_key_sanitize_roundtrip() {
    // Test basic alphanumeric keys
    let key = "simple_key123";
    let sanitized = sanitize_key(key).unwrap();
    assert_eq!(sanitized, "simple%5Fkey123");
    
    // Test with special characters
    let key = "path/to/file.txt";
    let sanitized = sanitize_key(key).unwrap();
    assert!(sanitized.contains("%2F")); // Forward slash encoded
    assert!(sanitized.contains("%2E")); // Dot encoded
    
    // Test empty key fails
    assert!(sanitize_key("").is_err());
    
    // Test too long key fails
    let long_key = "a".repeat(3000);
    assert!(sanitize_key(&long_key).is_err());
    
    // Test path traversal protection
    assert!(sanitize_key("../secret").is_err());
    assert!(sanitize_key("path/../secret").is_err());
}

#[test]
fn test_shard_dirs_stability() {
    // Test that the same key always produces the same shard dirs
    let key = "test_key";
    let path1 = blob_path(Path::new("/data"), key);
    let path2 = blob_path(Path::new("/data"), key);
    assert_eq!(path1, path2);
    
    // Test different keys produce different paths (with high probability)
    let key2 = "different_key";
    let path3 = blob_path(Path::new("/data"), key2);
    assert_ne!(path1, path3);
    
    // Test the structure is as expected (data/blobs/xx/yy/key)
    let expected_components = vec!["data", "blobs"];
    let components: Vec<_> = path1.components()
        .map(|c| c.as_os_str().to_str().unwrap())
        .skip_while(|&c| c == "/") // Skip root component on Unix
        .take(2)
        .collect();
    assert_eq!(components, expected_components);
    
    // Verify the shard directories are 2-char hex
    let parent = path1.parent().unwrap();
    let shard2 = parent.file_name().unwrap().to_str().unwrap();
    let shard1 = parent.parent().unwrap().file_name().unwrap().to_str().unwrap();
    
    assert_eq!(shard1.len(), 2);
    assert_eq!(shard2.len(), 2);
    assert!(shard1.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(shard2.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_meta_key_generation() {
    let user_key = "test%2Fkey";
    let meta_key = meta_key_for(user_key);
    assert_eq!(meta_key, "meta:test%2Fkey");
    
    // Test consistency
    let meta_key2 = meta_key_for(user_key);
    assert_eq!(meta_key, meta_key2);
}

#[test]
fn test_rocksdb_meta_transitions() {
    let temp_dir = TempDir::new().unwrap();
    let db = KvDb::open(temp_dir.path()).unwrap();
    
    let key = "test_key";
    
    // Test initial state - key doesn't exist
    let result: Option<Meta> = db.get(key).unwrap();
    assert!(result.is_none());
    
    // Test Pending state
    let pending_meta = Meta::pending("upload123".to_string());
    assert_eq!(pending_meta.state, TxState::Pending);
    assert_eq!(pending_meta.upload_id, Some("upload123".to_string()));
    
    db.put(key, &pending_meta).unwrap();
    let retrieved: Meta = db.get(key).unwrap().unwrap();
    assert_eq!(retrieved.state, TxState::Pending);
    assert_eq!(retrieved.upload_id, Some("upload123".to_string()));
    
    // Test transition to Committed
    let committed_meta = Meta::committed(1024, "abc123".to_string());
    assert_eq!(committed_meta.state, TxState::Committed);
    assert_eq!(committed_meta.size, 1024);
    assert_eq!(committed_meta.etag_hex, "abc123");
    assert!(committed_meta.upload_id.is_none());
    
    db.put(key, &committed_meta).unwrap();
    let retrieved: Meta = db.get(key).unwrap().unwrap();
    assert_eq!(retrieved.state, TxState::Committed);
    assert_eq!(retrieved.size, 1024);
    assert_eq!(retrieved.etag_hex, "abc123");
    
    // Test transition to Tombstoned
    let tombstoned_meta = Meta::tombstoned();
    assert_eq!(tombstoned_meta.state, TxState::Tombstoned);
    assert_eq!(tombstoned_meta.size, 0);
    assert!(tombstoned_meta.etag_hex.is_empty());
    assert!(tombstoned_meta.upload_id.is_none());
    
    db.put(key, &tombstoned_meta).unwrap();
    let retrieved: Meta = db.get(key).unwrap().unwrap();
    assert_eq!(retrieved.state, TxState::Tombstoned);
    
    // Test idempotent delete - calling delete on tombstoned should work
    db.put(key, &tombstoned_meta).unwrap(); // Put tombstoned again
    let retrieved: Meta = db.get(key).unwrap().unwrap();
    assert_eq!(retrieved.state, TxState::Tombstoned);
    
    // Test actual deletion from DB
    db.delete(key).unwrap();
    let result: Option<Meta> = db.get(key).unwrap();
    assert!(result.is_none());
    
    // Test idempotent delete on non-existent key
    db.delete(key).unwrap(); // Should not error
    let result: Option<Meta> = db.get(key).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_tmp_path_generation() {
    let root = Path::new("/data");
    let upload_id = "12345-abcde";
    let path = tmp_path(root, upload_id);
    
    assert_eq!(path, Path::new("/data/tmp/12345-abcde"));
}

#[test]
fn test_meta_timestamps() {
    let meta1 = Meta::pending("upload1".to_string());
    let meta2 = Meta::committed(100, "hash1".to_string());
    let meta3 = Meta::tombstoned();
    
    // All should have recent timestamps
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    assert!((meta1.created_ms - now).abs() < 1000); // Within 1 second
    assert!((meta2.created_ms - now).abs() < 1000);
    assert!((meta3.created_ms - now).abs() < 1000);
}

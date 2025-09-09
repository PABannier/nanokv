use common::file_utils::{blob_path, tmp_path};
use common::key_utils::{Key, meta_key_for};
use std::path::Path;

#[test]
fn test_key_sanitize_roundtrip() {
    // Test basic alphanumeric keys
    let raw_key = "simple_key123";
    let key = Key::from_percent_encoded(raw_key).unwrap();
    let sanitized = key.enc();
    assert_eq!(sanitized, "simple%5fkey123");

    // Test with special characters - should be forbidden
    let raw_key = "path/to/file.txt";
    assert!(matches!(
        Key::from_percent_encoded(raw_key),
        Err(common::error::KeyError::Forbidden)
    ));

    // Test empty key fails
    assert!(Key::from_percent_encoded("").is_err());

    // Test too long key fails
    let long_key = "a".repeat(5000);
    assert!(Key::from_percent_encoded(&long_key).is_err());
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
    let components: Vec<_> = path1
        .components()
        .map(|c| c.as_os_str().to_str().unwrap())
        .skip_while(|&c| c == "/") // Skip root component on Unix
        .take(2)
        .collect();
    assert_eq!(components, expected_components);

    // Verify the shard directories are 2-char hex
    let parent = path1.parent().unwrap();
    let shard2 = parent.file_name().unwrap().to_str().unwrap();
    let shard1 = parent
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

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
fn test_tmp_path_generation() {
    let root = Path::new("/data");
    let upload_id = "12345-abcde";
    let path = tmp_path(root, upload_id);

    assert_eq!(path, Path::new("/data/tmp/12345-abcde"));
}

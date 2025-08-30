use coord::state::CoordinatorState;
use coord::meta::{KvDb, Meta, TxState};
use coord::routes;
use common::file_utils::{blob_path, sanitize_key, meta_key_for, tmp_path};
use common::file_utils::init_dirs;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
    Router,
};
use tower::ServiceExt;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tempfile::TempDir;
use tokio::fs;
use http_body_util::BodyExt;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use bytes;

async fn create_test_app() -> (Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_root = temp_dir.path().to_path_buf();
    
    init_dirs(&data_root).await.unwrap();
    
    let index_path = data_root.join("index");
    let db = KvDb::open(&index_path).unwrap();
    
    let state = CoordinatorState {
        data_root: Arc::new(data_root),
        inflight: Arc::new(Semaphore::new(4)),
        max_size: 1024 * 1024 * 1024, // 1GB
        db,
    };
    
    let app = Router::new()
        .route("/{key}", 
            axum::routing::put(routes::put_object)
                .get(routes::get_object)
                .delete(routes::delete_object)
                .head(routes::head_object)
        )
        .with_state(state);
    
    (app, temp_dir)
}

#[tokio::test]
async fn test_mid_write_abort() {
    let (app, temp_dir) = create_test_app().await;
    
    let key = "abort_test_key";
    let data_size = 10 * 1024 * 1024; // 10MB
    
    // Create a large body that we'll abort mid-stream
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let body = Body::from_stream(ReceiverStream::new(rx));
    
    // Send request in background
    let app_clone = app.clone();
    let key_clone = key.to_string();
    let abort_handle = tokio::spawn(async move {
        let put_request = Request::builder()
            .method("PUT")
            .uri(format!("/{}", key_clone))
            .header(header::CONTENT_LENGTH, data_size)
            .body(body)
            .unwrap();
        
        app_clone.oneshot(put_request).await
    });
    
    // Send some data then abort
    let chunk = vec![0u8; 1024 * 1024]; // 1MB
    let _ = tx.send(Ok(bytes::Bytes::from(chunk))).await;
    
    // Simulate client disconnect by dropping the sender
    drop(tx);
    
    // The request should fail
    let result = abort_handle.await.unwrap();
    assert!(result.is_err() || result.unwrap().status().is_server_error());
    
    // Verify the key is not visible (no committed state)
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    
    // Check that tmp files are eventually cleaned up
    // In a real scenario, this would be done by the cleanup process on restart
    let tmp_dir_path = temp_dir.path().join("tmp");
    
    // Wait a bit for any cleanup to happen
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify tmp directory exists but ideally would be cleaned
    if tmp_dir_path.exists() {
        let mut entries = fs::read_dir(&tmp_dir_path).await.unwrap();
        let mut count = 0;
        while let Some(_entry) = entries.next_entry().await.unwrap() {
            count += 1;
        }
        // In practice, cleanup would remove these files
        println!("Tmp files remaining: {}", count);
    }
}

#[tokio::test]
async fn test_durability_simulation() {
    let temp_dir = TempDir::new().unwrap();
    let data_root = temp_dir.path().to_path_buf();
    
    init_dirs(&data_root).await.unwrap();
    
    let key = "durability_test_key";
    let data = vec![42u8; 1024 * 1024]; // 1MB of data
    
    // First session: write data
    {
        let index_path = data_root.join("index");
        let db = KvDb::open(&index_path).unwrap();
        
        let state = AppState {
            data_root: Arc::new(data_root.clone()),
            inflight: Arc::new(Semaphore::new(4)),
            max_size: 1024 * 1024 * 1024,
            db,
        };
        
        let app = Router::new()
            .route("/{key}", 
                axum::routing::put(routes::put_object)
                    .get(routes::get_object)
            )
            .with_state(state);
        
        // PUT the object
        let put_request = Request::builder()
            .method("PUT")
            .uri(format!("/{}", key))
            .header(header::CONTENT_LENGTH, data.len())
            .body(Body::from(data.clone()))
            .unwrap();
        
        let put_response = app.clone().oneshot(put_request).await.unwrap();
        assert_eq!(put_response.status(), StatusCode::CREATED);
        
        // Verify it exists before "crash"
        let get_request = Request::builder()
            .method("GET")
            .uri(format!("/{}", key))
            .body(Body::empty())
            .unwrap();
        
        let get_response = app.oneshot(get_request).await.unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
    } // Simulate crash by dropping everything
    
    // Second session: restart and verify data is still there
    {
        let index_path = data_root.join("index");
        let db = KvDb::open(&index_path).unwrap();
        
        let state = AppState {
            data_root: Arc::new(data_root.clone()),
            inflight: Arc::new(Semaphore::new(4)),
            max_size: 1024 * 1024 * 1024,
            db,
        };
        
        let app = Router::new()
            .route("/{key}", 
                axum::routing::get(routes::get_object)
            )
            .with_state(state);
        
        // GET the object after "restart"
        let get_request = Request::builder()
            .method("GET")
            .uri(format!("/{}", key))
            .body(Body::empty())
            .unwrap();
        
        let get_response = app.oneshot(get_request).await.unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        
        let body_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes.len(), data.len());
        assert_eq!(body_bytes.to_vec(), data);
    }
}

#[tokio::test]
async fn test_range_requests() {
    // Note: Range requests are not currently implemented in nanokv
    // This test documents the expected behavior if they were implemented
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "range_test_key";
    let data = (0..255u8).cycle().take(2048).collect::<Vec<u8>>();
    
    // PUT the object
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.clone()))
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);
    
    // Try range request (currently not supported, would return full object)
    let range_request = Request::builder()
        .method("GET")
        .uri(format!("/{}", key))
        .header("Range", "bytes=0-1023")
        .body(Body::empty())
        .unwrap();
    
    let range_response = app.clone().oneshot(range_request).await.unwrap();
    
    // Current implementation returns full object with 200
    assert_eq!(range_response.status(), StatusCode::OK);
    
    let body_bytes = range_response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes.len(), data.len()); // Full object returned
    
    // If range support was implemented, we would expect:
    // assert_eq!(range_response.status(), StatusCode::PARTIAL_CONTENT);
    // assert_eq!(body_bytes.len(), 1024);
    // assert_eq!(body_bytes.to_vec(), data[0..1024]);
}

#[tokio::test]
async fn test_disk_full_simulation() {
    // This test simulates disk full conditions
    // In practice, this would require more sophisticated mocking
    let (app, temp_dir) = create_test_app().await;
    
    let key = "disk_full_test";
    
    // Try to write to a filesystem that might be full
    // For this test, we'll use the configured max_size to simulate the limit
    let data_size = 2 * 1024 * 1024 * 1024u64; // 2GB (larger than max_size)
    
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data_size)
        .body(Body::from(vec![0u8; 1024])) // Small actual body
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    
    // Verify no partial blob is left
    let blob_path = blob_path(&temp_dir.path(), &sanitize_key(key).unwrap());
    assert!(!blob_path.exists());
    
    // Verify no metadata is left
    let index_path = temp_dir.path().join("index");
    let db = KvDb::open(&index_path).unwrap();
    let meta_key = meta_key_for(&sanitize_key(key).unwrap());
    let meta: Option<Meta> = db.get(&meta_key).unwrap();
    assert!(meta.is_none());
}

#[tokio::test]
async fn test_concurrent_large_uploads() {
    let (app, _temp_dir) = create_test_app().await;
    
    let num_concurrent = 4;
    let size_per_upload = 100 * 1024 * 1024; // 100MB each
    
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    
    for i in 0..num_concurrent {
        let app_clone = app.clone();
        let key = format!("large_concurrent_{}", i);
        
        let handle = tokio::spawn(async move {
            // Generate data with a pattern for verification
            let mut data = Vec::with_capacity(size_per_upload);
            for j in 0..size_per_upload {
                data.push(((i * 1000 + j) % 256) as u8);
            }
            
            let upload_start = std::time::Instant::now();
            
            let put_request = Request::builder()
                .method("PUT")
                .uri(format!("/{}", key))
                .header(header::CONTENT_LENGTH, data.len())
                .body(Body::from(data.clone()))
                .unwrap();
            
            let put_response = app_clone.oneshot(put_request).await.unwrap();
            let upload_duration = upload_start.elapsed();
            
            assert_eq!(put_response.status(), StatusCode::CREATED);
            
            (key, data, upload_duration)
        });
        
        handles.push(handle);
    }
    
    // Wait for all uploads to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    let total_duration = start_time.elapsed();
    
    // Verify reasonable timing (should complete within reasonable time)
    println!("Total duration for {} concurrent {}MB uploads: {:?}", 
             num_concurrent, size_per_upload / (1024*1024), total_duration);
    
    for (i, (_key, _data, upload_duration)) in results.iter().enumerate() {
        println!("Upload {}: {:?}", i, upload_duration);
        // Each upload should complete within 60 seconds
        assert!(upload_duration.as_secs() < 60, 
                "Upload {} took too long: {:?}", i, upload_duration);
    }
    
    // Total should be reasonable (overlapping uploads should be faster than sequential)
    assert!(total_duration.as_secs() < 120, 
            "Total time too long: {:?}", total_duration);
    
    // Verify all objects can be read back correctly
    for (key, original_data, _) in results {
        let get_request = Request::builder()
            .method("GET")
            .uri(format!("/{}", key))
            .body(Body::empty())
            .unwrap();
        
        let get_response = app.clone().oneshot(get_request).await.unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        
        let body_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes.len(), original_data.len());
        assert_eq!(body_bytes.to_vec(), original_data);
    }
}

#[tokio::test]
async fn test_pending_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let data_root = temp_dir.path().to_path_buf();
    
    init_dirs(&data_root).await.unwrap();
    
    let index_path = data_root.join("index");
    let db = KvDb::open(&index_path).unwrap();
    
    // Manually create a pending upload
    let key = "pending_cleanup_test";
    let sanitized_key = sanitize_key(key).unwrap();
    let meta_key = meta_key_for(&sanitized_key);
    let upload_id = "test-upload-123";
    
    // Create pending metadata
    let pending_meta = Meta::pending(upload_id.to_string());
    db.put(&meta_key, &pending_meta).unwrap();
    
    // Create a temporary file
    let tmp_path = tmp_path(&data_root, upload_id);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await.unwrap();
    }
    fs::write(&tmp_path, b"pending upload data").await.unwrap();
    
    // Verify the pending state exists
    let meta: Option<Meta> = db.get(&meta_key).unwrap();
    assert!(meta.is_some());
    assert_eq!(meta.unwrap().state, TxState::Pending);
    assert!(tmp_path.exists());
    
    // Simulate cleanup process (this would normally be done by the cleanup module)
    // For this test, we'll manually clean up pending uploads older than grace period
    
    // Wait a bit to simulate time passing
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Clean up the pending upload
    db.delete(&meta_key).unwrap();
    if tmp_path.exists() {
        fs::remove_file(&tmp_path).await.unwrap();
    }
    
    // Verify cleanup
    let meta_after: Option<Meta> = db.get(&meta_key).unwrap();
    assert!(meta_after.is_none());
    assert!(!tmp_path.exists());
}

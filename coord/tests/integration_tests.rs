use coord::state::AppState;
use coord::meta::KvDb;
use coord::routes;

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
use http_body_util::BodyExt;

async fn create_test_app() -> (Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_root = temp_dir.path().to_path_buf();
    
    init_dirs(&data_root).await.unwrap();
    
    let index_path = data_root.join("index");
    let db = KvDb::open(&index_path).unwrap();
    
    let state = AppState {
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
async fn test_basic_write_read_delete() {
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "test_file";
    let data = vec![0u8; 10 * 1024 * 1024]; // 10 MiB
    
    // PUT: Write the object
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.clone()))
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);
    
    let etag = put_response.headers().get("etag").unwrap().to_str().unwrap();
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    
    // GET: Read the object back
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    
    let get_etag = get_response.headers().get("etag").unwrap().to_str().unwrap();
    assert_eq!(get_etag, etag);
    
    let body_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes.len(), data.len());
    assert_eq!(body_bytes.to_vec(), data);
    
    // HEAD: Check the object exists
    let head_request = Request::builder()
        .method("HEAD")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let head_response = app.clone().oneshot(head_request).await.unwrap();
    assert_eq!(head_response.status(), StatusCode::OK);
    
    let head_state = head_response.headers().get("state").unwrap().to_str().unwrap();
    assert_eq!(head_state, "Committed");
    
    // DELETE: Remove the object
    let delete_request = Request::builder()
        .method("DELETE")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let delete_response = app.clone().oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
    
    // GET after DELETE: Should return 404
    let get_request_after_delete = Request::builder()
        .method("GET")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let get_response_after_delete = app.clone().oneshot(get_request_after_delete).await.unwrap();
    assert_eq!(get_response_after_delete.status(), StatusCode::NOT_FOUND);
    
    // HEAD after DELETE: Should return 404
    let head_request_after_delete = Request::builder()
        .method("HEAD")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let head_response_after_delete = app.clone().oneshot(head_request_after_delete).await.unwrap();
    assert_eq!(head_response_after_delete.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_write_once_conflict() {
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "write_once_key";
    let data = b"test data";
    
    // First PUT: Should succeed
    let put_request1 = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.as_slice()))
        .unwrap();
    
    let put_response1 = app.clone().oneshot(put_request1).await.unwrap();
    assert_eq!(put_response1.status(), StatusCode::CREATED);
    
    // Second PUT: Should return 409 Conflict
    let put_request2 = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.as_slice()))
        .unwrap();
    
    let put_response2 = app.clone().oneshot(put_request2).await.unwrap();
    assert_eq!(put_response2.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_head_operations() {
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "head_test_key";
    let data = b"test data for head";
    
    // HEAD on non-existent key: Should return 404
    let head_request_missing = Request::builder()
        .method("HEAD")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let head_response_missing = app.clone().oneshot(head_request_missing).await.unwrap();
    assert_eq!(head_response_missing.status(), StatusCode::NOT_FOUND);
    
    // PUT the object
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.as_slice()))
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);
    
    // HEAD on existing key: Should return 200
    let head_request_exists = Request::builder()
        .method("HEAD")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let head_response_exists = app.clone().oneshot(head_request_exists).await.unwrap();
    assert_eq!(head_response_exists.status(), StatusCode::OK);
    
    let state = head_response_exists.headers().get("state").unwrap().to_str().unwrap();
    assert_eq!(state, "Committed");
    
    let etag = head_response_exists.headers().get("etag").unwrap().to_str().unwrap();
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    
    let content_length = head_response_exists.headers().get("content-length").unwrap().to_str().unwrap();
    assert_eq!(content_length, data.len().to_string());
}

#[tokio::test]
async fn test_large_object() {
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "large_object";
    // Use a smaller size for testing (100MB instead of 1GB for speed)
    let size = 100 * 1024 * 1024; // 100 MB
    
    // Generate data in chunks to avoid memory issues
    let chunk_size = 1024 * 1024; // 1MB chunks
    let mut data = Vec::new();
    for i in 0..(size / chunk_size) {
        let mut chunk = vec![0u8; chunk_size];
        // Add some pattern to detect corruption
        for (j, byte) in chunk.iter_mut().enumerate() {
            *byte = ((i + j) % 256) as u8;
        }
        data.extend_from_slice(&chunk);
    }
    
    let start_time = std::time::Instant::now();
    
    // PUT: Write the large object
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.clone()))
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);
    
    let upload_duration = start_time.elapsed();
    println!("Upload duration: {:?}", upload_duration);
    
    // Verify reasonable upload time (should complete within 30 seconds for 100MB)
    assert!(upload_duration.as_secs() < 30, "Upload took too long: {:?}", upload_duration);
    
    // GET: Read the large object back
    let get_start = std::time::Instant::now();
    
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/{}", key))
        .body(Body::empty())
        .unwrap();
    
    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    
    let body_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
    let download_duration = get_start.elapsed();
    
    println!("Download duration: {:?}", download_duration);
    assert!(download_duration.as_secs() < 30, "Download took too long: {:?}", download_duration);
    
    // Verify data integrity
    assert_eq!(body_bytes.len(), data.len());
    assert_eq!(body_bytes.to_vec(), data);
}

#[tokio::test]
async fn test_payload_too_large() {
    let (app, _temp_dir) = create_test_app().await;
    
    let key = "too_large_key";
    // Try to upload more than the max size (1GB + 1 byte)
    let size = (1024 * 1024 * 1024u64) + 1;
    
    let put_request = Request::builder()
        .method("PUT")
        .uri(format!("/{}", key))
        .header(header::CONTENT_LENGTH, size)
        .body(Body::from(vec![0u8; 1024])) // Small body, but large Content-Length
        .unwrap();
    
    let put_response = app.clone().oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn test_invalid_keys() {
    let (app, _temp_dir) = create_test_app().await;
    
    let data = b"test data";
    
    // Test empty key
    let put_request_empty = Request::builder()
        .method("PUT")
        .uri("/")
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.as_slice()))
        .unwrap();
    
    let put_response_empty = app.clone().oneshot(put_request_empty).await.unwrap();
    assert_eq!(put_response_empty.status(), StatusCode::NOT_FOUND); // Router doesn't match
    
    // Test path traversal - this should be rejected by the sanitize_key function
    let put_request_traversal = Request::builder()
        .method("PUT")
        .uri("/test../secret")  // Contains ".." which should be rejected
        .header(header::CONTENT_LENGTH, data.len())
        .body(Body::from(data.as_slice()))
        .unwrap();
    
    let put_response_traversal = app.clone().oneshot(put_request_traversal).await.unwrap();
    // Path traversal in URL gets rejected by router (404) or sanitize_key (500)
    assert!(put_response_traversal.status() == StatusCode::NOT_FOUND || 
           put_response_traversal.status().is_server_error() || 
           put_response_traversal.status() == StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_concurrent_operations() {
    let (app, _temp_dir) = create_test_app().await;
    
    let data = vec![0u8; 1024 * 1024]; // 1MB per operation
    let num_concurrent = 4;
    
    let mut handles = Vec::new();
    
    for i in 0..num_concurrent {
        let app_clone = app.clone();
        let data_clone = data.clone();
        let key = format!("concurrent_key_{}", i);
        
        let handle = tokio::spawn(async move {
            let put_request = Request::builder()
                .method("PUT")
                .uri(format!("/{}", key))
                .header(header::CONTENT_LENGTH, data_clone.len())
                .body(Body::from(data_clone))
                .unwrap();
            
            let put_response = app_clone.oneshot(put_request).await.unwrap();
            assert_eq!(put_response.status(), StatusCode::CREATED);
            
            key
        });
        
        handles.push(handle);
    }
    
    // Wait for all uploads to complete
    let mut keys = Vec::new();
    for handle in handles {
        keys.push(handle.await.unwrap());
    }
    
    // Verify all objects can be read back
    for key in keys {
        let get_request = Request::builder()
            .method("GET")
            .uri(format!("/{}", key))
            .body(Body::empty())
            .unwrap();
        
        let get_response = app.clone().oneshot(get_request).await.unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        
        let body_bytes = get_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes.len(), data.len());
    }
}

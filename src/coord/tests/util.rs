#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use axum_server::Server;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use common::api_error::ApiError;
use common::file_utils::meta_key_for;
use common::schemas::{BlobHead, ListResponse, SweepTmpResponse};
use common::time_utils::utc_now_ms;
use common::url_utils::node_id_from_url;
use coord::core::meta::{KvDb, Meta};
use coord::core::node::NodeInfo;

/// Initialize tracing for tests
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();
}

/// Create a temporary RocksDB instance
pub fn mk_temp_db() -> Result<(KvDb, TempDir)> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    let db = KvDb::open(&db_path)?;
    Ok((db, temp_dir))
}

/// Create a NodeInfo from a base URL
pub fn mk_node_info(base_url: &str) -> NodeInfo {
    NodeInfo {
        node_id: node_id_from_url(base_url),
        public_url: base_url.to_string(),
        internal_url: base_url.to_string(),
        subvols: 1,
        capacity_bytes: Some(1024 * 1024 * 1024), // 1GB
        used_bytes: Some(0),
        version: Some("test".to_string()),
        last_heartbeat_ms: utc_now_ms(),
        status: coord::core::node::NodeStatus::Alive,
    }
}

/// Generate a test key with content
pub fn make_key(content: &[u8]) -> (String, u64, String) {
    let hash_hex = blake3::hash(content).to_hex();
    let name = format!("key-{}", &hash_hex.as_str()[..8]);
    let key_enc = utf8_percent_encode(&name, NON_ALPHANUMERIC).to_string();
    let size = content.len() as u64;
    let etag_hex = blake3::hash(content).to_hex().to_string();
    (key_enc, size, etag_hex)
}

/// Write a blob to the volume's file system
pub async fn write_blob(volume_root: &Path, key_enc: &str, content: &[u8]) -> Result<()> {
    let decoded = percent_decode_str(key_enc).decode_utf8()?;
    let key = decoded.as_ref();

    // Create sharded path: blobs/aa/bb/key
    let hash = blake3::hash(key.as_bytes());
    let hex = hash.to_hex();
    let aa = &hex.as_str()[0..2];
    let bb = &hex.as_str()[2..4];

    let blob_dir = volume_root.join("blobs").join(aa).join(bb);
    fs::create_dir_all(&blob_dir).await?;

    let blob_path = blob_dir.join(key);
    fs::write(blob_path, content).await?;
    Ok(())
}

/// Read a blob from the volume's file system
pub async fn read_blob(volume_root: &Path, key_enc: &str) -> Result<Vec<u8>> {
    let decoded = percent_decode_str(key_enc).decode_utf8()?;
    let key = decoded.as_ref();

    let hash = blake3::hash(key.as_bytes());
    let hex = hash.to_hex();
    let aa = &hex.as_str()[0..2];
    let bb = &hex.as_str()[2..4];

    let blob_path = volume_root.join("blobs").join(aa).join(bb).join(key);
    let content = fs::read(blob_path).await?;
    Ok(content)
}

/// Check if a blob exists in the volume's file system
pub async fn blob_exists(volume_root: &Path, key_enc: &str) -> bool {
    match read_blob(volume_root, key_enc).await {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Assert a meta exists in the database with the given properties
pub fn assert_meta<F>(db: &KvDb, key_enc: &str, check: F) -> Result<()>
where
    F: FnOnce(&Meta) -> Result<()>,
{
    let meta_key = meta_key_for(key_enc);
    let meta: Option<Meta> = db.get(&meta_key)?;
    match meta {
        Some(m) => check(&m),
        None => bail!("Meta not found for key: {}", key_enc),
    }
}

/// Volume server options for fault injection and behavior control
#[derive(Debug, Clone, Default)]
pub struct VolumeOptions {
    pub fail_admin_blob_once: bool,
    pub fail_admin_list_once: bool,
    pub fail_internal_delete_once: bool,
    pub fail_internal_prepare_once: bool,
    pub fail_internal_pull_once: bool,
    pub fail_internal_commit_once: bool,
    pub inject_latency_ms: Option<u64>,
    pub pagination_limit: Option<usize>, // Force small pages for testing
    pub corrupt_size_for_key: Option<String>, // Return wrong size for this key
    pub corrupt_etag_for_key: Option<String>, // Return wrong etag for this key
}

/// Shared state for the fake volume server
#[derive(Debug, Clone)]
pub struct FakeVolumeState {
    pub volume_root: PathBuf,
    pub node_id: String,
    pub options: Arc<Mutex<VolumeOptions>>,
    pub temp_uploads: Arc<Mutex<HashMap<String, (u64, Vec<u8>)>>>, // upload_id -> (expected_size, content)
    pub call_counts: Arc<Mutex<HashMap<String, usize>>>,           // endpoint -> count
}

impl FakeVolumeState {
    fn new(volume_root: PathBuf, node_id: String, options: VolumeOptions) -> Self {
        Self {
            volume_root,
            node_id,
            options: Arc::new(Mutex::new(options)),
            temp_uploads: Arc::new(Mutex::new(HashMap::new())),
            call_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn increment_call_count(&self, endpoint: &str) {
        let mut counts = self.call_counts.lock().unwrap();
        *counts.entry(endpoint.to_string()).or_insert(0) += 1;
    }

    pub fn get_call_count(&self, endpoint: &str) -> usize {
        let counts = self.call_counts.lock().unwrap();
        counts.get(endpoint).copied().unwrap_or(0)
    }
}

/// Query parameters for /admin/list
#[derive(Deserialize)]
struct AdminListQuery {
    limit: Option<usize>,
    after: Option<String>,
}

/// Query parameters for /admin/blob
#[derive(Deserialize)]
struct AdminBlobQuery {
    key: String,
    deep: Option<String>,
}

/// Query parameters for /internal/prepare
#[derive(Deserialize)]
struct InternalPrepareQuery {
    key: String,
    upload_id: String,
    expected_size: Option<u64>,
}

/// Query parameters for /internal/pull
#[derive(Deserialize)]
struct InternalPullQuery {
    upload_id: String,
    from: String, // Source URL or direct content injection
}

/// Query parameters for /internal/commit
#[derive(Deserialize)]
struct InternalCommitQuery {
    upload_id: String,
    key: String,
}

/// Query parameters for /internal/delete
#[derive(Deserialize)]
struct InternalDeleteQuery {
    key: String,
}

/// Response for /internal/pull
#[derive(Serialize)]
struct PullResponse {
    size: u64,
    etag: String,
}

// Handler implementations

pub async fn admin_list_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<ListResponse>, ApiError> {
    state.increment_call_count("admin_list");

    // Check fault injection
    let should_fail = {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_admin_list_once {
            opts.fail_admin_list_once = false;
            true
        } else {
            false
        }
    };

    if should_fail {
        return Err(ApiError::Any(anyhow::anyhow!(
            "Fault injection: admin_list failed"
        )));
    }

    let latency_ms = {
        let opts = state.options.lock().unwrap();
        opts.inject_latency_ms
    };

    if let Some(ms) = latency_ms {
        sleep(Duration::from_millis(ms)).await;
    }

    let limit = {
        let opts = state.options.lock().unwrap();
        opts.pagination_limit.unwrap_or(query.limit.unwrap_or(1000))
    };

    // Walk the blobs directory and collect keys
    let mut keys = Vec::new();
    let blobs_dir = state.volume_root.join("blobs");

    if blobs_dir.exists() {
        let mut entries = Vec::new();
        collect_blob_keys(&blobs_dir, &mut entries).await;
        entries.sort();

        // Apply pagination
        let start_idx = match &query.after {
            Some(after) => entries
                .iter()
                .position(|k| k > after)
                .unwrap_or(entries.len()),
            None => 0,
        };

        let end_idx = (start_idx + limit).min(entries.len());
        keys = entries[start_idx..end_idx].to_vec();
    }

    let next_after = if keys.len() == limit && keys.len() > 0 {
        Some(keys.last().unwrap().clone())
    } else {
        None
    };

    Ok(Json(ListResponse { keys, next_after }))
}

pub async fn admin_blob_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<AdminBlobQuery>,
) -> Result<Json<BlobHead>, ApiError> {
    state.increment_call_count("admin_blob");

    // Check fault injection
    let should_fail = {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_admin_blob_once {
            opts.fail_admin_blob_once = false;
            true
        } else {
            false
        }
    };

    if should_fail {
        return Err(ApiError::Any(anyhow::anyhow!(
            "Fault injection: admin_blob failed"
        )));
    }

    let latency_ms = {
        let opts = state.options.lock().unwrap();
        opts.inject_latency_ms
    };

    if let Some(ms) = latency_ms {
        sleep(Duration::from_millis(ms)).await;
    }

    let deep = query.deep.as_deref() == Some("true");

    match read_blob(&state.volume_root, &query.key).await {
        Ok(content) => {
            let mut size = content.len() as u64;
            let mut etag = if deep {
                Some(blake3::hash(&content).to_hex().to_string())
            } else {
                None
            };

            // Apply corruption for testing
            {
                let opts = state.options.lock().unwrap();
                if let Some(corrupt_key) = &opts.corrupt_size_for_key {
                    if query.key == *corrupt_key {
                        size += 1; // Wrong size
                    }
                }
                if let Some(corrupt_key) = &opts.corrupt_etag_for_key {
                    if query.key == *corrupt_key && deep {
                        etag = Some("corrupt_etag".to_string());
                    }
                }
            }

            Ok(Json(BlobHead {
                exists: true,
                size,
                etag,
            }))
        }
        Err(_) => Ok(Json(BlobHead {
            exists: false,
            size: 0,
            etag: None,
        })),
    }
}

pub async fn internal_prepare_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<InternalPrepareQuery>,
) -> Result<StatusCode, ApiError> {
    state.increment_call_count("internal_prepare");

    // Check fault injection
    {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_internal_prepare_once {
            opts.fail_internal_prepare_once = false;
            return Err(ApiError::Any(anyhow::anyhow!(
                "Fault injection: prepare failed"
            )));
        }
    }

    // Store the upload preparation
    let mut uploads = state.temp_uploads.lock().unwrap();
    uploads.insert(
        query.upload_id,
        (query.expected_size.unwrap_or(0), Vec::new()),
    );

    Ok(StatusCode::OK)
}

pub async fn internal_pull_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<InternalPullQuery>,
) -> Result<Json<PullResponse>, ApiError> {
    state.increment_call_count("internal_pull");

    // Check fault injection
    {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_internal_pull_once {
            opts.fail_internal_pull_once = false;
            return Err(ApiError::Any(anyhow::anyhow!(
                "Fault injection: pull failed"
            )));
        }
    }

    // For testing, we'll simulate pulling by either:
    // 1. Fetching from the "from" URL if it's a real URL
    // 2. Using injected content if "from" starts with "inject:"
    let content = if query.from.starts_with("inject:") {
        let data = query.from.strip_prefix("inject:").unwrap();
        // Simple hex decoding for test content injection
        hex::decode(data).map_err(|_| ApiError::Any(anyhow::anyhow!("Bad hex data")))?
    } else {
        // Try to fetch from the source URL
        let client = Client::new();
        let resp = client
            .get(&query.from)
            .send()
            .await
            .map_err(|_| ApiError::Any(anyhow::anyhow!("Failed to fetch from source")))?;
        resp.bytes()
            .await
            .map_err(|_| ApiError::Any(anyhow::anyhow!("Failed to read response")))?
            .to_vec()
    };

    let size = content.len() as u64;
    let etag = blake3::hash(&content).to_hex().to_string();

    // Store the content for commit
    {
        let mut uploads = state.temp_uploads.lock().unwrap();
        if let Some((expected_size, stored_content)) = uploads.get_mut(&query.upload_id) {
            *stored_content = content;
            if *expected_size > 0 && *expected_size != size {
                return Err(ApiError::Any(anyhow::anyhow!("Size mismatch")));
            }
        } else {
            return Err(ApiError::Any(anyhow::anyhow!("Upload not prepared")));
        }
    }

    Ok(Json(PullResponse { size, etag }))
}

pub async fn internal_commit_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<InternalCommitQuery>,
) -> Result<StatusCode, ApiError> {
    state.increment_call_count("internal_commit");

    // Check fault injection
    {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_internal_commit_once {
            opts.fail_internal_commit_once = false;
            return Err(ApiError::Any(anyhow::anyhow!(
                "Fault injection: commit failed"
            )));
        }
    }

    // Move temp upload to final blob
    let content = {
        let mut uploads = state.temp_uploads.lock().unwrap();
        match uploads.remove(&query.upload_id) {
            Some((_, content)) => content,
            None => return Err(ApiError::Any(anyhow::anyhow!("Upload not found"))),
        }
    };

    write_blob(&state.volume_root, &query.key, &content)
        .await
        .map_err(|_| ApiError::Any(anyhow::anyhow!("Failed to write blob")))?;

    Ok(StatusCode::OK)
}

pub async fn internal_delete_handler(
    State(state): State<FakeVolumeState>,
    Query(query): Query<InternalDeleteQuery>,
) -> Result<StatusCode, ApiError> {
    state.increment_call_count("internal_delete");

    // Check fault injection
    {
        let mut opts = state.options.lock().unwrap();
        if opts.fail_internal_delete_once {
            opts.fail_internal_delete_once = false;
            return Err(ApiError::Any(anyhow::anyhow!(
                "Fault injection: delete failed"
            )));
        }
    }

    // Delete the blob if it exists (idempotent)
    let decoded = percent_decode_str(&query.key)
        .decode_utf8()
        .map_err(|_| ApiError::Any(anyhow::anyhow!("Invalid key encoding")))?;
    let key = decoded.as_ref();

    let hash = blake3::hash(key.as_bytes());
    let hex = hash.to_hex();
    let aa = &hex.as_str()[0..2];
    let bb = &hex.as_str()[2..4];

    let blob_path = state.volume_root.join("blobs").join(aa).join(bb).join(key);
    let _ = fs::remove_file(blob_path).await; // Ignore errors (idempotent)

    Ok(StatusCode::OK)
}

pub async fn admin_sweep_tmp_handler(
    State(state): State<FakeVolumeState>,
) -> Result<Json<SweepTmpResponse>, ApiError> {
    state.increment_call_count("admin_sweep_tmp");

    // For testing, just return a fixed response
    Ok(Json(SweepTmpResponse {
        removed: 0,
        kept_recent: 0,
    }))
}

// Helper function to recursively collect blob keys
fn collect_blob_keys<'a>(
    dir: &'a Path,
    keys: &'a mut Vec<String>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        let mut read_dir = match fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(_) => return,
        };

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                collect_blob_keys(&path, keys).await;
            } else if let Some(filename) = path.file_name() {
                if let Some(key) = filename.to_str() {
                    // Percent-encode the key
                    let key_enc = utf8_percent_encode(key, NON_ALPHANUMERIC).to_string();
                    keys.push(key_enc);
                }
            }
        }
    })
}

/// Handle for a fake volume server
pub struct FakeVolumeHandle {
    pub base_url: String,
    pub internal_url: String,
    pub node_id: String,
    pub state: FakeVolumeState,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: JoinHandle<Result<(), anyhow::Error>>,
}

impl FakeVolumeHandle {
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
        let _ = self.handle.await;
        Ok(())
    }
}

/// Spawn a fake volume server with initial blobs and options
pub async fn spawn_volume(
    initial_blobs: Vec<(String, Vec<u8>)>, // (key_enc, content)
    options: VolumeOptions,
) -> Result<FakeVolumeHandle> {
    let temp_dir = TempDir::new()?;
    let volume_root = temp_dir.path().to_path_buf();

    // Create directory structure
    fs::create_dir_all(volume_root.join("blobs")).await?;
    fs::create_dir_all(volume_root.join("tmp")).await?;

    // Write initial blobs
    for (key_enc, content) in initial_blobs {
        write_blob(&volume_root, &key_enc, &content).await?;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);
    let node_id = node_id_from_url(&base_url);

    let state = FakeVolumeState::new(volume_root, node_id.clone(), options);

    let app = Router::new()
        .route("/admin/list", get(admin_list_handler))
        .route("/admin/blob", get(admin_blob_handler))
        .route("/admin/sweep_tmp", post(admin_sweep_tmp_handler))
        .route("/internal/prepare", post(internal_prepare_handler))
        .route("/internal/pull", post(internal_pull_handler))
        .route("/internal/commit", post(internal_commit_handler))
        .route("/internal/delete", post(internal_delete_handler))
        .with_state(state.clone());

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        let server = Server::from_tcp(listener.into_std()?).serve(app.into_make_service());

        tokio::select! {
            res = server => res.map_err(anyhow::Error::from),
            _ = shutdown_rx.changed() => Ok(()),
        }
    });

    // Leak the temp_dir so it persists for the lifetime of the server
    std::mem::forget(temp_dir);

    Ok(FakeVolumeHandle {
        base_url: base_url.clone(),
        internal_url: base_url,
        node_id,
        state,
        shutdown_tx,
        handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fake_volume_basic() -> Result<()> {
        init_tracing();

        let (key_enc, _, _) = make_key(b"test content");
        let initial_blobs = vec![(key_enc.clone(), b"test content".to_vec())];

        let volume = spawn_volume(initial_blobs, VolumeOptions::default()).await?;
        let client = Client::new();

        // Test /admin/list
        let url = format!("{}/admin/list", volume.base_url);
        let resp = client.get(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::OK);
        let list: ListResponse = resp.json().await?;
        assert_eq!(list.keys.len(), 1);
        assert_eq!(list.keys[0], key_enc);

        // Test /admin/blob
        let url = format!("{}/admin/blob?key={}&deep=true", volume.base_url, key_enc);
        let resp = client.get(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::OK);
        let blob: BlobHead = resp.json().await?;
        assert!(blob.exists);
        assert_eq!(blob.size, 12);
        assert!(blob.etag.is_some());

        volume.shutdown().await?;
        Ok(())
    }
}

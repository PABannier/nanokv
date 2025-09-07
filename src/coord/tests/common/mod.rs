#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum_server::Server;
use reqwest::Client;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use common::file_utils::init_dirs;
use common::schemas::JoinRequest;
use coord::core::health::node_status_sweeper;
use coord::core::meta::KvDb;
use coord::core::node::{NodeInfo, NodeStatus};
use coord::core::placement::rank_nodes;
use coord::core::routes::{delete_object, get_object, put_object, head_object, list_nodes as coord_list_nodes, join_node, heartbeat};
use coord::core::state::CoordinatorState;
use volume::health::heartbeat_loop;
use volume::routes;
use volume::state::VolumeState;
use volume::store::disk_usage;

pub struct TestServer {
    pub handle: JoinHandle<Result<(), anyhow::Error>>,
    pub shutdown_tx: watch::Sender<bool>,
    pub addr: SocketAddr,
    pub url: String,
}

impl TestServer {
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
        let _ = self.handle.await;
        Ok(())
    }
}

pub struct TestCoordinator {
    pub server: TestServer,
    pub state: CoordinatorState,
    pub data_dir: TempDir,
    pub sweeper_handle: JoinHandle<Result<(), anyhow::Error>>,
}

impl TestCoordinator {
    pub async fn new() -> Result<Self> {
        Self::with_config(1500, 3000, 500).await
    }

    pub async fn new_with_replicas(n_replicas: usize) -> Result<Self> {
        Self::with_config_and_replicas(1500, 3000, 500, n_replicas).await
    }

    pub async fn with_config(alive_ms: u64, down_ms: u64, sweep_ms: u64) -> Result<Self> {
        Self::with_config_and_replicas(alive_ms, down_ms, sweep_ms, 1).await
    }

    pub async fn with_config_and_replicas(alive_ms: u64, down_ms: u64, sweep_ms: u64, n_replicas: usize) -> Result<Self> {
        let data_dir = TempDir::new()?;
        let index_dir = data_dir.path().join("index");

        init_dirs(data_dir.path()).await?;
        let db = KvDb::open(&index_dir)?;

        let state = CoordinatorState {
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            inflight: Arc::new(tokio::sync::Semaphore::new(4)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            db,
            max_size: 1024 * 1024 * 1024, // 1GB
            hb_alive_secs: alive_ms / 1000,
            hb_down_secs: down_ms / 1000,
            node_status_sweep_secs: sweep_ms / 1000,
            n_replicas,
        };

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweeper_state = state.clone();
        let sweeper_handle = tokio::spawn(node_status_sweeper(
            sweeper_state,
            Duration::from_millis(sweep_ms),
            shutdown_rx,
        ));

        let app = Router::new()
            .route("/{key}", put(put_object).get(get_object).delete(delete_object).head(head_object))
            .route("/admin/nodes", get(coord_list_nodes))
            .route("/admin/join", post(join_node))
            .route("/admin/heartbeat", post(heartbeat))
            // Debug endpoint (test-only)
            .route("/debug/placement/{key}", get(coord::core::debug::debug_placement))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let url = format!("http://{}", addr);

        let (server_shutdown_tx, mut server_shutdown_rx) = watch::channel(false);
        let server_handle = tokio::spawn(async move {
            let server = Server::from_tcp(listener.into_std()?)
                .serve(app.into_make_service());

            tokio::select! {
                res = server => res.map_err(anyhow::Error::from),
                _ = server_shutdown_rx.changed() => Ok(()),
            }
        });

        let server = TestServer {
            handle: server_handle,
            shutdown_tx: server_shutdown_tx,
            addr,
            url: url.clone(),
        };

        Ok(TestCoordinator {
            server,
            state,
            data_dir,
            sweeper_handle,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.server.shutdown_tx.send(true);
        self.sweeper_handle.abort();
        self.server.handle.abort();
        let _ = self.server.handle.await;
        let _ = self.sweeper_handle.await;
        Ok(())
    }

    pub fn url(&self) -> &str {
        &self.server.url
    }
}

pub struct TestVolume {
    pub server: TestServer,
    pub state: VolumeState,
    pub data_dir: TempDir,
    pub heartbeat_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

impl TestVolume {
    pub async fn new(coordinator_url: String, node_id: String) -> Result<Self> {
        let data_dir = TempDir::new()?;

        init_dirs(data_dir.path()).await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let url = format!("http://{}", addr);

        let state = VolumeState {
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            data_root: Arc::new(data_dir.path().to_path_buf()),
            coordinator_url,
            node_id,
            public_url: url.clone(),
            internal_url: url.clone(),
            subvols: 1,
            heartbeat_interval_secs: 1,
            http_timeout_secs: 10,
            fault_injector: Arc::new(volume::fault_injection::FaultInjector::new()),
        };

        // For Phase 2, volumes need the full set of internal endpoints
        let app = Router::new()
            .route("/blobs/{key}", get(routes::get_handler))
            .route("/internal/prepare", post(routes::prepare_handler))
            .route("/internal/write/{upload_id}", put(routes::write_handler))
            .route("/internal/read/{upload_id}", get(routes::read_handler))
            .route("/internal/pull", post(routes::pull_handler))
            .route("/internal/commit", post(routes::commit_handler))
            .route("/internal/abort", post(routes::abort_handler))
            .route("/internal/delete/{key}", delete(routes::delete_handler))
            // Fault injection endpoints (test-only)
            .route("/admin/fail/prepare", post(volume::fault_injection::fail_prepare))
            .route("/admin/fail/pull", post(volume::fault_injection::fail_pull))
            .route("/admin/fail/commit", post(volume::fault_injection::fail_commit))
            .route("/admin/fail/read_tmp", post(volume::fault_injection::fail_read_tmp))
            .route("/admin/fail/etag_mismatch", post(volume::fault_injection::fail_etag_mismatch))
            .route("/admin/inject/latency", post(volume::fault_injection::inject_latency))
            .route("/admin/pause", post(volume::fault_injection::pause_server))
            .route("/admin/resume", post(volume::fault_injection::resume_server))
            .route("/admin/kill", post(volume::fault_injection::kill_server))
            .route("/admin/reset", post(volume::fault_injection::reset_faults))
            .with_state(state.clone());

        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let server_handle = tokio::spawn(async move {
            let server = Server::from_tcp(listener.into_std()?)
                .serve(app.into_make_service());
            server.await.map_err(anyhow::Error::from)
        });

        let server = TestServer {
            handle: server_handle,
            shutdown_tx,
            addr,
            url: url.clone(),
        };

        Ok(TestVolume {
            server,
            state,
            data_dir,
            heartbeat_handle: None,
        })
    }

    pub async fn join_coordinator(&self) -> Result<()> {
        let (used, cap) = disk_usage(&self.state.data_root)?;

        let payload = JoinRequest {
            node_id: self.state.node_id.clone(),
            public_url: self.state.public_url.clone(),
            internal_url: self.state.internal_url.clone(),
            subvols: self.state.subvols,
            capacity_bytes: cap,
            used_bytes: used,
            version: Some("test".to_string()),
        };

        let url = format!("{}/admin/join", self.state.coordinator_url);
        let resp = self.state.http_client.post(url).json(&payload).send().await?;

        if !resp.status().is_success() {
            anyhow::bail!("join failed: {}", resp.status());
        }

        Ok(())
    }

    pub fn start_heartbeat(&mut self, interval_ms: u64) -> Result<()> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let state = self.state.clone();

        let handle = tokio::spawn(heartbeat_loop(
            state,
            Duration::from_millis(interval_ms),
            shutdown_rx,
        ));

        self.heartbeat_handle = Some(handle);
        Ok(())
    }

    pub fn stop_heartbeat(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.stop_heartbeat();
        self.server.shutdown().await
    }

    pub fn url(&self) -> &str {
        &self.server.url
    }
}

// HTTP client utilities
pub async fn put_via_coordinator(
    client: &Client,
    coord_url: &str,
    key: &str,
    bytes: Vec<u8>,
) -> Result<(reqwest::StatusCode, String, u64)> {
    let url = format!("{}/{}", coord_url, key);
    let len = bytes.len() as u64; // Get length before moving bytes
    let resp = client.put(url).body(bytes).send().await?;

    let status = resp.status();

    // If we get an error, print the response body for debugging
    if !status.is_success() {
        let error_body = resp.text().await.unwrap_or_else(|_| "Failed to read error body".to_string());
        eprintln!("PUT request failed with status {}: {}", status, error_body);
        return Ok((status, String::new(), 0));
    }

    let etag = resp.headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_matches('"')
        .to_string();

    // Debug output removed for cleaner test runs

    Ok((status, etag, len))
}

pub async fn get_via_coordinator(
    client: &Client,
    coord_url: &str,
    key: &str,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let url = format!("{}/{}", coord_url, key);
    let resp = client.get(url).send().await?;

    let status = resp.status();
    let bytes = resp.bytes().await?.to_vec();

    Ok((status, bytes))
}

pub async fn get_redirect_location(
    _client: &Client,
    coord_url: &str,
    key: &str,
) -> Result<(reqwest::StatusCode, Option<String>)> {
    // Create a client that doesn't follow redirects
    let no_redirect_client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let url = format!("{}/{}", coord_url, key);
    let resp = no_redirect_client.get(url)
        .header("User-Agent", "test")
        .send()
        .await?;

    let status = resp.status();
    let location = resp.headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Ok((status, location))
}

pub async fn delete_via_coordinator(
    client: &Client,
    coord_url: &str,
    key: &str,
) -> Result<reqwest::StatusCode> {
    let url = format!("{}/{}", coord_url, key);
    let resp = client.delete(url).send().await?;
    Ok(resp.status())
}

pub async fn list_nodes(
    client: &Client,
    coord_url: &str,
) -> Result<Vec<NodeInfo>> {
    let url = format!("{}/admin/nodes", coord_url);
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("list_nodes failed: {}", resp.status());
    }

    let nodes: Vec<NodeInfo> = resp.json().await?;
    Ok(nodes)
}

// Utility functions
pub async fn wait_until<F, Fut>(timeout_ms: u64, mut check_fn: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_millis(timeout_ms);

    loop {
        if check_fn().await? {
            return Ok(());
        }

        if start.elapsed() > timeout_duration {
            anyhow::bail!("wait_until timed out after {}ms", timeout_ms);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub fn generate_random_bytes(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

// Test placement utility - expose coordinator's placement logic for testing
pub fn test_placement(key: &str, nodes: &[NodeInfo]) -> Option<String> {
    if nodes.is_empty() {
        return None;
    }

    let alive_nodes: Vec<NodeInfo> = nodes.iter()
        .filter(|n| n.status == NodeStatus::Alive)
        .cloned()
        .collect();

    if alive_nodes.is_empty() {
        return None;
    }

    let ranked = rank_nodes(key, &alive_nodes);
    ranked.first().map(|n| n.node_id.clone())
}

// Phase 2 test utilities for replication

/// Get the placement of N replicas for a key
pub fn test_placement_n(key: &str, nodes: &[NodeInfo], n: usize) -> Vec<String> {
    if nodes.is_empty() {
        return vec![];
    }

    let alive_nodes: Vec<NodeInfo> = nodes.iter()
        .filter(|n| n.status == NodeStatus::Alive)
        .cloned()
        .collect();

    if alive_nodes.is_empty() {
        return vec![];
    }

    let ranked = rank_nodes(key, &alive_nodes);
    ranked.into_iter().take(n).map(|n| n.node_id.clone()).collect()
}

/// Create a client that follows redirects for end-to-end GET tests
pub fn create_redirect_client() -> Result<Client> {
    Ok(Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(Duration::from_secs(30))
        .build()?)
}

/// Create a client that doesn't follow redirects for testing 302 responses
pub fn create_no_redirect_client() -> Result<Client> {
    Ok(Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?)
}

/// Follow a redirect and get the final response body
pub async fn follow_redirect_get(
    client: &Client,
    coord_url: &str,
    key: &str,
) -> Result<Vec<u8>> {
    let url = format!("{}/{}", coord_url, key);
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("GET request failed: {}", resp.status());
    }

    Ok(resp.bytes().await?.to_vec())
}

/// Read metadata directly from RocksDB for testing
pub fn meta_of(db: &KvDb, key: &str) -> Result<Option<Meta>> {
    use common::file_utils::meta_key_for;
    let meta_key = meta_key_for(&common::file_utils::sanitize_key(key)?);
    db.get(&meta_key)
}

/// Check which volumes have the blob file for a key
pub fn which_volume_has_file(volumes: &[&TestVolume], key: &str) -> Result<Vec<String>> {
    use common::file_utils::{sanitize_key, blob_path};

    let key_enc = sanitize_key(key)?;
    let mut found_nodes = Vec::new();

    for vol in volumes {
        let path = blob_path(&vol.state.data_root, &key_enc);
        if path.exists() {
            found_nodes.push(vol.state.node_id.clone());
        }
    }

    Ok(found_nodes)
}

/// Test harness for creating multiple volumes
pub async fn create_volumes(coord_url: &str, count: usize) -> Result<Vec<TestVolume>> {
    let mut volumes = Vec::new();

    for i in 0..count {
        let node_id = format!("vol-{}", i + 1);
        let volume = TestVolume::new(coord_url.to_string(), node_id).await?;
        volumes.push(volume);
    }

    Ok(volumes)
}

/// Join all volumes to coordinator and start heartbeats
pub async fn join_and_heartbeat_volumes(volumes: &mut [TestVolume], heartbeat_interval_ms: u64) -> Result<()> {
    for vol in volumes.iter_mut() {
        vol.join_coordinator().await?;
        vol.start_heartbeat(heartbeat_interval_ms)?;
    }
    Ok(())
}

/// Wait for all volumes to be alive
pub async fn wait_for_volumes_alive(client: &Client, coord_url: &str, expected_count: usize, timeout_ms: u64) -> Result<()> {
    wait_until(timeout_ms, || async {
        let nodes = list_nodes(client, coord_url).await?;
        Ok(nodes.len() == expected_count && nodes.iter().all(|n| n.status == NodeStatus::Alive))
    }).await
}

/// Shutdown multiple volumes
pub async fn shutdown_volumes(volumes: Vec<TestVolume>) -> Result<()> {
    for vol in volumes {
        vol.shutdown().await?;
    }
    Ok(())
}

/// Get placement from coordinator debug endpoint (assumed to exist in Phase 2)
pub async fn get_debug_placement(
    client: &Client,
    coord_url: &str,
    key: &str,
) -> Result<Vec<String>> {
    let url = format!("{}/debug/placement/{}", coord_url, key);
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("Debug placement request failed: {}", resp.status());
    }

    #[derive(serde::Deserialize)]
    struct PlacementResponse {
        replicas: Vec<String>,
    }

    let placement: PlacementResponse = resp.json().await?;
    Ok(placement.replicas)
}

// Import the Meta type for the meta_of function
use coord::core::meta::Meta;

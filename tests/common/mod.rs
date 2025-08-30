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
use coord::health::node_status_sweeper;
use coord::meta::KvDb;
use coord::node::{NodeInfo, NodeStatus};
use coord::placement::rank_nodes;
use coord::routes::{delete_object, get_object, put_object, head_object, list_nodes as coord_list_nodes, join_node, heartbeat};
use coord::state::CoordinatorState;
use volume::health::heartbeat_loop;
use volume::routes::{put_object as vol_put_object, get_object as vol_get_object, delete_object as vol_delete_object};
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

    pub async fn with_config(alive_ms: u64, down_ms: u64, sweep_ms: u64) -> Result<Self> {
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
        let _ = self.sweeper_handle.abort();
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
        };

        let app = Router::new()
            .route("/blobs/{key}", get(vol_get_object))
            .route("/internal/object/{key}", put(vol_put_object))
            .route("/internal/delete/{key}", delete(vol_delete_object))
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

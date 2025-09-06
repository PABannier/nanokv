use std::fmt::Display;
use tracing::info;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::Semaphore;
use clap::Parser;
use reqwest::Client;
use std::time::Duration;
use std::path::Path;
use futures_util::{stream::{FuturesUnordered}, future::try_join_all};
use serde_json;

use common::schemas::{BlobHead, ListResponse};
use common::file_utils::meta_key_for;

use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct VerifyArgs {
    /// RocksDB directory path
    #[arg(long)]
    index: String,

    /// List of node URLs
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Fix inconsistencies
    #[arg(long, default_value_t = false)]
    fix: bool,

    /// Perform deep verification (slower)
    #[arg(long, default_value_t = false)]
    deep: bool,

    /// Maximum concurrent operations
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
}

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub scanned_keys: AtomicUsize,
    pub under_replicated: AtomicUsize,
    pub corrupted: AtomicUsize,
    pub unindexed: AtomicUsize,
    pub should_gc: AtomicUsize,
}

impl Display for VerifyReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Verify report:")?;
        writeln!(f, "  Scanned keys: {}", self.scanned_keys.load(Ordering::Relaxed))?;
        writeln!(f, "  Under replicated: {}", self.under_replicated.load(Ordering::Relaxed))?;
        writeln!(f, "  Corrupted: {}", self.corrupted.load(Ordering::Relaxed))?;
        writeln!(f, "  Unindexed: {}", self.unindexed.load(Ordering::Relaxed))?;
        writeln!(f, "  Should GC: {}", self.should_gc.load(Ordering::Relaxed))?;
        Ok(())
    }
}


pub async fn verify(args: VerifyArgs) -> anyhow::Result<()> {
    let db = KvDb::open(&Path::new(&args.index))?;

    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let report = Arc::new(VerifyReport::default());

    walk_db(&db, &http, &args, &report).await?;

    walk_volumes(&db, &http, &args, &report).await?;

    info!("{}", report);

    Ok(())
}

async fn walk_db(db: &KvDb, http: &Client, args: &VerifyArgs, report: &Arc<VerifyReport>) -> anyhow::Result<()> {
    let sem = Arc::new(Semaphore::new(args.concurrency));
    let tasks = FuturesUnordered::new();

    for entry in db.iter() {
        let (k, v) = entry?;
        let key = String::from_utf8_lossy(&k).to_string();
        let meta = serde_json::from_slice::<Meta>(&v)?;

        if meta.state != TxState::Committed { continue; }

        let http = http.clone();
        let sem = sem.clone();
        let report = report.clone();
        let deep = args.deep;

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let replicas = meta.replicas;
            for replica in replicas.iter() {
                let url = format!("{}/admin/blob?key={}&deep={}", replica, key, deep);
                match http.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(head) = resp.json::<BlobHead>().await {
                            if !head.exists { report.under_replicated.fetch_add(1, Ordering::Relaxed); } else {
                                match head.etag {
                                    Some(etag) if etag != meta.etag_hex => {
                                        report.under_replicated.fetch_add(1, Ordering::Relaxed);
                                    }
                                    _ => if meta.size != head.size {
                                        report.corrupted.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }));
    }

    // Collect
    try_join_all(tasks).await.map_err(|e| anyhow::anyhow!("error in tasks: {}", e))?;

    Ok(())
}

async fn walk_volumes(db: &KvDb, http: &Client, args: &VerifyArgs, report: &Arc<VerifyReport>) -> anyhow::Result<()> {
    let volumes = args.nodes.clone();

    for vol in volumes {
        let mut after: Option<String> = None;
        loop {
            let mut url = format!("{}/admin/list?limit=1000", vol);
            if let Some(a) = &after { url.push_str("after="); url.push_str(a); }
            let resp = http.get(&url).send().await?.error_for_status()?;
            let page: ListResponse = resp.json().await?;
            for key_enc in page.keys {
                let meta_key = meta_key_for(&key_enc);
                match db.get::<Meta>(&meta_key) {
                    Ok(Some(meta)) => {
                        if meta.state == TxState::Tombstoned {
                            report.should_gc.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => { report.unindexed.fetch_add(1, Ordering::Relaxed); }
                }
            }
            after = page.next_after;
            if after.is_none() { break; }
        }
    }

    Ok(())
}
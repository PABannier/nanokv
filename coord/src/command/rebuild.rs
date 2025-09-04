use std::{fmt::Display, path::Path};
use tracing::{error, info};
use tokio::sync::Semaphore;
use clap::Parser;
use reqwest::Client;
use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};
use futures_util::{stream::{FuturesUnordered}, StreamExt};

use common::{file_utils::meta_key_for};
use common::schemas::{ListResponse, BlobHead};
use common::time_utils::utc_now_ms;

use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct RebuildArgs {
    /// RocksDB directory path
    #[arg(long)]
    index: String,

    /// List of node URLs
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Perform a dry run without making changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Perform deep verification (slower)
    #[arg(long, default_value_t = false)]
    deep: bool,

    /// Maximum concurrent operations
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
}

type KeyMetadata = (String, u64, Option<String>);

pub struct RebuildReport {
    pub scanned_keys: usize,
    pub written_metas: usize,
    pub tombstones_preserved: usize,
    pub conflicts: usize,
    pub sample_conflicts: Vec<(String, KeyMetadata)>,
}

impl Display for RebuildReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Rebuild report:")?;
        writeln!(f, "  Scanned keys: {}", self.scanned_keys)?;
        writeln!(f, "  Written metas: {}", self.written_metas)?;
        writeln!(f, "  Tombstones preserved: {}", self.tombstones_preserved)?;
        writeln!(f, "  Conflicts: {}", self.conflicts)?;
        writeln!(f, "  Sample conflicts: {:?}", self.sample_conflicts)?;
        Ok(())
    }
}


pub async fn rebuild(args: RebuildArgs) -> anyhow::Result<()> {
    let db = KvDb::open(&Path::new(&args.index))?;

    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let present = match scan_keys(&http, &args.nodes).await {
        Ok(p) => p,
        Err(e) => {
            error!("failed to scan keys: {}", e);
            return Err(anyhow::anyhow!("failed to scan keys: {}", e));
        }
    };

    let mut vol_ids: HashMap<String,String> = HashMap::new();
    for v in &args.nodes { vol_ids.insert(v.clone(), node_id_from_url(v)); }

    let report = match reconcile_keys(&http, &db, &vol_ids, &present, &args).await {
        Ok(r) => r,
        Err(e) => {
            error!("failed to reconcile keys: {}", e);
            return Err(anyhow::anyhow!("failed to reconcile keys: {}", e));
        }
    };

    info!("{}", report);

    Ok(())
}


async fn scan_keys(http: &Client, volumes: &[String]) -> anyhow::Result<HashMap<String, HashSet<String>>> {
    let mut present : HashMap<String, HashSet<String>> = HashMap::new();

    for vol in volumes {
        let mut after: Option<String> = None;
        loop {
            let mut url = format!("{}/admin/list?limit=1000", vol);
            if let Some(a) = &after { url.push_str("after="); url.push_str(a); }
            let resp = http.get(&url).send().await?.error_for_status()?;
            let page: ListResponse = resp.json().await?;
            for key_enc in page.keys {
                present.entry(key_enc).or_default().insert(vol.clone());
            }
            after = page.next_after;
            if after.is_none() { break; }
        }
    }

    Ok(present)
}

// For each key, collect size/etag from one or more vols and reconcile
async fn reconcile_keys(
    http: &Client,
    db: &KvDb,
    vol_ids: &HashMap<String, String>,
    present: &HashMap<String, HashSet<String>>,
    rebuild_args: &RebuildArgs,
) -> anyhow::Result<RebuildReport> {
    let sem = Arc::new(Semaphore::new(rebuild_args.concurrency));
    let mut tasks = FuturesUnordered::new();
    let mut report = RebuildReport {
        scanned_keys: present.len(),
        written_metas: 0,
        tombstones_preserved: 0,
        conflicts: 0,
        sample_conflicts: Vec::new(),
    };

    for (key_enc, vols) in present {
        let http = http.clone();
        let db = db.clone();
        let sem = sem.clone();
        let deep = rebuild_args.deep;
        let dry_run = rebuild_args.dry_run;
        let vol_ids = vol_ids.clone();
        let key_enc = key_enc.clone();
        let vols = vols.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            // Probe a representative subset
            let mut info: Vec<KeyMetadata> = Vec::new();
            for v in vols.iter() {
                let url = format!("{}/admin/blob?key={}&deep={}", v, key_enc, deep);
                match http.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(head) = resp.json::<BlobHead>().await {
                            if head.exists {
                                info.push((v.clone(), head.size, head.etag));
                            }
                        }
                    }
                    _ => {}
                }
                if !deep && info.len() >= 1 { 
                    break; // size-only fast path - stop after first successful probe
                }
            }

            // Reconcile: require all equal (within probed vols)
            let equal = info.windows(2).all(|w| w[0].1 == w[1].1 && w[0].2 == w[1].2);
            let replicas: Vec<String> = vols.iter().map(|v| vol_ids[v].clone()).collect();

            // Check existing meta: preserve tombstone
            let meta_key = meta_key_for(&key_enc);
            let existing = db.get::<Meta>(&meta_key).ok().flatten();

            if let Some(m) = existing.as_ref() {
                if matches!(m.state, TxState::Tombstoned) {
                    return Ok::<(bool, Option<(String, Vec<KeyMetadata>)>), anyhow::Error>((false, None)); // preserved
                }
            }

            if !equal {
                return Ok::<_, anyhow::Error>((false, Some((key_enc.clone(), info))));
            }

            // Choose canonical (first)
            let (size, etag) = info.first().map(|t| (t.1, t.2.clone())).unwrap_or((0, None));
            // If deep=false, we may not have etag; store empty and let verify/repair fill later or keep previous etag if exists
            let etag_hex = if let Some(m) = existing.as_ref().filter(|m| !m.etag_hex.is_empty()) {
                m.etag_hex.clone()
            } else {
                etag.unwrap_or_default()
            };

            if !dry_run {
                // Write Committed(meta)
                let meta = Meta {
                    state: TxState::Committed,
                    size,
                    etag_hex,
                    created_ms: utc_now_ms(),
                    upload_id: None,
                    replicas,
                };
                db.put(&meta_key, &meta)?;
            }
            Ok::<_, anyhow::Error>((true, None))
        }));
    }

    // Collect
    while let Some(res) = tasks.next().await {
        match res {
            Ok(Ok((written, conflict))) => {
                if written { report.written_metas += 1; }
                if let Some(c) = conflict {
                    report.conflicts += 1;
                    if report.sample_conflicts.len() < 20 {
                        report.sample_conflicts.push((c.0, c.1.first().unwrap().clone()));
                    }
                }
            }
            Ok(Err(e)) => {
                report.conflicts += 1;
                error!("error in task: {}", e);
            }
            Err(_join_err) => { return Err(anyhow::anyhow!("could not join task")) }
        }
    }

    Ok(report)
}

fn node_id_from_url(u: &str) -> String {
    // simple: host:port as node id; or parse URL properly
    u.trim_start_matches("http://").trim_start_matches("https://").to_string()
}
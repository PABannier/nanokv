use anyhow::{Context, Result, bail};
use clap::Parser;
use futures::stream::{self, StreamExt};
use reqwest::{Client, Url};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
    time::Duration,
};
use tracing::{error, info, warn};

use common::schemas::{BlobHead, ListResponse};
use common::time_utils::utc_now_ms;
use common::{key_utils::meta_key_for, url_utils::node_id_from_url};

use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct RebuildArgs {
    /// RocksDB directory path
    #[arg(long)]
    pub index: PathBuf,

    /// List of volume base URLs (internal or public)
    #[arg(long, value_delimiter = ',')]
    pub nodes: Vec<String>,

    /// Perform a dry run without writing metas
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Ask volumes to compute etag (deep). Slower.
    #[arg(long, default_value_t = false)]
    pub deep: bool,

    /// Max concurrent key reconciliations
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,

    /// Per-request timeout, seconds
    #[arg(long, default_value_t = 5)]
    pub http_timeout_secs: u64,
}

type Variant = (
    String,         /*volume_url*/
    u64,            /*size*/
    Option<String>, /*etag*/
);

pub struct RebuildReport {
    pub scanned_keys: usize,
    pub written_metas: usize,
    pub tombstones_preserved: usize,
    pub conflicts: usize,
    pub probe_errors: usize,
    pub sample_conflicts: Vec<(String /*key*/, Vec<Variant>)>,
}

impl Display for RebuildReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Rebuild report:")?;
        writeln!(f, "  Scanned keys:        = {}", self.scanned_keys)?;
        writeln!(f, "  Written metas        = {}", self.written_metas)?;
        writeln!(f, "  Tombstones preserved = {}", self.tombstones_preserved)?;
        writeln!(f, "  Conflicts            = {}", self.conflicts)?;
        writeln!(f, "  Probe errors         = {}", self.probe_errors)?;
        if !self.sample_conflicts.is_empty() {
            writeln!(f, "  Sample conflicts:")?;
            for (k, vars) in &self.sample_conflicts {
                writeln!(f, "    key={} variants={:?}", k, vars)?;
            }
        }
        Ok(())
    }
}

pub async fn rebuild(args: RebuildArgs) -> Result<()> {
    let db = KvDb::open(&args.index)
        .with_context(|| format!("opening RocksDB at {}", args.index.display()))?;

    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    // Parse and normalize volume URLs + derive node ids
    let mut vol_ids: HashMap<String, String> = HashMap::new(); // base_url -> node_id
    let mut volumes: Vec<Url> = Vec::with_capacity(args.nodes.len());
    for raw in &args.nodes {
        let u = Url::parse(raw).with_context(|| format!("bad node URL: {raw}"))?;
        volumes.push(u.clone());
        vol_ids.insert(u.as_str().to_string(), node_id_from_url(u.as_str()));
    }

    let present = scan(&http, &volumes, Duration::from_secs(args.http_timeout_secs))
        .await
        .context("scan volumes")?;

    let report = reconcile(
        &http,
        &db,
        &vol_ids,
        &present,
        &args,
        Duration::from_secs(args.http_timeout_secs),
    )
    .await
    .context("reconcile metas")?;

    info!("{}", report);
    Ok(())
}

/// Build map: key_enc -> set{volume_url_that_has_it}
async fn scan(
    http: &Client,
    volumes: &[Url],
    timeout: Duration,
) -> Result<HashMap<String, HashSet<String>>> {
    let mut present: HashMap<String, HashSet<String>> = HashMap::new();

    for base in volumes {
        let mut after: Option<String> = None;
        loop {
            let mut url = base.clone();
            url.set_path("/admin/list");
            {
                let mut qp = url.query_pairs_mut();
                qp.append_pair("limit", "1000");
                if let Some(a) = &after {
                    qp.append_pair("after", a);
                }
            }

            let resp = http
                .get(url.clone())
                .timeout(timeout)
                .send()
                .await
                .with_context(|| format!("GET {}", url))?;

            if !resp.status().is_success() {
                bail!("{} returned {}", url, resp.status());
            }
            let page: ListResponse = resp
                .json()
                .await
                .with_context(|| format!("decode list page from {}", base))?;
            for key_enc in page.keys {
                present
                    .entry(key_enc)
                    .or_default()
                    .insert(base.as_str().to_string());
            }
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
    Ok(present)
}

/// For every key present on any volume:
///  - probe one or all volumes for (size, etag?) depending on `deep`
///  - if all probed agree -> write Committed meta (replicas = set of node_ids that reported it)
///  - if any tombstone exists in DB -> preserve (do not resurrect)
///  - if no successful probe -> skip (do not write size=0)
///  - on disagreement -> record conflict, do not write meta
async fn reconcile(
    http: &Client,
    db: &KvDb,
    vol_ids: &HashMap<String, String>, // base_url -> node_id
    present: &HashMap<String, HashSet<String>>,
    args: &RebuildArgs,
    timeout: Duration,
) -> Result<RebuildReport> {
    let mut report = RebuildReport {
        scanned_keys: present.len(),
        written_metas: 0,
        tombstones_preserved: 0,
        conflicts: 0,
        probe_errors: 0,
        sample_conflicts: Vec::new(),
    };

    let results = stream::iter(present.iter())
        .map(|(key_enc, vols)| {
            let http = http.clone();
            let db = db.clone();
            let key_enc = key_enc.clone();
            let vols = vols.clone();
            let vol_ids = vol_ids.clone();

            async move {
                // If DB has a tombstone, preserve it
                let meta_key = meta_key_for(&key_enc);
                if let Ok(Some(existing)) = db.get::<Meta>(&meta_key)
                    && existing.state == TxState::Tombstoned
                {
                    return Ok::<(Outcome, String), anyhow::Error>((
                        Outcome::TombstonePreserved,
                        key_enc,
                    ));
                }

                // Probe volumes: in deep=false mode, stop after first success
                let mut variants: Vec<Variant> = Vec::new();
                let mut probe_errs = 0usize;

                for v in vols.iter() {
                    let mut u = Url::parse(v).context("parse volume url")?;
                    u.set_path("/admin/blob");
                    {
                        let mut qp = u.query_pairs_mut();
                        qp.append_pair("key", &key_enc);
                        qp.append_pair("deep", if args.deep { "true" } else { "false" });
                    }

                    // tiny retry (3 attempts) for transient issues
                    let mut attempt = 0;
                    let head: Option<BlobHead> = loop {
                        attempt += 1;
                        match http.get(u.clone()).timeout(timeout).send().await {
                            Ok(r) if r.status().is_success() => match r.json::<BlobHead>().await {
                                Ok(h) => break Some(h),
                                Err(e) => {
                                    warn!(%e, "bad JSON from {}", u);
                                    break None;
                                }
                            },
                            Ok(r) => {
                                if r.status().is_server_error() && attempt < 3 {
                                    continue;
                                }
                                warn!(status=%r.status(), url=%u, "head non-success");
                                break None;
                            }
                            Err(e) => {
                                if (e.is_connect() || e.is_timeout() || e.is_request())
                                    && attempt < 3
                                {
                                    continue;
                                }
                                warn!(%e, url=%u, "head transport error");
                                break None;
                            }
                        }
                    };

                    match head {
                        Some(h) if h.exists => {
                            variants.push((v.clone(), h.size, h.etag));
                            if !args.deep {
                                break;
                            } // fast path: first good size is enough
                        }
                        Some(_) => { /* exists=false: ignore */ }
                        None => {
                            probe_errs += 1;
                        }
                    }
                }

                if variants.is_empty() {
                    // nothing trustworthy -> skip writing, but count probe errors upstream
                    return Ok((Outcome::NoData { probe_errs }, key_enc));
                }

                // Decide consensus
                let all_equal = variants
                    .windows(2)
                    .all(|w| w[0].1 == w[1].1 && w[0].2 == w[1].2);

                if !all_equal {
                    return Ok((Outcome::Conflict { variants }, key_enc));
                }

                // Consensus -> write/overwrite meta
                let (size, etag_opt) = {
                    let first = &variants[0];
                    (first.1, first.2.clone())
                };

                // prefer existing etag if present
                let etag_hex = match db.get::<Meta>(&meta_key).ok().flatten() {
                    Some(m) if !m.etag_hex.is_empty() => m.etag_hex,
                    _ => etag_opt.unwrap_or_default(),
                };

                let replicas: Vec<String> = variants
                    .iter()
                    .filter_map(|(vurl, _, _)| vol_ids.get(vurl).cloned())
                    .collect();

                if !args.dry_run {
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

                Ok((Outcome::Written, key_enc))
            }
        })
        .buffer_unordered(args.concurrency)
        .collect::<Vec<_>>()
        .await;

    // Collect
    for r in results {
        match r {
            Ok((Outcome::Written, _)) => report.written_metas += 1,
            Ok((Outcome::TombstonePreserved, _)) => report.tombstones_preserved += 1,
            Ok((Outcome::Conflict { variants }, key)) => {
                report.conflicts += 1;
                if report.sample_conflicts.len() < 20 {
                    report
                        .sample_conflicts
                        .push((key, variants.into_iter().take(4).collect()));
                }
            }
            Ok((Outcome::NoData { probe_errs }, _)) => report.probe_errors += probe_errs,
            Err(e) => {
                report.conflicts += 1;
                error!(error=%e, "reconcile task error");
            }
        }
    }

    Ok(report)
}

#[derive(Debug)]
enum Outcome {
    Written,
    TombstonePreserved,
    Conflict { variants: Vec<Variant> },
    NoData { probe_errs: usize },
}

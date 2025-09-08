use anyhow::{Result, bail, Context};
use reqwest::Client;
use url::Url;
use uuid::Uuid;

use common::constants::NODE_KEY_PREFIX;
use common::schemas::BlobHead;
use common::time_utils::utc_now_ms;
use common::url_utils::node_id_from_url;

use crate::core::meta::KvDb;
use crate::core::node::{NodeInfo, NodeStatus};
use crate::core::op::{commit::retry_commit, prepare::retry_prepare, pull::retry_pull};

pub fn nodes_from_db(db: &KvDb, include_suspect: bool) -> Result<Vec<NodeInfo>> {
    let mut nodes = Vec::new();
    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(NODE_KEY_PREFIX.as_bytes()) {
            continue;
        }
        let node_info: NodeInfo = serde_json::from_slice(&v)?;
        match node_info.status {
            NodeStatus::Alive => nodes.push(node_info),
            NodeStatus::Suspect => {
                if include_suspect {
                    nodes.push(node_info)
                }
            }
            NodeStatus::Down => (),
        }
    }
    Ok(nodes)
}

pub fn nodes_from_explicit(volume_urls: &[String]) -> Vec<NodeInfo> {
    let mut nodes = Vec::new();
    for volume_url in volume_urls {
        nodes.push(NodeInfo {
            node_id: node_id_from_url(volume_url),
            public_url: volume_url.clone(),
            internal_url: volume_url.clone(),
            subvols: 1,
            capacity_bytes: None,
            used_bytes: None,
            version: None,
            last_heartbeat_ms: utc_now_ms(),
            status: NodeStatus::Alive,
        });
    }
    nodes
}

pub async fn probe_exists(http: &Client, node: &NodeInfo, key_enc: &str) -> Result<bool> {
    let url = format!("{}/admin/blob?key={}", node.internal_url, key_enc);
    let r = http.get(&url).send().await?.error_for_status()?;
    let h: BlobHead = r.json().await?;
    Ok(h.exists)
}

pub async fn probe_matches(
    http: &Client,
    node: &NodeInfo,
    key_enc: &str,
    etag: &str,
    size: u64,
) -> Result<bool> {
    let mut url = Url::parse(&node.internal_url)
        .with_context(|| format!("bad URL for node {}: {}", node.node_id, node.internal_url))?;
    url.set_path("/admin/blob");
    url.query_pairs_mut()
        .append_pair("key", key_enc)
        .append_pair("deep", "true");

    let r = http.get(url.clone()).send().await?.error_for_status()?;
    let h: BlobHead = r.json().await?;
    Ok(h.exists && h.size == size && (etag.is_empty() || h.etag.as_deref() == Some(etag)))
}

pub async fn copy_one(
    http: &Client,
    src: &NodeInfo,
    dst: &NodeInfo,
    key_enc: &str,
    size: u64,
    etag: &str,
    prefix: &str,
) -> Result<()> {
    let upload_id = format!("{}-{}", prefix, Uuid::new_v4());

    retry_prepare(http, dst, key_enc, &upload_id).await?;

    let r = retry_pull(http, src, dst, &upload_id, size, etag).await?;
    if r.etag != etag || r.size != size {
        bail!("size/checksum mismatch on dst {}", dst.node_id);
    }

    retry_commit(http, dst, &upload_id, key_enc).await?;

    Ok(())
}

use std::io::Read;
use std::collections::HashMap;
use std::path::Path;
use walkdir::WalkDir;
use tracing::info;

use crate::kvdb::KvDb;
use crate::constants::{META_KEY_PREFIX, BLOB_DIR_NAME};
use crate::file_utils::blob_path;
use crate::meta::{TxState, Meta};

#[derive(Default)]
pub struct VerifyReport {
    blobs_total: usize,
    blobs_unindexed: usize,
    blobs_size_mismatch: usize,
    blobs_hash_mismatch: usize,
    metas_total: usize,
    metas_missing_files: usize,
}

impl VerifyReport {
    pub fn has_issues(&self) -> bool {
        self.blobs_unindexed > 0 || self.blobs_size_mismatch > 0 || self.blobs_hash_mismatch > 0 || self.metas_missing_files > 0
    }

    pub fn print(&self) {
        info!("Verify Report:");
        info!("  Total blobs: {}", self.blobs_total);
        info!("  Unindexed blobs: {}", self.blobs_unindexed);
        info!("  Size mismatch: {}", self.blobs_size_mismatch);
        info!("  Hash mismatch: {}", self.blobs_hash_mismatch);
        info!("  Total metas: {}", self.metas_total);
        info!("  Missing files: {}", self.metas_missing_files);
        if self.has_issues() { info!("STATUS: ISSUES FOUND"); } else { info!("STATUS: OK"); }
    }
}

pub async fn verify(data_root: &Path, db: &KvDb, deep: bool) -> anyhow::Result<VerifyReport> {
    let mut report = VerifyReport::default();

    // Build a map of committed metas for quick lookup
    let mut committed: HashMap<String, Meta> = HashMap::new();
    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let kstr = std::str::from_utf8(&k)?.to_string();
        let user_key_enc = kstr.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta: Meta = serde_json::from_slice(&v)?;
        if matches!(meta.state, TxState::Committed) {
            committed.insert(user_key_enc, meta);
            report.metas_total += 1;
        }
    }

    // Scan all blobs
    let blobs_root = data_root.join(BLOB_DIR_NAME);
    for entry in WalkDir::new(&blobs_root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() { continue; }
        // filename is the percent-encoded key
        let key_enc = path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
        if key_enc.is_empty() { continue; }
        report.blobs_total += 1;

        match committed.get(&key_enc) {
            None => {
                report.blobs_unindexed += 1;
            }
            Some(meta) => {
                // size check
                let size = std::fs::metadata(path)?.len();
                if size != meta.size {
                    report.blobs_size_mismatch += 1;
                } else if deep {
                    // deep hash
                    let mut f = std::fs::File::open(path)?;
                    let mut hasher = blake3::Hasher::new();
                    let mut buf = vec![0u8; 1024 * 1024];
                    loop {
                        let n = f.read(&mut buf)?;
                        if n == 0 { break; }
                        hasher.update(&buf[..n]);
                    }
                    let h = hasher.finalize().to_hex().to_string();
                    if h != meta.etag_hex {
                        report.blobs_hash_mismatch += 1;
                    }
                }
            }
        }
    }

    // Detect metas whose file is missing
    for (key_enc, _meta) in committed.iter() {
        let fpath = blob_path(data_root, key_enc);
        if !fpath.exists() {
            report.metas_missing_files += 1;
        }
    }

    Ok(report)
}
use rocksdb::{DB, IteratorMode, Options, ReadOptions, WriteOptions};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt::Display, path::Path, sync::Arc};

use common::time_utils::utc_now_ms;

const MAX_OPEN_FILES: i32 = 512;

#[derive(Clone)]
pub struct KvDb {
    inner: Arc<DB>,
}

impl KvDb {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_max_open_files(MAX_OPEN_FILES);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

        // Performance optimizations
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB SST files
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(8);
        opts.set_level_zero_stop_writes_trigger(12);

        // Optimize for write-heavy workload
        opts.set_max_background_jobs(4);
        opts.set_bytes_per_sync(1024 * 1024); // 1MB

        let db = DB::open(&opts, path)?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        let v = self.inner.get(key.as_bytes())?;
        if let Some(raw) = v {
            let t = serde_json::from_slice::<T>(&raw)?;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    // Optimized put for pending metadata (can skip WAL for performance)
    pub fn put_pending<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(value)?;
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true); // Skip WAL for pending writes (faster)
        self.inner.put_opt(key.as_bytes(), buf, &write_opts)?;
        Ok(())
    }

    // Standard put for committed metadata (with WAL for durability)
    pub fn put<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(value)?;
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true); // Force sync for committed writes
        self.inner.put_opt(key.as_bytes(), buf, &write_opts)?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key.as_bytes())?;
        Ok(())
    }

    pub fn iter(&self) -> rocksdb::DBIterator<'_> {
        let readopts = ReadOptions::default();
        self.inner.iterator_opt(IteratorMode::Start, readopts)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxState {
    Pending,    // upload started, not committed
    Committed,  // fully durable and visible
    Tombstoned, // deleted (logical)
}

impl Display for TxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxState::Pending => write!(f, "Pending"),
            TxState::Committed => write!(f, "Committed"),
            TxState::Tombstoned => write!(f, "Tombstoned"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub state: TxState,
    pub size: u64,
    pub etag_hex: String,          // hex blake3
    pub created_ms: i128,          // epoch ms
    pub upload_id: Option<String>, // present during Pending
    pub replicas: Vec<String>,     // node_ids
}

impl Meta {
    pub fn pending(upload_id: String, replicas: Vec<String>) -> Self {
        Self {
            state: TxState::Pending,
            size: 0,
            etag_hex: String::new(),
            created_ms: utc_now_ms(),
            upload_id: Some(upload_id),
            replicas,
        }
    }

    pub fn committed(size: u64, etag_hex: String, replicas: Vec<String>) -> Self {
        Self {
            state: TxState::Committed,
            size,
            etag_hex,
            created_ms: utc_now_ms(),
            upload_id: None,
            replicas,
        }
    }

    pub fn tombstoned() -> Self {
        Self {
            state: TxState::Tombstoned,
            size: 0,
            etag_hex: String::new(),
            created_ms: utc_now_ms(),
            upload_id: None,
            replicas: vec![],
        }
    }
}

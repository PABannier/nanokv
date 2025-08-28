use std::{path::Path, sync::Arc};
use rocksdb::{Options, DB};
use serde::{de::DeserializeOwned, Serialize};

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
        let db = DB::open(&opts, path)?;
        Ok(Self { inner: Arc::new(db) })
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

    pub fn put<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(value)?;
        self.inner.put(key.as_bytes(), buf)?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key.as_bytes())?;
        Ok(())
    }

    pub fn exists(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.inner.get_pinned(key.as_bytes())?.is_some())
    }
}
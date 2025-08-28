use serde::{Serialize, Deserialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxState { 
    Pending,     // upload started, not committed
    Committed,   // fully durable and visible
    Tombstoned   // deleted (logical)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub state: TxState,
    pub size: u64,
    pub etag_hex: String,  // hex blake3
    pub created_ms: i128,  // epoch ms
    pub upload_id: Option<String>,  // present during Pending 
}

impl Meta {
    pub fn pending(upload_id: String) -> Self {
        Self {
            state: TxState::Pending,
            size: 0,
            etag_hex: String::new(),
            created_ms: OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
            upload_id: Some(upload_id),
        }
    }

    pub fn committed(size: u64, etag_hex: String) -> Self {
        Self {
            state: TxState::Committed,
            size: size,
            etag_hex: etag_hex,
            created_ms: OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
            upload_id: None,
        }
    }

    pub fn tombstoned() -> Self {
        Self {
            state: TxState::Tombstoned,
            size: 0,
            etag_hex: String::new(),
            created_ms: OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
            upload_id: None,
        }
    }
}

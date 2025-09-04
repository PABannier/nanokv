use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub public_url: String,
    pub internal_url: String,
    pub subvols: u16,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub version: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub used_bytes: Option<u64>,
    pub capacity_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct PutResponse { pub etag: String, pub size: u64 }

#[derive(Serialize, Deserialize)]
pub struct ListResponse {
    pub keys: Vec<String>,  // percent-encoded keys
    pub next_after: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct BlobHead { pub exists: bool, pub size: u64, pub etag: Option<String> }
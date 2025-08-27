struct Meta {
    size: u64,
    etag: [u8; 32],   // blake3 hash bytes
    created_at: i64,  // epoch ms
    state: State,
}

enum State { Pending, Committed, Tombstoned }
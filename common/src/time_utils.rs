use time::OffsetDateTime;

pub fn utc_now_ms() -> i128 {
    OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000
}

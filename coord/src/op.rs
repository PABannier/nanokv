use anyhow::anyhow;

use crate::node::NodeInfo;
use crate::state::CoordinatorState;

use common::api_error::ApiError;


pub mod guard {
    use futures_util::future::join_all;

    use crate::op::send_abort_request;
    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;

    pub struct AbortGuard<'a> {
        ctx: &'a CoordinatorState,
        replicas: Vec<NodeInfo>,
        upload_id: String,
        armed: bool,
    }

    impl<'a> AbortGuard<'a> {
        pub fn new(ctx: &'a CoordinatorState, replicas: &[NodeInfo], upload_id: String) -> Self {
            Self {
                ctx,
                replicas: replicas.to_vec(),
                upload_id,
                armed: true,
            }
        }

        pub fn disarm(&mut self) { self.armed = false; }
    }

    impl Drop for AbortGuard<'_> {
        fn drop(&mut self) {
            if self.armed {
                let ctx = self.ctx.clone();
                let replicas = self.replicas.clone();
                let upload_id = self.upload_id.clone();
                tokio::spawn(async move {
                    let futs = replicas.iter()
                        .map(|r| send_abort_request(&ctx, r, &upload_id));
                    let _ = join_all(futs).await;
                });
            }
        }
    }
}

pub mod meta {
    use uuid::Uuid;

    use common::api_error::ApiError;

    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;
    use crate::meta::{Meta};

    pub fn write_pending_meta(ctx: &CoordinatorState, meta_key: &str, replicas: &Vec<NodeInfo>) -> Result<String, ApiError> {
        let upload_id = Uuid::new_v4().to_string();

        let replica_ids = replicas
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<Vec<_>>();

        let meta = Meta::pending(upload_id.clone(), replica_ids);

        ctx.db.put(meta_key, &meta)?;

        Ok(upload_id)
    }

    pub fn write_committed_meta(
        ctx: &CoordinatorState,
        meta_key: &str,
        size: u64,
        etag_hex: String,
        replicas: &Vec<NodeInfo>
    ) -> Result<(), ApiError> {
        let replica_ids = replicas.iter().map(|r| r.node_id.clone()).collect::<Vec<_>>();
        let meta = Meta::committed(size, etag_hex, replica_ids);
        ctx.db.put(meta_key, &meta)?;
        Ok(())
    }
}

pub mod prepare {
    use common::api_error::ApiError;
    use super::retry::{retry_timeboxed, RetryClass, classify_reqwest};

    use futures_util::future::try_join_all;

    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;
    use crate::op::retry::RetryConfig;

    pub async fn retry_prepare_all(ctx: &CoordinatorState, replicas: &[NodeInfo], key: &str, upload_id: &str) -> Result<(), ApiError> {
        let cfg = RetryConfig::default();

        try_join_all(
            replicas
                .iter()
                .map(|r| retry_prepare(&ctx, r, &key, &upload_id, &cfg))
        ).await.map_err(|e| ApiError::Any(e.into()))?;

        Ok(())
    }

    async fn retry_prepare(ctx: &CoordinatorState, replica: &NodeInfo, key: &str, upload_id: &str, cfg: &RetryConfig) -> Result<(), ApiError> {
        retry_timeboxed(&cfg, || {
            send_prepare_request(ctx, replica, key, upload_id, &cfg)
        }, |e| match e {
            ApiError::UpstreamStatus(st) => {
                if st.is_server_error() || *st == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    RetryClass::Retryable
                } else { RetryClass::NonRetryable }
            }
            ApiError::UpstreamReq(err) => classify_reqwest(None, err),
            _ => RetryClass::NonRetryable,
        }).await?;

        Ok(())
    }

    async fn send_prepare_request(
        ctx: &CoordinatorState,
        replica: &NodeInfo,
        key: &str,
        upload_id: &str,
        cfg: &RetryConfig
    ) -> Result<(), ApiError> {
        let vol_url = format!("{}/internal/prepare", replica.internal_url);

        let req = ctx.http_client
            .post(&vol_url)
            .query(&[("key", key)])
            .query(&[("upload_id", upload_id)])
            .timeout(cfg.per_attempt_timeout)
            .build()
            .unwrap();

        let resp = ctx.http_client.execute(req).await.map_err(ApiError::UpstreamReq)?;
        let st = resp.status();
        if st.is_success() { Ok(())} else {
            Err(ApiError::UpstreamStatus(st))
        }
    }
}

pub mod write {
    use axum::body::Body;
    use anyhow::anyhow;
    use reqwest;
    use futures_util::TryStreamExt;

    use common::api_error::ApiError;
    use common::schemas::PutResponse;

    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;

    pub async fn write_to_head_single_shot(
        ctx: &CoordinatorState,
        head: &NodeInfo,
        body: Body,
        upload_id: &str
    ) -> Result<(u64, String), ApiError> {
        let stream = body
            .into_data_stream()
            .map_err(|e| ApiError::Any(anyhow!("failed to stream to node: {}", e)));

        let upstream_body = reqwest::Body::wrap_stream(stream);

        let volume_url = format!("{}/internal/write/{}", head.internal_url, upload_id);
        let req = ctx.http_client.put(&volume_url);

        let resp = req
            .body(upstream_body)
            .send()
            .await
            .map_err(|e| ApiError::Any(anyhow!("failed to stream to node: {}", e)))?;

        if !resp.status().is_success() {
            return Err(ApiError::Any(anyhow!("failed to stream to node. Volume replied: {}", resp.status())));
        }

        let put_resp: PutResponse = resp.json::<PutResponse>().await
            .map_err(|e| ApiError::Any(anyhow!("failed to parse volume put response: {}", e)))?;

        Ok((put_resp.size, put_resp.etag))
    }
}

pub mod pull {
    use common::api_error::ApiError;

    use futures_util::future::try_join_all;

    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;
    use crate::op::retry::{retry_timeboxed, RetryClass, classify_reqwest};
    use crate::op::retry::RetryConfig;

    pub async fn retry_pull_all(
        ctx: &CoordinatorState,
        head: &NodeInfo,
        followers: &[NodeInfo],
        upload_id: &str,
        expected_size: u64,
        expected_etag: &str,
    ) -> Result<(), ApiError> {
        let cfg = RetryConfig::default();

        try_join_all(
            followers
                .iter()
                .map(|f| retry_pull(ctx, head, f, upload_id, expected_size, expected_etag, &cfg))
        ).await.map_err(|e| ApiError::Any(e.into()))?;

        Ok(())
    }

    async fn retry_pull(
        ctx: &CoordinatorState,
        head: &NodeInfo,
        follower: &NodeInfo,
        upload_id: &str,
        expected_size: u64,
        expected_etag: &str,
        cfg: &RetryConfig
    ) -> Result<(), ApiError> {
        retry_timeboxed(&cfg, || {
            pull_from_head(ctx, head, follower, upload_id, expected_size, expected_etag)
        }, |e| match e {
            ApiError::UpstreamStatus(st) => {
                if st.is_server_error() || *st == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    RetryClass::Retryable
                } else { RetryClass::NonRetryable }
            }
            ApiError::UpstreamReq(err) => classify_reqwest(None, err),
            _ => RetryClass::NonRetryable,
        }).await?;

        Ok(())
    }

    async fn pull_from_head(
        ctx: &CoordinatorState,
        head: &NodeInfo,
        follower: &NodeInfo,
        upload_id: &str,
        expected_size: u64,
        expected_etag: &str,
    ) -> Result<(), ApiError> {
        let req_url = format!("{}/internal/pull", follower.internal_url);
        let from_url = format!("{}/internal/read/{}", head.internal_url, upload_id);

        let req = ctx.http_client
            .post(req_url)
            .query(&[("upload_id", upload_id)])
            .query(&[("from", from_url)])
            .query(&[("expected_size", expected_size.to_string())])
            .query(&[("expected_etag", expected_etag)]);

        let res = req.send().await.map_err(ApiError::UpstreamReq)?;
        let st = res.status();

        if st.is_success() { Ok(()) } else {
            Err(ApiError::UpstreamStatus(st))
        }
    }

}

pub mod commit {
    use common::api_error::ApiError;
    use super::retry::{retry_timeboxed, RetryClass, classify_reqwest};
    use super::retry::RetryConfig;

    use futures_util::future::try_join_all;

    use crate::state::CoordinatorState;
    use crate::node::NodeInfo;

    pub async fn retry_commit_all(
        ctx: &CoordinatorState,
        replicas: &[NodeInfo],
        upload_id: &str,
        key: &str
    ) -> Result<(), ApiError> {
        let cfg = RetryConfig::default();

        try_join_all(
            replicas
                .iter()
                .map(|r| retry_commit(ctx, r, upload_id, key, &cfg))
        ).await.map_err(|e| ApiError::Any(e.into()))?;

        Ok(())
    }

    async fn retry_commit(
        ctx: &CoordinatorState,
        replica: &NodeInfo,
        upload_id: &str,
        key: &str,
        cfg: &RetryConfig
    ) -> Result<(), ApiError> {
        retry_timeboxed(&cfg, || {
            send_commit_request(ctx, replica, upload_id, key)
        }, |e| match e {
            ApiError::UpstreamStatus(st) => {
                if st.is_server_error() || *st == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    RetryClass::Retryable
                } else { RetryClass::NonRetryable }
            }
            ApiError::UpstreamReq(err) => classify_reqwest(None, err),
            _ => RetryClass::NonRetryable,
        }).await?;

        Ok(())
    }

    async fn send_commit_request(
        ctx: &CoordinatorState,
        node: &NodeInfo,
        upload_id: &str,
        key: &str
    ) -> Result<(), ApiError> {
        let req_url = format!("{}/internal/commit", node.internal_url);

        let req = ctx.http_client
            .post(req_url)
            .query(&[("upload_id", upload_id)])
            .query(&[("key", key)]);

        let res = req.send().await.map_err(|e| ApiError::UpstreamReq(e))?;
        let st = res.status();

        if st.is_success() { Ok(()) } else {
            return Err(ApiError::UpstreamStatus(res.status()));
        }
    }

}

async fn send_abort_request(
    ctx: &CoordinatorState,
    node: &NodeInfo,
    upload_id: &str
) -> Result<(), ApiError> {
    let req_url = format!("{}/internal/abort", node.internal_url);

    let req = ctx.http_client
        .post(req_url)
        .query(&[("upload_id", upload_id)]);

    let res = req
        .send()
        .await
        .map_err(|e| ApiError::Any(anyhow!("failed to send request to node: {}", e)))?;

    if !res.status().is_success() {
        return Err(ApiError::Any(anyhow!("failed to send request to node. Node replied: {}", res.status())));
    }

    Ok(())
}

mod retry {
    use std::future::Future;
    use tokio::time::{sleep, Instant, Duration};
    use rand::random_range;

    const DEFAULT_TOTAL_BUDGET: Duration = Duration::from_secs(60);
    const DEFAULT_PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
    const DEFAULT_BACKOFF_BASE: Duration = Duration::from_secs(1);
    const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(30);
    const DEFAULT_JITTER_FRAC: f32 = 0.5;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RetryClass {
        Retryable,     // transient errors, 5xx, network errors, etc.
        NonRetryable,  // 4xx, checksum mismatch, logic errors
    }

    pub struct RetryConfig {
        pub total_budget: Duration,
        pub per_attempt_timeout: Duration,
        pub backoff_base: Duration,
        pub backoff_max: Duration,
        pub jitter_frac: f32,
    }

    impl Default for RetryConfig {
        fn default() -> Self {
            Self {
                total_budget: DEFAULT_TOTAL_BUDGET,
                per_attempt_timeout: DEFAULT_PER_ATTEMPT_TIMEOUT,
                backoff_base: DEFAULT_BACKOFF_BASE,
                backoff_max: DEFAULT_BACKOFF_MAX,
                jitter_frac: DEFAULT_JITTER_FRAC,
            }
        }
    }

    fn jitter(d: Duration, frac: f32) -> Duration {
        let ms = d.as_millis() as i64;
        let delta = (ms as f32 * frac) as i64;
        let j = random_range(-delta..=delta);
        Duration::from_millis((ms + j).max(0) as u64)
    }

    pub async fn retry_timeboxed<E, F, Fut, C>(
        cfg: &RetryConfig,
        mut op: F,
        classify: C,
    ) -> Result<(), E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
        C: Fn(&E) -> RetryClass,
    {
        let deadline = Instant::now() + cfg.total_budget;
        let mut backoff = cfg.backoff_base;

        loop {
            // Run the op (should already include a per-attempt timeout)
            match op().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if classify(&e) == RetryClass::NonRetryable {
                        return Err(e);
                    }
                    // Check deadline
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(e);
                    }
                    // Sleep with jitter but not beyond deadline
                    let sleep_dur = jitter(backoff.min(cfg.backoff_max), cfg.jitter_frac);
                    let remaining = deadline.saturating_duration_since(now);
                    if sleep_dur > remaining {
                        return Err(e);
                    }
                    sleep(sleep_dur).await;
                    // Exponential increase
                    backoff = (backoff * 2).min(cfg.backoff_max);
                }
            }
        }
    }

    pub fn classify_reqwest(resp_status: Option<reqwest::StatusCode>, err: &reqwest::Error) -> RetryClass {
        if err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() {
            return RetryClass::Retryable;
        }
        if let Some(st) = resp_status {
            if st.is_server_error() || st == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return RetryClass::Retryable;
            }
            // 409 Conflict, 400, 404, 403 are logic or permanent in our flow
            return RetryClass::NonRetryable;
        }
        // No status (transport error): was handled above
        RetryClass::Retryable
    }
}

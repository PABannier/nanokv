use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::{
    extract::{Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;

use crate::state::VolumeState;
use common::api_error::ApiError;

/// Fault injection state for testing
#[derive(Debug, Default)]
pub struct FaultInjector {
    // Prepare failures
    pub fail_prepare_once: AtomicBool,
    pub fail_prepare_always: AtomicBool,

    // Pull failures
    pub fail_pull_once: AtomicBool,
    pub fail_pull_always: AtomicBool,
    pub fail_pull_mid_stream_once: AtomicBool,

    // Commit failures
    pub fail_commit_once: AtomicBool,
    pub fail_commit_always: AtomicBool,
    pub fail_commit_timeout_once: AtomicBool,

    // Read temp failures
    pub fail_read_tmp_once: AtomicBool,
    pub fail_read_tmp_always: AtomicBool,

    // Etag mismatch
    pub fail_etag_mismatch_once: AtomicBool,
    pub fail_etag_mismatch_always: AtomicBool,

    // Latency injection
    pub inject_latency_ms: AtomicU64,

    // Server pause/kill
    pub is_paused: AtomicBool,
    pub is_killed: AtomicBool,
}

impl FaultInjector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if we should fail prepare operations
    pub fn should_fail_prepare(&self) -> bool {
        if self.fail_prepare_always.load(Ordering::Relaxed) {
            return true;
        }

        if self.fail_prepare_once.load(Ordering::Relaxed) {
            self.fail_prepare_once.store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should fail pull operations
    pub fn should_fail_pull(&self) -> bool {
        if self.fail_pull_always.load(Ordering::Relaxed) {
            return true;
        }

        if self.fail_pull_once.load(Ordering::Relaxed) {
            self.fail_pull_once.store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should fail pull mid-stream
    pub fn should_fail_pull_mid_stream(&self) -> bool {
        if self.fail_pull_mid_stream_once.load(Ordering::Relaxed) {
            self.fail_pull_mid_stream_once
                .store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should fail commit operations
    pub fn should_fail_commit(&self) -> bool {
        if self.fail_commit_always.load(Ordering::Relaxed) {
            return true;
        }

        if self.fail_commit_once.load(Ordering::Relaxed) {
            self.fail_commit_once.store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should timeout commit operations
    pub fn should_timeout_commit(&self) -> bool {
        if self.fail_commit_timeout_once.load(Ordering::Relaxed) {
            self.fail_commit_timeout_once
                .store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should fail read temp operations
    pub fn should_fail_read_tmp(&self) -> bool {
        if self.fail_read_tmp_always.load(Ordering::Relaxed) {
            return true;
        }

        if self.fail_read_tmp_once.load(Ordering::Relaxed) {
            self.fail_read_tmp_once.store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Check if we should return wrong etag
    pub fn should_fail_etag_mismatch(&self) -> bool {
        if self.fail_etag_mismatch_always.load(Ordering::Relaxed) {
            return true;
        }

        if self.fail_etag_mismatch_once.load(Ordering::Relaxed) {
            self.fail_etag_mismatch_once.store(false, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Get injected latency in milliseconds
    pub fn get_latency_ms(&self) -> u64 {
        self.inject_latency_ms.load(Ordering::Relaxed)
    }

    /// Check if server is paused
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }

    /// Check if server is killed
    pub fn is_killed(&self) -> bool {
        self.is_killed.load(Ordering::Relaxed)
    }

    /// Apply latency if configured
    pub async fn apply_latency(&self) {
        let latency_ms = self.get_latency_ms();
        if latency_ms > 0 {
            sleep(Duration::from_millis(latency_ms)).await;
        }
    }

    /// Wait while paused
    pub async fn wait_if_paused(&self) {
        while self.is_paused() && !self.is_killed() {
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Check if killed and return error
    pub fn check_killed(&self) -> Result<(), ApiError> {
        if self.is_killed() {
            return Err(ApiError::Any(anyhow::anyhow!("Server is killed")));
        }
        Ok(())
    }

    /// Reset all fault injection flags
    pub fn reset(&self) {
        self.fail_prepare_once.store(false, Ordering::Relaxed);
        self.fail_prepare_always.store(false, Ordering::Relaxed);
        self.fail_pull_once.store(false, Ordering::Relaxed);
        self.fail_pull_always.store(false, Ordering::Relaxed);
        self.fail_pull_mid_stream_once
            .store(false, Ordering::Relaxed);
        self.fail_commit_once.store(false, Ordering::Relaxed);
        self.fail_commit_always.store(false, Ordering::Relaxed);
        self.fail_commit_timeout_once
            .store(false, Ordering::Relaxed);
        self.fail_read_tmp_once.store(false, Ordering::Relaxed);
        self.fail_read_tmp_always.store(false, Ordering::Relaxed);
        self.fail_etag_mismatch_once.store(false, Ordering::Relaxed);
        self.fail_etag_mismatch_always
            .store(false, Ordering::Relaxed);
        self.inject_latency_ms.store(0, Ordering::Relaxed);
        self.is_paused.store(false, Ordering::Relaxed);
        self.is_killed.store(false, Ordering::Relaxed);
    }
}

#[derive(Deserialize)]
pub struct FaultQuery {
    #[serde(default)]
    pub once: bool,
    #[serde(default)]
    pub always: bool,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

/// POST /admin/fail/prepare?once=true -> next prepare returns 500.
#[cfg(test)]
pub async fn fail_prepare(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if params.once {
        ctx.fault_injector
            .fail_prepare_once
            .store(true, Ordering::Relaxed);
    }
    if params.always {
        ctx.fault_injector
            .fail_prepare_always
            .store(true, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/fail/pull?once=true -> next pull returns 500 mid-stream.
#[cfg(test)]
pub async fn fail_pull(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if params.once {
        ctx.fault_injector
            .fail_pull_once
            .store(true, Ordering::Relaxed);
        // Also set mid-stream failure for more realistic testing
        ctx.fault_injector
            .fail_pull_mid_stream_once
            .store(true, Ordering::Relaxed);
    }
    if params.always {
        ctx.fault_injector
            .fail_pull_always
            .store(true, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/fail/commit?once=true -> next commit returns 500 or times out.
#[cfg(test)]
pub async fn fail_commit(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if params.once {
        // Randomly choose between failure and timeout for more variety
        if rand::random::<bool>() {
            ctx.fault_injector
                .fail_commit_once
                .store(true, Ordering::Relaxed);
        } else {
            ctx.fault_injector
                .fail_commit_timeout_once
                .store(true, Ordering::Relaxed);
        }
    }
    if params.always {
        ctx.fault_injector
            .fail_commit_always
            .store(true, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/fail/read_tmp?once=true -> next read_tmp returns 500.
#[cfg(test)]
pub async fn fail_read_tmp(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if params.once {
        ctx.fault_injector
            .fail_read_tmp_once
            .store(true, Ordering::Relaxed);
    }
    if params.always {
        ctx.fault_injector
            .fail_read_tmp_always
            .store(true, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/fail/etag_mismatch?once=true -> next pull reports wrong etag.
#[cfg(test)]
pub async fn fail_etag_mismatch(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if params.once {
        ctx.fault_injector
            .fail_etag_mismatch_once
            .store(true, Ordering::Relaxed);
    }
    if params.always {
        ctx.fault_injector
            .fail_etag_mismatch_always
            .store(true, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/inject/latency?latency_ms=1000 -> inject latency into all operations
#[cfg(test)]
pub async fn inject_latency(
    Query(params): Query<FaultQuery>,
    State(ctx): State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    if let Some(latency_ms) = params.latency_ms {
        ctx.fault_injector
            .inject_latency_ms
            .store(latency_ms, Ordering::Relaxed);
    }
    Ok(StatusCode::OK)
}

/// POST /admin/pause -> pause all volume operations
#[cfg(test)]
pub async fn pause_server(State(ctx): State<VolumeState>) -> Result<StatusCode, ApiError> {
    ctx.fault_injector.is_paused.store(true, Ordering::Relaxed);
    Ok(StatusCode::OK)
}

/// POST /admin/resume -> resume all volume operations
#[cfg(test)]
pub async fn resume_server(State(ctx): State<VolumeState>) -> Result<StatusCode, ApiError> {
    ctx.fault_injector.is_paused.store(false, Ordering::Relaxed);
    Ok(StatusCode::OK)
}

/// POST /admin/kill -> kill the volume server (simulate crash)
#[cfg(test)]
pub async fn kill_server(State(ctx): State<VolumeState>) -> Result<StatusCode, ApiError> {
    ctx.fault_injector.is_killed.store(true, Ordering::Relaxed);
    Ok(StatusCode::OK)
}

/// POST /admin/reset -> reset all fault injection flags
#[cfg(test)]
pub async fn reset_faults(State(ctx): State<VolumeState>) -> Result<StatusCode, ApiError> {
    ctx.fault_injector.reset();
    Ok(StatusCode::OK)
}

// Non-test stubs that return errors
#[cfg(not(test))]
pub async fn fail_prepare(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn fail_pull(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn fail_commit(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn fail_read_tmp(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn fail_etag_mismatch(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn inject_latency(
    _: Query<FaultQuery>,
    _: State<VolumeState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn pause_server(_: State<VolumeState>) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn resume_server(_: State<VolumeState>) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn kill_server(_: State<VolumeState>) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

#[cfg(not(test))]
pub async fn reset_faults(_: State<VolumeState>) -> Result<StatusCode, ApiError> {
    Err(ApiError::Any(anyhow::anyhow!(
        "Fault injection only available in test builds"
    )))
}

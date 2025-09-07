use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use reqwest;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("conflict: key already exists")]
    KeyAlreadyExists,
    #[error("conflict: tmp directory already exists")]
    TmpDirExists,
    #[error("not found: tmp directory not found")]
    TmpDirNotFound,
    #[error("not found")]
    KeyNotFound,
    #[error("not enough nodes")]
    NoQuorum,
    #[error("no replicas available")]
    NoReplicasAvailable,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    // #[error("not enough storage on disk")]
    // InsufficientStorage,
    #[error("payload too large")]
    TooLarge,
    #[error("unknown node")]
    UnknownNode,
    #[error("upstream status")]
    UpstreamReq(reqwest::Error),
    #[error("upstream status")]
    UpstreamStatus(reqwest::StatusCode),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status_code = match self {
            ApiError::KeyAlreadyExists => StatusCode::CONFLICT,
            ApiError::TmpDirExists => StatusCode::CONFLICT,
            ApiError::KeyNotFound => StatusCode::NOT_FOUND,
            ApiError::TmpDirNotFound => StatusCode::NOT_FOUND,
            ApiError::NoQuorum => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::NoReplicasAvailable => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::ChecksumMismatch => StatusCode::UNPROCESSABLE_ENTITY,
            // ApiError::InsufficientStorage => StatusCode::INSUFFICIENT_STORAGE,
            ApiError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::UnknownNode => StatusCode::NOT_FOUND,
            ApiError::UpstreamReq(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::UpstreamStatus(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Any(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status_code, self.to_string()).into_response()
    }
}

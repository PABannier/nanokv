use std::io;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("conflict: key already exists")]
    KeyAlreadyExists,
    #[error("not found")]
    KeyNotFound,
    #[error("not enough storage on disk")]
    // InsufficientStorage,
    // #[error("payload too large")]
    TooLarge,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status_code = match self {
            ApiError::KeyAlreadyExists => StatusCode::CONFLICT,
            ApiError::KeyNotFound => StatusCode::NOT_FOUND,
            // ApiError::InsufficientStorage => StatusCode::INSUFFICIENT_STORAGE,
            ApiError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Any(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status_code, self.to_string()).into_response()
    }
}

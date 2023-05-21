use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

pub(crate) struct AppError(anyhow::Error);

// https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{extract::Query, routing::get, Router};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use serde::Deserialize;

mod csv;
mod orc;
mod parquet;
mod web;

#[derive(Deserialize)]
pub enum OutputType {
    CSV,
    TSV,
    PARQUET,
    ORC,
}

#[derive(Deserialize)]
pub struct DownloadParams {
    path: String,
    output_type: Option<OutputType>,
    sql: Option<String>,
}

// pub trait Reader {
//     fn new() -> Box<dyn Reader + Send>
//     where
//         Self: Sized;
//
//     fn read(&self, params: &DownloadParams) -> anyhow::Result<(Chunk<Box<dyn Array>>, Schema)>;
// }
//
// pub trait Writer {
//     fn new() -> Self;
//
//     fn write<W: std::io::Write, A: AsRef<dyn Array> + Send + Sync + 'static>(
//         &self,
//         writer: &mut W,
//         schema: Schema,
//         chunks: Vec<arrow2::error::Result<Chunk<A>>>,
//     ) -> anyhow::Result<()>;
// }

// https://jorgecarleitao.github.io/arrow2/io/index.html
#[axum_macros::debug_handler]
async fn download(query: Query<DownloadParams>) -> Result<impl IntoResponse, web::error::AppError> {
    log::info!("path: {}", query.path);
    let reader = match query.path.split(".").last() {
        Some("csv") => csv::CsvReader::new(),
        Some("parquet") => {
            // parquet::ParquetReader::new()
            return Ok((StatusCode::BAD_REQUEST, "Unsupported file type").into_response());
        }
        Some(_) => return Ok((StatusCode::BAD_REQUEST, "Unsupported file type").into_response()),
        None => return Ok((StatusCode::BAD_REQUEST, "Unknown file type").into_response()),
    };

    let mut w: Vec<u8> = Vec::new();
    // TODO: Writer が generics なので, `let writer = ...` とできない...
    match query.output_type.as_ref().unwrap_or(&OutputType::CSV) {
        OutputType::CSV => {}
        OutputType::PARQUET => {}
        OutputType::ORC => return Ok("NOT IMPLEMENTED".into_response()),
        OutputType::TSV => return Ok("NOT IMPLEMENTED".into_response()),
    };

    Ok(w.into_response())
}

// https://docs.rs/axum/latest/axum/
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    log::info!("starting HTTP server at http://localhost:3000");

    let app = Router::new()
        .route("/download", get(download))
        .route("/", get(|| async { "Hello, World!" }));

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

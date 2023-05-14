use arrow2::{array::Array, chunk::Chunk, datatypes::Schema};
use axum::response::IntoResponse;
use axum::{extract::Query, routing::get, Router};
use serde::Deserialize;

mod csv;
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
}

pub trait Reader {
    fn read(&self, params: &DownloadParams) -> anyhow::Result<(Chunk<Box<dyn Array>>, Schema)>;
}

pub trait Writer {
    fn write<W: std::io::Write, A: AsRef<dyn Array>>(
        &self,
        writer: &mut W,
        schema: Schema,
        columns: Box<[Chunk<A>]>,
    ) -> anyhow::Result<()>;
}

// https://jorgecarleitao.github.io/arrow2/io/csv_reader.html
async fn download(query: Query<DownloadParams>) -> Result<impl IntoResponse, web::error::AppError> {
    log::info!("path: {}", query.path);

    let reader = csv::CsvReader {};
    let (batch, schema) = reader.read(&query.0)?;

    let mut w: Vec<u8> = Vec::new();
    match query.output_type.as_ref().unwrap_or(&OutputType::CSV) {
        OutputType::CSV => {
            let writer = csv::CsvWriter {};
            writer.write(&mut w, schema, Box::new([batch]))?;
        }
        OutputType::TSV => return Ok("NOT IMPLEMENTED".into_response()),
        OutputType::PARQUET => {
            let writer = parquet::ParquetWriter {};
            writer.write(&mut w, schema, vec![Ok(batch)])?;
        }
        OutputType::ORC => return Ok("NOT IMPLEMENTED".into_response()),
    }

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

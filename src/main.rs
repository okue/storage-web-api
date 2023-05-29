use crate::csv::CsvRegisterer;
use crate::parquet::ParquetRegisterer;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use serde::Deserialize;
use std::collections::HashMap;

mod csv;
mod orc;
mod parquet;
mod web;

#[derive(Debug, Deserialize)]
pub enum OutputType {
    CSV,
    TSV,
    PARQUET,
    ORC,
}

#[derive(Debug, Deserialize)]
pub struct DownloadParams {
    input_tables: HashMap<String, String>,
    sql: String,
    output_type: Option<OutputType>,
}

#[axum_macros::debug_handler]
async fn download(
    request: Json<DownloadParams>,
) -> Result<impl IntoResponse, web::error::AppError> {
    log::info!("path: {:?}", request.0);

    let ctx = SessionContext::new();
    for (table_name, table_path) in &request.input_tables {
        match table_path.split(".").last() {
            Some("csv") => {
                CsvRegisterer::new()
                    .register(&ctx, &table_name, &table_path)
                    .await?
            }
            Some("parquet") => {
                ParquetRegisterer::new()
                    .register(&ctx, &table_name, &table_path)
                    .await?
            }
            Some(_) => {
                return Ok((StatusCode::BAD_REQUEST, "Unsupported file type").into_response())
            }
            None => return Ok((StatusCode::BAD_REQUEST, "Unknown file type").into_response()),
        };
    }

    let df = ctx.sql(&request.sql).await?;

    log::info!("dataframe schema is {}", df.schema());

    let mut output: Vec<u8> = Vec::new();
    match request.output_type.as_ref().unwrap_or(&OutputType::CSV) {
        OutputType::CSV => {
            log::info!("writing csv");
            csv::CsvWriter::new().write(&mut output, df).await?;
        }
        OutputType::TSV => {
            log::info!("writing tsv");
            csv::CsvWriter::new().write_tsv(&mut output, df).await?;
        }
        OutputType::PARQUET => {
            log::info!("writing parquet");
            parquet::ParquetWriter::new().write(&mut output, df).await?;
        }
        OutputType::ORC => return Ok("NOT IMPLEMENTED".into_response()),
    };

    Ok(output.into_response())
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

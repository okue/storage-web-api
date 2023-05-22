use datafusion::arrow;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use std::fs::File;

use crate::DownloadParams;

pub(crate) struct CsvReader {}

pub(crate) struct CsvWriter {}

impl CsvReader {
    pub(crate) fn new() -> Self {
        CsvReader {}
    }

    async fn read(&self, params: DownloadParams) -> anyhow::Result<DataFrame> {
        let ctx = SessionContext::new();
        ctx.register_csv("t", params.path.as_str(), CsvReadOptions::new())
            .await?;
        Ok(ctx
            .sql(
                params
                    .sql
                    .as_ref()
                    .unwrap_or(&"select * from t".to_string()),
            )
            .await?)
    }
}

impl CsvWriter {
    fn new() -> Self {
        CsvWriter {}
    }

    fn write<W: std::io::Write>(&self, output: &mut W, df: DataFrame) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::csv::CsvReader;
    use crate::DownloadParams;
    use crate::OutputType::CSV;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::fs::read;
    use std::io::stdout;

    #[tokio::test]
    async fn read_csv() -> anyhow::Result<()> {
        let reader = CsvReader::new();
        let df = reader
            .read(DownloadParams {
                path: "./arrow-testing/data/csv/aggregate_test_100.csv".to_string(),
                output_type: Option::from(CSV),
                sql: Option::from("select c1, c2 from t".to_string()),
            })
            .await?;
        df.show_limit(10).await?;

        Ok(())
    }

    #[tokio::test]
    async fn write_csv() -> anyhow::Result<()> {
        let reader = CsvReader::new();
        let df = reader
            .read(DownloadParams {
                path: "./arrow-testing/data/csv/aggregate_test_100.csv".to_string(),
                output_type: Option::from(CSV),
                sql: Option::from("select c1, c2 from t".to_string()),
            })
            .await?;

        let output = stdout();
        let record_batch_list: Vec<RecordBatch> = df.collect().await?;
        let mut writer = datafusion::arrow::csv::writer::WriterBuilder::default().build(output);
        for record_batch in &record_batch_list {
            writer.write(record_batch)?;
        }

        Ok(())
    }
}

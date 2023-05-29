use datafusion::arrow::csv::Writer;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{CsvReadOptions, SessionContext};

use futures_util::{StreamExt, TryStreamExt};

pub struct CsvRegisterer {}
pub struct CsvWriter {}

impl CsvRegisterer {
    pub fn new() -> Self {
        CsvRegisterer {}
    }

    pub async fn register(
        &self,
        ctx: &SessionContext,
        table_name: &String,
        table_location: &String,
    ) -> anyhow::Result<()> {
        ctx.register_csv(table_name, table_location, CsvReadOptions::default())
            .await?;
        Ok(())
    }
}

impl CsvWriter {
    pub fn new() -> Self {
        CsvWriter {}
    }

    pub async fn write<W: std::io::Write>(
        &self,
        output: &mut W,
        df: DataFrame,
    ) -> anyhow::Result<()> {
        let mut writer: Writer<&mut W> =
            datafusion::arrow::csv::writer::WriterBuilder::default().build(output);
        self.write_internal(&mut writer, df).await
    }

    pub async fn write_tsv<W: std::io::Write>(
        &self,
        output: &mut W,
        df: DataFrame,
    ) -> anyhow::Result<()> {
        let mut writer: Writer<&mut W> = datafusion::arrow::csv::writer::WriterBuilder::default()
            .with_delimiter(b'\t')
            .build(output);
        self.write_internal(&mut writer, df).await
    }

    async fn write_internal<W: std::io::Write>(
        &self,
        writer: &mut Writer<&mut W>,
        df: DataFrame,
    ) -> anyhow::Result<()> {
        let stream = df.execute_stream().await?;
        stream
            .map(|batch| writer.write(&batch?))
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn write_csv() -> anyhow::Result<()> {
        Ok(())
    }
}

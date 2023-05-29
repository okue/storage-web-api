use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use futures_util::{StreamExt, TryStreamExt};

pub struct ParquetRegisterer {}
pub struct ParquetWriter {}

impl ParquetRegisterer {
    pub fn new() -> Self {
        ParquetRegisterer {}
    }

    pub async fn register(
        &self,
        ctx: &SessionContext,
        table_name: &String,
        table_location: &String,
    ) -> anyhow::Result<()> {
        ctx.register_parquet(table_name, table_location, ParquetReadOptions::default())
            .await?;
        Ok(())
    }
}

impl ParquetWriter {
    pub fn new() -> Self {
        ParquetWriter {}
    }

    pub async fn write<W: std::io::Write>(
        &self,
        output: &mut W,
        df: DataFrame,
    ) -> anyhow::Result<()> {
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            output,
            SchemaRef::from(df.schema().clone()),
            Some(WriterProperties::builder().build()),
        )?;
        let stream = df.execute_stream().await?;
        stream
            .map(|batch| writer.write(&batch?).map_err(DataFusionError::ParquetError))
            .try_collect::<Vec<_>>()
            .await?;

        writer.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

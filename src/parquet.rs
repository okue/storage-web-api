use std::io::Write;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};

pub(crate) struct ParquetWriter {}

impl ParquetWriter {
    // https://jorgecarleitao.github.io/arrow2/main/guide/io/parquet_write.html
    pub fn write<W: Write, A: AsRef<dyn Array> + Send + Sync + 'static>(
        &self,
        writer: &mut W,
        schema: Schema,
        columns: Vec<arrow2::error::Result<Chunk<A>>>,
    ) -> anyhow::Result<()> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };
        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();
        let row_groups =
            RowGroupIterator::try_new(columns.into_iter(), &schema, options, encodings)?;

        let mut writer = FileWriter::try_new(writer, schema, options)?;
        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;
        Ok(())
    }
}

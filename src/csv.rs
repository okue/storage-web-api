use arrow2::datatypes::Schema;
use arrow2::{array::Array, chunk::Chunk, io::csv::read, io::csv::write};

use crate::{DownloadParams, Reader, Writer};

pub(crate) struct CsvReader {}

pub(crate) struct CsvWriter {}

impl Reader for CsvReader {
    fn read(&self, params: &DownloadParams) -> anyhow::Result<(Chunk<Box<dyn Array>>, Schema)> {
        let mut reader = read::ReaderBuilder::new().from_path(params.path.as_str())?;
        let (fields, _) = read::infer_schema(&mut reader, None, true, &read::infer)?;
        log::info!("{:?}", fields);

        let mut rows = vec![read::ByteRecord::default(); 100];
        let rows_read = read::read_rows(&mut reader, 0, &mut rows)?;
        let rows = &rows[..rows_read];

        Ok((
            read::deserialize_batch(rows, &fields, None, 0, read::deserialize_column)?,
            Schema::from(fields),
        ))
    }
}

impl Writer for CsvWriter {
    fn write<W: std::io::Write, A: AsRef<dyn Array>>(
        &self,
        writer: &mut W,
        _schema: Schema,
        columns: Box<[Chunk<A>]>,
    ) -> anyhow::Result<()> {
        let options = write::SerializeOptions::default();
        columns
            .iter()
            .try_for_each(|batch| write::write_chunk(writer, batch, &options))?;
        Ok(())
    }
}

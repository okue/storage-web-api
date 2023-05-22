use crate::DownloadParams;
use std::fs::File;

pub(crate) struct ParquetReader {}

pub(crate) struct ParquetWriter {}

impl ParquetReader {
    pub(crate) fn new() -> Self {
        ParquetReader {}
    }

    fn read(&self, params: &DownloadParams) -> anyhow::Result<()> {
        Ok(())
    }
}

impl ParquetWriter {
    fn new() -> Self {
        ParquetWriter {}
    }

    // https://jorgecarleitao.github.io/arrow2/main/guide/io/parquet_write.html
    fn write<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
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

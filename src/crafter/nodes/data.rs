use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use polars::prelude::*;
use polars::sql::SQLContext;

use crate::crafter::{Node, OutputType};

// region -- DataSrcParquet
#[derive(Clone, Debug, Default)]
pub struct DataSrcParquet {
    path: PathBuf,
    ticker: String,
    col_name: String,
}

impl DataSrcParquet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn path(mut self, file_path: impl Into<PathBuf>) -> Self {
        self.path = file_path.into();
        self
    }

    pub fn ticker(mut self, ticker: &str) -> Self {
        self.ticker = ticker.into();
        self
    }

    pub fn col_name(mut self, col_name: &str) -> Self {
        self.col_name = col_name.into();
        self
    }

    pub fn validate(&self) -> bool {
        if !self.path.is_file() {
            eprintln!(
                "Error: [{:?}], does not exist!",
                self.path.to_string_lossy()
            );
            return false;
        }
        true
    }

    fn filter(&self, df: DataFrame) -> DataFrame {
        if self.ticker.is_empty() {
            df
        } else {
            let mut ctx = SQLContext::new();
            ctx.register("tbl", df.lazy());

            let qry = format!("SELECT * FROM tbl WHERE Ticker = '{}'", self.ticker);
            println!("filtering parquet: {qry}");

            ctx.execute(&qry).unwrap().collect().unwrap()
        }
    }
}

impl Node for DataSrcParquet {
    fn run(&self) -> anyhow::Result<OutputType> {
        let mut src_file = File::open(&self.path)
            .with_context(|| format!("failed to open a src parquet file {:?}", &self.path))?;
        let df = ParquetReader::new(&mut src_file).finish()?;

        Ok(OutputType::DFrame(self.filter(df)))
    }
}

// endregion -- DataSrcParquet

// region -- WrapDF
#[derive(Clone, Debug)]
pub struct WrapDF {
    df: DataFrame,
}

impl WrapDF {
    pub fn new(df: DataFrame) -> Self {
        Self { df }
    }
}

impl Node for WrapDF {
    fn run(&self) -> Result<OutputType> {
        Ok(OutputType::DFrame(self.df.clone()))
    }
}

// endregion -- WrapDF

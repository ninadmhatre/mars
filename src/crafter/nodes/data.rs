use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use polars::prelude::*;
use polars::sql::SQLContext;

use crate::crafter::{Node, OutputType};

#[derive(Clone, Debug)]
pub struct DataSrcDB {
    pub connection_string: String,
    pub query_function: String,
}

// region -- DataSrcParquet
#[derive(Clone, Debug)]
pub struct DataSrcParquet {
    path: PathBuf,
    ticker: String,
}

impl DataSrcParquet {
    pub fn with_path(file_path: &str) -> Self {
        Self {
            path: file_path.into(),
            ticker: "".to_string(),
        }
    }

    pub fn with_ticker(file_path: &str, ticker: &str) -> Self {
        Self {
            path: file_path.into(),
            ticker: ticker.to_string(),
        }
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

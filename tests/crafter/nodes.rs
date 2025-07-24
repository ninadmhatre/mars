use polars::df;
use polars::prelude::*;
use std::path::{Path, PathBuf};

use mars::prelude::*;

pub fn get_project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

pub fn get_ref_data_src(name: &str) -> PathBuf {
    let path = get_project_root().join(format!("resources/{}.parquet", name));
    println!("generated path: {:?}", &path);
    path
}

fn get_wrapped_node(add_no_chg_val: bool) -> WrapDF {
    let mut dates = vec![1, 2, 3];
    let mut closes = vec![100.0, 101.0, 102.1];

    if add_no_chg_val {
        dates.push(4);
        closes.push(102.1);
    }

    let df = df!(
        "Date" => dates,
        "Close" => closes
    )
    .unwrap();

    WrapDF::new(df)
}

#[test]
fn test_node_parquet_read() {
    let node = DataSrcParquet::new().path(get_ref_data_src("stock_data"));

    assert!(node.validate(), "node failed validation!");

    match node.run() {
        Ok(OutputType::DFrame(df)) => {
            assert_eq!(df.shape(), (84, 7));
        }
        _ => panic!("Unexpected result type!"),
    }
}

#[test]
fn test_node_parquet_ticker_read() {
    let node = DataSrcParquet::new()
        .path(get_ref_data_src("stock_data"))
        .ticker("AAPL");

    match node.run() {
        Ok(OutputType::DFrame(df)) => {
            assert_eq!(df.shape(), (42, 7));
        }
        _ => panic!("unexpected result type!"),
    }
}

#[test]
fn test_node_extract() {
    let src_node = DataSrcParquet::new()
        .path(get_ref_data_src("stock_data"))
        .ticker("AAPL");
    let extract_node = ExtractDfCol::from_node(Box::new(src_node), "Close", Some("AAPL_Close"));

    match extract_node.run() {
        Ok(OutputType::DFrame(df)) => {
            assert_eq!(df.shape(), (42, 2));
            assert_eq!(df.get_column_names_str(), ["Date", "Px"]);
        }
        _ => panic!("test failed!"),
    }
}

#[test]
fn test_node_math_return() {
    let src_node = DataSrcParquet::new()
        .path(get_ref_data_src("stock_data"))
        .ticker("MSFT");
    let extract_node = ExtractDfCol::from_node(Box::new(src_node), "Close", Some("Px"));
    let returns = CalcReturn::from_node(Box::new(extract_node));

    match returns.run() {
        Ok(OutputType::DFrame(df)) => {
            assert_eq!(df.shape(), (42, 2));
            assert_eq!(df.get_column_names_str(), ["Date", "PctChg"]);
        }
        _ => panic!("test failed!"),
    }
}

#[test]
fn test_node_wrap_df() {
    let wrapped = get_wrapped_node(false);

    match wrapped.run() {
        Ok(OutputType::DFrame(df)) => {
            println!("{}", df);
            assert_eq!(df.shape(), (3, 2));
            assert_eq!(df.get_column_names_str(), ["Date", "Close"]);
        }
        _ => panic!("test failed!"),
    }
}

#[test]
fn test_node_wrap_extract() {
    let df = Box::new(get_wrapped_node(false));
    let extract = ExtractDfCol::from_node(df, "Close", Some("Px"));

    match extract.run() {
        Ok(OutputType::DFrame(df)) => {
            println!("{}", df);
            assert_eq!(df.shape(), (3, 2));
            assert_eq!(df.get_column_names_str(), ["Date", "Px"]);
        }
        _ => panic!("test failed!"),
    }
}

#[test]
fn test_node_wrap_returns() {
    let df = Box::new(get_wrapped_node(true));
    let extract = ExtractDfCol::from_node(df, "Close", Some("Px"));
    let returns = CalcReturn::from_node(Box::new(extract));

    let expected = df! {
        "Date" => [1, 2, 3, 4],
        "PctChg" => [None, Some(0.01), Some(0.01), Some(0.0)]
    }
    .unwrap();

    match returns.run() {
        Ok(OutputType::DFrame(df)) => {
            dbg!(&df);
            assert_eq!(df.shape(), (4, 2));
            assert_eq!(df.get_column_names_str(), ["Date", "PctChg"]);

            let df2: &DataFrame = &df
                .lazy()
                .with_column(
                    col("PctChg")
                        .round(2, RoundMode::HalfToEven)
                        .alias("PctChg"),
                )
                .collect()
                .unwrap();

            dbg!(&df2);
            assert_eq!(*df2, expected);
        }
        _ => panic!("test failed!"),
    }
}

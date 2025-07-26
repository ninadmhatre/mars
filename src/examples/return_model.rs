use crate::crafter::*;
use std::path::{Path, PathBuf};

fn get_project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn get_ref_data_src(name: &str) -> PathBuf {
    let path = get_project_root().join(format!("resources/{}.parquet", name));
    println!("generated path: {:?}", &path);
    path
}

fn get_stock_data_path() -> PathBuf {
    get_ref_data_src("stock_data")
}

fn get_index_data_path() -> PathBuf {
    get_ref_data_src("index_data")
}

pub fn create_simple_return_model() {
    let aapl_data = DataSrcParquet::new()
        .path(get_stock_data_path())
        .ticker("AAPL");

    let msft_data = DataSrcParquet::new()
        .path(get_stock_data_path())
        .ticker("MSFT");

    let index_data = DataSrcParquet::new()
        .path(get_index_data_path())
        .ticker("SPX");

    let aapl_close = ExtractDfCol::new()
        .node(Box::new(aapl_data))
        .col("Close")
        .extract_col_as("AAPL_Close");

    let msft_close = ExtractDfCol::new()
        .node(Box::new(msft_data))
        .col("Close")
        .extract_col_as("MSFT_Close");

    let spx_close = ExtractDfCol::new()
        .node(Box::new(index_data))
        .col("Close")
        .extract_col_as("SPX_Close");

    // dbg!(&aapl_close.run());
    // dbg!(&msft_close.run());
    // dbg!(&spx_close.run());

    let all_pxs = StackDfs::new()
        .nodes(vec![
            Box::new(aapl_close),
            Box::new(msft_close),
            Box::new(spx_close),
        ])
        .join_by("Date")
        .horizontal();

    let returns = CalcReturn::new().node(Box::new(all_pxs));

    dbg!(returns.run());

    // let backtest = BackTest::new(WindowType::SlidingWindow)
    //     .src(returns)
    //     .train_days(10)
    //     .test_days(3);
    //
    // let in_sample_metrics = Metrics::new(MetricsType::InSample).r2(true).mse(true);
    // let out_sample_metrics = Metrics::new(MetricsType::OutSample).y_true(true).y_pred(true).residual(true);
    //
    // let fit = ModelFitting::new(ModelType::Linear).target("SPX_Close_PctChg").capture(in_sample_metrics).capture(out_sample_metrics);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_model() {
        create_simple_return_model();
    }
}

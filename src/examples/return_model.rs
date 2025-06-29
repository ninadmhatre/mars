use crate::crafter::*;

const STOCK_PARQUET_PATH: &str = "/home/ninad/Documents/python/stock_data.parquet";
const INDEX_PARQUET_PATH: &str = "/home/ninad/Documents/python/index_data.parquet";

fn main() {
    create_simple_return_model();
}

pub fn create_simple_return_model() {
    let aapl_data = DataSrcParquet::with_ticker(STOCK_PARQUET_PATH, "AAPL");
    let msft_data = DataSrcParquet::with_ticker(STOCK_PARQUET_PATH, "MSFT");

    let index_data = DataSrcParquet::with_ticker(INDEX_PARQUET_PATH, "SPX");

    let aapl_close = ExtractDfCol::from_node(Box::new(aapl_data), "Close", "AAPL_Close".into());
    let msft_close = ExtractDfCol::from_node(Box::new(msft_data), "Close", "MSFT_Close".into());
    let spx_close = ExtractDfCol::from_node(Box::new(index_data), "Close", "SPX_Close".into());

    let all_pxs = HStackDfs::new()
        .with_nodes(vec![
            Box::new(aapl_close),
            Box::new(msft_close),
            Box::new(spx_close),
        ])
        .with_index("Date")
        .drop_nulls();

    let returns = CalcReturn::from_node(Box::new(all_pxs));

    dbg!(&returns.run());
    // let model = ReturnModel::from_node(Box::new(returns));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_model() {
        create_simple_return_model();
    }
}

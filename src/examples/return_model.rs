// use crate::crafter::*;

// const STOCK_PARQUET_PATH: &str = "../resources/stock_data.parquet";
// const INDEX_PARQUET_PATH: &str = "../resources/index_data.parquet";

// pub fn create_simple_return_model() {
//     let aapl_data = DataSrcParquet::new()
//         .path(STOCK_PARQUET_PATH)
//         .ticker("AAPL");
//
//     let msft_data = DataSrcParquet::new()
//         .path(STOCK_PARQUET_PATH)
//         .ticker("MSFT");
//
//     let index_data = DataSrcParquet::new().path(INDEX_PARQUET_PATH).ticker("SPX");
//
//     let aapl_close = ExtractDfCol::from_node(Box::new(aapl_data), "Close", "AAPL_Close".into());
//     let msft_close = ExtractDfCol::from_node(Box::new(msft_data), "Close", "MSFT_Close".into());
//     let spx_close = ExtractDfCol::from_node(Box::new(index_data), "Close", "SPX_Close".into());
//
//     dbg!(&aapl_close.run());
//     dbg!(&msft_close.run());
//     dbg!(&spx_close.run());
//
//     let all_pxs = StackDfs::with_nodes(
//         vec![
//             Box::new(aapl_close),
//             Box::new(msft_close),
//             Box::new(spx_close),
//         ],
//         "Date",
//         "horizontal",
//     );
//
//     dbg!(&all_pxs.run());
//
//     let returns = CalcReturn::from_node(Box::new(all_pxs));
//
//     dbg!(&returns.run());
//     // let model = ReturnModel::from_node(Box::new(returns));
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_return_model() {
//         create_simple_return_model();
//     }
// }

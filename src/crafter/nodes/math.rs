use crate::crafter::{InputType, Node, OutputType, WrapDF};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CalcReturn {
    pub src: InputType,
}

impl CalcReturn {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(mut self, src: Box<dyn Node>) -> Self {
        self.src = InputType::Node(src);
        self
    }

    fn calc_for_node(&self, node: &Box<dyn Node>) -> anyhow::Result<OutputType> {
        match node.run()? {
            OutputType::DFrame(df) => self.calc(df),
            _ => Err(anyhow::anyhow!("Only OutputType::DFrame is supported!")),
        }
    }

    fn calc_for_wrapped_df(&self, node: &WrapDF) -> anyhow::Result<OutputType> {
        println!("Calculating wrapped df...");
        match node.run()? {
            OutputType::DFrame(df) => self.calc(df),
            _ => Err(anyhow::anyhow!("Only OutputType::DFrame is supported!")),
        }
    }
    fn calc(&self, df: DataFrame) -> anyhow::Result<OutputType> {
        let mut result = df.clone();
        let mut final_cols: Vec<String> = vec!["Date".to_string()];

        for px_col in df.get_column_names_str() {
            if px_col == "Date" {
                continue;
            }

            let alias = format!("{px_col}_PctChg");
            result = result
                .lazy()
                .with_column((col(px_col) / col(px_col).shift(lit(1)) - lit(1.0)).alias(&alias))
                .collect()?;

            final_cols.push(alias);
        }

        let final_df = result.select(final_cols)?;

        dbg!(&final_df);
        Ok(OutputType::DFrame(final_df))
    }
}

#[typetag::serde]
impl Node for CalcReturn {
    fn run(&self) -> anyhow::Result<OutputType> {
        match &self.src {
            InputType::Node(node) => self.calc_for_node(node),
            InputType::WrappedDFrame(wrap_df) => self.calc_for_wrapped_df(wrap_df),
            _ => Err(anyhow::anyhow!("InputType Not supported!")),
        }
    }
}

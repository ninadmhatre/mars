use polars::prelude::*;

use crate::crafter::{InputType, Node, OutputType};

#[derive(Debug, Default)]
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
}

impl Node for CalcReturn {
    fn run(&self) -> anyhow::Result<OutputType> {
        match &self.src {
            InputType::Node(node) => match node.run()? {
                OutputType::DFrame(df) => {
                    let result = df
                        .lazy()
                        .with_columns([
                            (col("Px") / col("Px").shift(lit(1)) - lit(1.0)).alias("PctChg")
                        ])
                        .collect()?;

                    let final_df = result.select(["Date", "PctChg"])?;
                    Ok(OutputType::DFrame(final_df))
                }
                _ => Err(anyhow::anyhow!("Only OutputType::DFrame is supported!")),
            },
            _ => Err(anyhow::anyhow!("InputType Not supported!")),
        }
    }
}

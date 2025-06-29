use polars::prelude::*;

use crate::crafter::{InputType, Node, OutputType};

// region -- ExtractDfCol
#[derive(Debug)]
pub struct ExtractDfCol {
    src: InputType,
    col: String,
    alias: String,
}

impl ExtractDfCol {
    pub fn from_df(src: DataFrame, col: &str, alias: Option<&str>) -> Self {
        let alias_d = if alias.is_none() {
            "Px"
        } else {
            alias.unwrap()
        }
        .to_string();

        Self {
            src: InputType::DFrame(src),
            col: col.to_string(),
            alias: alias_d,
        }
    }

    pub fn from_node(src: Box<dyn Node>, col: &str, alias: Option<&str>) -> Self {
        let alias_d = if alias.is_none() {
            "Px"
        } else {
            alias.unwrap()
        }
        .to_string();

        Self {
            src: InputType::Node(src),
            col: col.to_string(),
            alias: alias_d,
        }
    }
}

impl Node for ExtractDfCol {
    fn run(&self) -> anyhow::Result<OutputType> {
        match &self.src {
            InputType::Node(node) => {
                dbg!(&node);
                let result = node.run()?;
                dbg!(&result);
                match result {
                    OutputType::DFrame(df) => {
                        let mut selected_df = df.select(["Date", &self.col])?;
                        selected_df.rename(&self.col, "Px".into())?;

                        Ok(OutputType::DFrame(selected_df))
                    }
                    _ => Err(anyhow::anyhow!("Dataframe has no col: {}", self.col)),
                }
            }
            _ => Err(anyhow::anyhow!("InputType Not supported!!")),
        }
    }
}

// endregion -- ExtractDfCol

// region -- FilterOnDfCol
#[derive(Debug)]
pub struct FilterOnDfCol {
    src: InputType,
    col: String,
    val: String,
}

impl FilterOnDfCol {
    pub fn from_df(src: DataFrame, col: &str, val: &str) -> Self {
        Self {
            src: InputType::DFrame(src),
            col: col.to_string(),
            val: val.to_string(),
        }
    }

    pub fn from_node(src: Box<dyn Node>, col: &str, val: &str) -> Self {
        Self {
            src: InputType::Node(src),
            col: col.to_string(),
            val: val.to_string(),
        }
    }
}

impl Node for FilterOnDfCol {
    fn run(&self) -> anyhow::Result<OutputType> {
        match &self.src {
            InputType::Node(node) => {
                let result = node.run()?;
                match result {
                    OutputType::DFrame(df) => {
                        let filtered_df = df
                            .lazy()
                            .filter(col(&self.col).eq(lit(self.val.as_str())))
                            .collect()?;

                        Ok(OutputType::DFrame(filtered_df))
                    }
                    _ => Err(anyhow::anyhow!("Dataframe has no col: {}", self.col)),
                }
            }
            _ => Err(anyhow::anyhow!("InputType Not supported!!")),
        }
    }
}

// endregion -- FilterOnDfCol

// region -- HStackDfs
#[derive(Debug, Default)]
pub struct HStackDfs {
    dfs: Vec<InputType>,
    index: String,
    drop_nulls: bool,
}

impl HStackDfs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_nodes(mut self, nodes: Vec<Box<dyn Node>>) -> Self {
        self.dfs = nodes.into_iter().map(|n| InputType::Node(n)).collect();
        self
    }

    pub fn with_index(mut self, index: &str) -> Self {
        self.index = index.to_string();
        self
    }

    pub fn drop_nulls(mut self) -> Self {
        self.drop_nulls = true;
        self
    }
}

impl Node for HStackDfs {
    fn run(&self) -> anyhow::Result<OutputType, anyhow::Error> {
        let dfs: Vec<DataFrame> = self
            .dfs
            .iter()
            .map(|n| match n {
                InputType::Node(n) => match n.run() {
                    Ok(OutputType::DFrame(df)) => df,
                    _ => Err(anyhow::anyhow!("InputType Not supported!!")),
                },
                _ => Err(anyhow::anyhow!("InputType Not supported!!")),
            })
            .collect::<anyhow::Result<Vec<DataFrame>>>()?;

        let mut merged_df = dfs[0].clone();
        for df in dfs.iter().skip(1) {
            merged_df = merged_df.merge(df, self.drop_nulls, MergeStrategy::Append)?;
        }
        let df = dfs
            .iter()
            .map(|df| df.select(["Date", &self.index]).unwrap())
            .collect::<Vec<DataFrame>>();
        let df = df.lazy().hstack(df.iter().map(|df| df.lazy())).collect()?;

        Ok(OutputType::DFrame(df))
    }
}

// endregion -- HStackDfs

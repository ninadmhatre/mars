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
                match result {
                    OutputType::DFrame(df) => {
                        let mut selected_df = df.select(["Date", &self.col])?;
                        selected_df.rename(&self.col, self.alias.as_str().into())?;

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

// region -- StackDfs
#[derive(Debug, Default)]
pub struct StackDfs {
    dfs: Vec<InputType>,
    uniq_col: String,
    direction: String,
}

impl StackDfs {
    pub fn new() -> Self {
        Self {
            dfs: Vec::default(),
            uniq_col: String::new(),
            direction: "horizontal".to_string(),
        }
    }

    pub fn with_nodes(nodes: Vec<Box<dyn Node>>, uniq_col: &str, direction: &str) -> Self {
        let dfs = nodes.into_iter().map(|n| InputType::Node(n)).collect();

        Self {
            dfs,
            uniq_col: uniq_col.into(),
            direction: direction.into(),
        }
    }
}

impl Node for StackDfs {
    fn run(&self) -> anyhow::Result<OutputType> {
        let mut dfs: Vec<DataFrame> = Vec::new();

        for df in &self.dfs {
            match df {
                InputType::Node(node) => match node.run() {
                    Ok(OutputType::DFrame(df)) => dfs.push(df),
                    _ => eprintln!("failed to run node {:?}", node),
                },
                _ => eprintln!(
                    "ignoring input type other than InputType::Node! found: {:?}",
                    df
                ),
            }
        }

        if self.direction == "horizontal" {
            // let joined_df = concat_df_horizontal(&dfs, true)?;
            // Ok(OutputType::DFrame(joined_df))
            let mut acc = dfs[0].clone();
            for df in dfs.iter().skip(1) {
                acc = acc.join(
                    df,
                    [self.uniq_col.as_str()],
                    [self.uniq_col.as_str()],
                    JoinArgs::new(JoinType::Cross),
                    None,
                )?;
            }

            Ok(OutputType::DFrame(acc))
        } else {
            let mut acc = dfs[0].clone();
            for df in dfs.iter().skip(1) {
                acc = acc.join(
                    df,
                    [self.uniq_col.as_str()],
                    [self.uniq_col.as_str()],
                    JoinArgs::new(JoinType::Cross),
                    None,
                )?;
            }

            Ok(OutputType::DFrame(acc))
        }
    }
}

// endregion -- StackDfs

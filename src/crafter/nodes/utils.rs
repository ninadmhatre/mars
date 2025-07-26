use polars::prelude::*;

use crate::crafter::{InputType, Node, OutputType, WrapDF};

// region -- ExtractDfCol
#[derive(Debug)]
pub struct ExtractDfCol {
    src: InputType,
    col: String,
    alias: String,
}

impl Default for ExtractDfCol {
    fn default() -> Self {
        Self {
            src: InputType::default(),
            col: String::default(),
            alias: "Px".to_string(),
        }
    }
}

impl ExtractDfCol {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(mut self, src: Box<dyn Node>) -> Self {
        self.src = InputType::Node(src);
        self
    }

    pub fn df(mut self, df: WrapDF) -> Self {
        self.src = InputType::WrappedDFrame(df);
        self
    }

    pub fn col(mut self, name: &str) -> Self {
        self.col = name.into();
        self
    }

    pub fn extract_col_as(mut self, name: &str) -> Self {
        self.alias = name.into();
        self
    }
}

impl Node for ExtractDfCol {
    fn run(&self) -> anyhow::Result<OutputType> {
        match &self.src {
            InputType::Node(node) => {
                // dbg!(&node);
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
            InputType::WrappedDFrame(node) => {
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
#[derive(Debug, Default)]
pub struct FilterOnDfCol {
    src: InputType,
    col: String,
    val: String,
}

impl FilterOnDfCol {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(mut self, src: Box<dyn Node>) -> Self {
        self.src = InputType::Node(src);
        self
    }

    pub fn df(mut self, df: WrapDF) -> Self {
        self.src = InputType::WrappedDFrame(df);
        self
    }

    pub fn col(mut self, name: &str) -> Self {
        self.col = name.into();
        self
    }

    pub fn filter(mut self, val: &str) -> Self {
        self.val = val.into();
        self
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
#[derive(Debug)]
pub struct StackDfs {
    dfs: Vec<InputType>,
    uniq_col: String,
    direction: String,
}

impl Default for StackDfs {
    fn default() -> Self {
        Self {
            dfs: Vec::default(),
            uniq_col: String::new(),
            direction: "horizontal".to_string(),
        }
    }
}

impl StackDfs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn nodes(mut self, nodes: Vec<Box<dyn Node>>) -> Self {
        self.dfs = nodes.into_iter().map(|n| InputType::Node(n)).collect();
        self
    }

    pub fn join_by(mut self, col: &str) -> Self {
        self.uniq_col = col.into();
        self
    }

    pub fn horizontal(mut self) -> Self {
        self.direction = "horizontal".into();
        self
    }

    pub fn vertical(mut self) -> Self {
        self.direction = "vertical".into();
        self
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
                    JoinArgs::new(JoinType::Left),
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

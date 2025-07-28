use std::path::PathBuf;

use crate::crafter::WrapDF;
use anyhow::Result;
use polars::frame::DataFrame;
use polars::prelude::Series;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum OutputType {
    DFrame(DataFrame),
    DSeries(Series),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InputType {
    DFrame(DataFrame),
    WrappedDFrame(WrapDF),
    Node(Box<dyn Node + 'static>),
    NodeName(String),
}

impl Default for InputType {
    fn default() -> Self {
        InputType::DFrame(DataFrame::default())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DBFunction {
    ClosePx,
    OpenPx,
    BothPx,
}

#[typetag::serde(tag = "type")]
pub trait Node: std::fmt::Debug {
    fn run(&self) -> anyhow::Result<OutputType>;
}

pub trait DBNode: Node {
    /// Trait for Nodes which runs database queries and return a single
    /// Dataframe
    fn run_query(file_path: PathBuf, table_name: &str) -> Result<OutputType>;
}

use std::path::PathBuf;

use crate::crafter::WrapDF;
use anyhow::Result;
use polars::frame::DataFrame;
use polars::prelude::Series;

#[derive(Debug)]
pub enum OutputType {
    DFrame(DataFrame),
    DSeries(Series),
}

#[derive(Debug)]
pub enum InputType {
    DFrame(DataFrame),
    WrapDFrame(WrapDF),
    Node(Box<dyn Node + 'static>),
    NodeName(String),
}

impl Default for InputType {
    fn default() -> Self {
        InputType::DFrame(DataFrame::default())
    }
}

#[derive(Clone, Debug)]
pub enum DBFunction {
    ClosePx,
    OpenPx,
    BothPx,
}

pub trait Node: std::fmt::Debug {
    fn run(&self) -> anyhow::Result<OutputType>;
}

pub trait DBNode: Node {
    /// Trait for Nodes which runs database queries and return a single
    /// Dataframe
    fn run_query(file_path: PathBuf, table_name: &str) -> Result<OutputType>;
}

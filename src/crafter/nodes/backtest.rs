use crate::crafter::{InputType, Metrics, ModelFit};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WindowType {
    Sliding,
    Expanding,
}

impl Default for WindowType {
    fn default() -> Self {
        WindowType::Sliding
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BackTest {
    pub fit: ModelFit,
    window_type: WindowType,
    train_days: u32,
    test_days: u8,
}

impl BackTest {
    pub fn new(window_type: WindowType) -> Self {
        Self {
            window_type,
            ..Default::default()
        }
    }

    pub fn fit(mut self, fit: ModelFit) -> Self {
        self.fit = fit;
        self
    }

    pub fn train_on(mut self, days: u32) -> Self {
        self.train_days = days;
        self
    }

    pub fn test_on(mut self, days: u8) -> Self {
        self.test_days = days;
        self
    }

    pub fn run(self) -> Metrics {}
}

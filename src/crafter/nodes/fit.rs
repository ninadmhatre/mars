use crate::crafter::InputType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Metrics {
    r2: bool,
    mse: bool,
    mae: bool,
    y_true: bool,
    y_pred: bool,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn r2(mut self, flag: bool) -> Self {
        self.r2 = flag;
        self
    }

    pub fn mse(mut self, flag: bool) -> Self {
        self.mse = flag;
        self
    }

    pub fn mae(mut self, flag: bool) -> Self {
        self.mae = flag;
        self
    }

    pub fn y_true(mut self, flag: bool) -> Self {
        self.y_true = flag;
        self
    }

    pub fn y_pred(mut self, flag: bool) -> Self {
        self.y_pred = flag;
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FitType {
    OLS,
    Ridge,
}

impl Default for FitType {
    fn default() -> Self {
        FitType::OLS
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CaptureMetrics {
    in_sample: Metrics,
    out_sample: Metrics,
}

impl Default for CaptureMetrics {
    fn default() -> Self {
        let in_sample = Metrics::new().r2(true).mae(true).mse(true);
        let out_sample = Metrics::new().y_true(true).y_pred(true);

        Self {
            in_sample,
            out_sample,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ModelFit {
    src: InputType,
    fit_type: FitType,
    features: Vec<String>,
    target: String,
    metrics: CaptureMetrics,
}

impl ModelFit {
    pub fn new(src: InputType) -> Self {
        Self {
            src,
            ..Default::default()
        }
    }

    pub fn fit_type(mut self, fit_type: FitType) -> Self {
        self.fit_type = fit_type;
        self
    }

    pub fn features(mut self, names: Vec<&str>) -> Self {
        self.features = names.iter().map(|c| c.to_string()).collect();
        self
    }

    pub fn target(mut self, target: &str) -> Self {
        self.target = target.into();
        self
    }

    pub fn capture(mut self, what: CaptureMetrics) -> Self {
        self.metrics = what;
        self
    }
}

use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeSeries {
    metric: HashMap<String, String>,
    pub values: Vec<Option<f64>>,
    timestamps: Vec<u64>,
}

impl TimeSeries {
    pub fn name(&self) -> &str {
        self.metric.get("__name__").expect("Got a metric without name")
    }
}
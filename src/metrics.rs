use std::collections::HashMap;
use std::fmt::Write;

use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeSeries {
    pub metric: HashMap<String, String>,
    pub values: Vec<Option<f64>>,
    timestamps: Vec<u64>,
}

impl TimeSeries {
    pub fn name(&self) -> &str {
        self.metric.get("__name__").expect("Got a metric without name")
    }

    pub fn format_metric(&self) -> String {
        let mut metric = self.name().to_owned();

        let mut labels: Vec<_> = self.metric.iter().filter(|(name, _value)| *name != "__name__").collect();
        labels.sort();

        for (index, (name, value)) in labels.iter().enumerate() {
            if index == 0 {
                metric.push('{');
            } else {
                metric.push_str(", ");
            }

            _ = write!(&mut metric, "{name}={value:?}");

            if index == labels.len() - 1 {
                metric.push('}');
            }
        }

        metric
    }

    pub fn rewrite(&self, metric: HashMap<String, String>) -> TimeSeries {
        TimeSeries {
            metric,
            values: self.values.clone(),
            timestamps: self.timestamps.clone(),
        }
    }
}

#[allow(dead_code)]
pub enum MigratedTimeSeries {
    Unchanged,
    Changed(TimeSeries),
    Deleted,
}
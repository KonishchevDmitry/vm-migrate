use std::collections::HashMap;
use std::fmt::Write;

use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeSeries {
    metric: HashMap<String, String>,
    values: Vec<Option<f64>>,
    timestamps: Vec<i64>,
}

impl TimeSeries {
    pub fn name(&self) -> &str {
        self.metric.get("__name__").expect("Got a metric without name")
    }

    pub fn label(&self, name: &str) -> &str {
        self.metric.get(name).map(String::as_str).unwrap_or_default()
    }

    pub fn set_label(&mut self, name: &str, value: &str) {
        self.metric.insert(name.to_owned(), value.to_owned());
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

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn has_records_before(&self, timestamp: i64) -> bool {
        self.timestamps.iter().any(|&record| record < timestamp)
    }

    pub fn iter(&self) -> impl Iterator<Item = (i64, Option<f64>)> + use<'_> {
        assert_eq!(self.timestamps.len(), self.values.len());
        self.timestamps.iter().cloned().zip(self.values.iter().cloned())
    }

    pub fn add(&mut self, time: i64, value: Option<f64>) {
        self.timestamps.push(time);
        self.values.push(value);
    }

    pub fn clone_empty(&self) -> TimeSeries {
        TimeSeries {
            metric: self.metric.clone(),
            values: Vec::new(),
            timestamps: Vec::new(),
        }
    }
}

#[allow(dead_code)]
pub enum MigratedTimeSeries {
    Unchanged,
    Changed(TimeSeries),
    Rewrite(Vec<TimeSeries>),
    Deleted,
}
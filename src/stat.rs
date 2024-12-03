use std::collections::HashMap;
use std::io::{self, Write};

use tabled::{Table, Tabled};
use tabled::settings::{Alignment, Height, object::{Rows, Columns}, style::Style};

use crate::types::TimeSeries;

pub struct Stat {
    total: u64,
    metrics: HashMap<String, u64>,
}

impl Stat {
    pub fn new() -> Stat {
        Stat {
            total: 0,
            metrics: HashMap::new(),
        }
    }

    pub fn add(&mut self, time_series: &TimeSeries) {
        let namespace = get_metric_namespace(time_series.name());
        let count = time_series.values.len().try_into().unwrap();

        if let Some(total) = self.metrics.get_mut(namespace) {
            *total += count;
        } else {
            self.metrics.insert(namespace.to_owned(), count);
        }

        self.total += count;
    }

    pub fn print(self) {
        let mut rows = Vec::new();
        let mut add = |name, count| {
            rows.push(StatRow {
                name,
                percentage: format!("{:.1}%", count as f64 / self.total as f64 * 100.),
            });
        };

        let mut other = 0;
        let mut metrics: Vec<_> = self.metrics.into_iter().collect();
        metrics.sort_by(|(_, a), (_, b)| b.cmp(a));

        for (name, count) in metrics {
            if count * 100 / self.total >= 1 {
                add(name, count);
            } else {
                other += count;
            }
        }

        if other != 0 {
            add("other".to_owned(), other);
        }

        let mut table = Table::new(&rows);
        table.with(Style::blank());
        table.modify(Rows::first(), Height::increase(2));
        table.modify(Columns::single(1), Alignment::right());

        let _ = writeln!(io::stdout(), "\n{}", table);
    }
}

#[derive(Tabled)]
struct StatRow {
    #[tabled(rename = "Metric")]
    name: String,

    #[tabled(rename = "Percentage")]
    percentage: String,
}

fn get_metric_namespace(name: &str) -> &str {
    let mut delimiters = 0;

    for (index, char) in name.char_indices() {
        if char == '_' {
            delimiters += 1;
            if delimiters >= 2 {
                return &name[..index];
            }
        }
    }

    name
}
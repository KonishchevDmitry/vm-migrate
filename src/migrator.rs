use crate::metrics::{TimeSeries, MigratedTimeSeries};

pub fn migrate(time_series: &TimeSeries) -> MigratedTimeSeries {
    for (name, value) in &time_series.metric {
        if value == "md0" {
            let mut metric = time_series.metric.clone();
            metric.insert(name.to_owned(), "root".to_owned());
            return MigratedTimeSeries::Changed(time_series.rewrite(metric));
        }
    }

    if time_series.name().starts_with("server_services_") {
        return MigratedTimeSeries::Deleted;
    }

    MigratedTimeSeries::Unchanged
}
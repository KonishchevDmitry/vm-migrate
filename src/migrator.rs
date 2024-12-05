use chrono::{Local, TimeZone};

use crate::metrics::{TimeSeries, MigratedTimeSeries};

pub fn migrate(time_series: &TimeSeries) -> MigratedTimeSeries {
    if time_series.name() == "investments_performance" && time_series.label("instrument") == "Russian bonds" {
        let split_time = date(2021, 11, 6);
        if !time_series.has_records_before(split_time) {
            return MigratedTimeSeries::Unchanged
        }

        let mut euro_bonds = time_series.clone_empty();
        euro_bonds.set_label("instrument", "Russian Eurobonds");

        let mut bonds = time_series.clone_empty();

        for (time, value) in time_series.iter() {
            if time < split_time {
                euro_bonds.add(time, value);
            } else {
                bonds.add(time, value);
            }
        }

        return MigratedTimeSeries::Rewrite(vec![euro_bonds, bonds])
    }

    MigratedTimeSeries::Unchanged
}

fn date(year: i32, month: u32, day: u32) -> i64 {
    Local.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap().timestamp() * 1000
}
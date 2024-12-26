use chrono::{Local, TimeZone};

use crate::metrics::{TimeSeries, MigratedTimeSeries};

pub fn migrate(time_series: &TimeSeries) -> MigratedTimeSeries {
    if time_series.name().starts_with("backup_") && time_series.label("job") == "node" && time_series.label("name") == "job" {
        return MigratedTimeSeries::Deleted;
    }

    if time_series.label("service").contains("-org.fedoraproject.SetroubleshootPrivileged@") {
        return MigratedTimeSeries::Deleted;
    }

    if time_series.label("job") == "node" && time_series.label("instance") != "proxy" && time_series.label("device") == "/dev/md127" {
        let mut time_series = time_series.clone();
        time_series.set_label("device", "/dev/md/root");
        return MigratedTimeSeries::Rewrite(vec![time_series]);
    }

    if time_series.label("job") == "node" && time_series.label("instance") != "proxy" && time_series.label("device") == "md127" {
        let mut time_series = time_series.clone();
        time_series.set_label("device", "md/root");
        return MigratedTimeSeries::Rewrite(vec![time_series]);
    }

    if time_series.label("job") == "node" && time_series.label("instance") != "proxy" && time_series.label("device") == "/dev/md0" {
        let mut rewritten = time_series.clone_empty();
        rewritten.set_label("device", "/dev/md/root");

        for (time, value) in time_series.iter() {
            if time < 1734447790 * 1000 {
                rewritten.add(time, value);
            }
        }

        return MigratedTimeSeries::Rewrite(vec![rewritten]);
    }

    if time_series.label("job") == "node" && time_series.label("instance") != "proxy" && time_series.label("device") == "md0" {
        let mut rewritten = time_series.clone_empty();
        rewritten.set_label("device", "md/root");

        for (time, value) in time_series.iter() {
            if time < 1734447790 * 1000 {
                rewritten.add(time, value);
            }
        }

        return MigratedTimeSeries::Rewrite(vec![rewritten]);
    }

    if time_series.name() == "investments_performance" {
        if time_series.label("instrument") == "Russian bonds" {
            let euro_bonds_until = date(2021, 11, 6);
            let min_time = date(2023, 3, 2);

            let mut euro_bonds = time_series.clone_empty();
            euro_bonds.set_label("instrument", "Russian Eurobonds");

            let mut bonds = time_series.clone_empty();

            for (time, value) in time_series.iter() {
                if time < euro_bonds_until {
                    euro_bonds.add(time, value);
                } else if time >= min_time {
                    bonds.add(time, value);
                }
            }

            return MigratedTimeSeries::Rewrite(vec![euro_bonds, bonds])
        } else if time_series.label("instrument") == "Global REIT" {
            let min_time = date(2023, 10, 26);
            return MigratedTimeSeries::Changed(time_series.filter(|time, _value| {
                time >= min_time
            }))
        } else if time_series.label("instrument") == "Emerging Markets stocks" {
            let min_time = date(2023, 7, 29);
            return MigratedTimeSeries::Changed(time_series.filter(|time, _value| {
                time >= min_time
            }))
        }
    }

    MigratedTimeSeries::Unchanged
}

fn date(year: i32, month: u32, day: u32) -> i64 {
    Local.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap().timestamp() * 1000
}
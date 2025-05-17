use chrono::{Local, TimeZone};

use crate::metrics::{TimeSeries, MigratedTimeSeries};

// TODO(konishchev): node_memory_MemTotal_bytes -> server_memory_meminfo{name="MemTotal"}
// TODO(konishchev): node_memory_MemFree_bytes
// TODO(konishchev): node_memory_Cached_bytes
// TODO(konishchev): node_memory_Shmem_bytes
// TODO(konishchev): node_memory_Buffers_bytes
// TODO(konishchev): node_memory_KReclaimable_bytes
// TODO(konishchev): node_memory_SReclaimable_bytes
// TODO(konishchev): node_memory_MemAvailable_bytes
// TODO(konishchev): node_memory_Percpu_bytes
// TODO(konishchev): node_memory_KernelStack_bytes
// TODO(konishchev): node_memory_PageTables_bytes
// TODO(konishchev): node_memory_SecPageTables_bytes
// TODO(konishchev): node_memory_VmallocUsed_bytes
// TODO(konishchev): node_memory_Zswap_bytes
// TODO(konishchev): node_memory_SwapCached_bytes
// TODO(konishchev): node_memory_SUnreclaim_bytes
// TODO(konishchev): node_memory_AnonPages_bytes
// TODO(konishchev): node_memory_Unevictable_bytes
// TODO(konishchev): node_memory_Active_anon_bytes -> Active(anon)
// TODO(konishchev): node_memory_Active_file_bytes -> Active(file)
// TODO(konishchev): node_memory_Inactive_anon_bytes -> Inactive(anon)
// TODO(konishchev): node_memory_Inactive_file_bytes -> Inactive(file)
// TODO(konishchev): node_memory_Writeback_bytes
// TODO(konishchev): node_memory_Dirty_bytes
// TODO(konishchev): node_memory_SwapTotal_bytes
// TODO(konishchev): node_memory_SwapFree_bytes
pub fn migrate(time_series: &TimeSeries) -> MigratedTimeSeries {
    let name = time_series.name();

    if time_series.label("job") == "node" {
        if name.starts_with("backup_") && time_series.label("name") == "macos.laptop" {
            let mut time_series = time_series.clone();
            time_series.set_label("name", "laptop");
            return MigratedTimeSeries::Changed(time_series);
        }

        if name == "node_systemd_unit_state" {
            return MigratedTimeSeries::Deleted;
        }
    }

    if name == "investments_brokers" && time_series.label("broker") == "Тинькофф" {
        let mut time_series = time_series.clone();
        time_series.set_label("broker", "Т‑Банк");
        return MigratedTimeSeries::Changed(time_series);
    }

    if name == "investments:asset_classes:funds" && time_series.label("issuer") == "Tinkoff" {
        let mut time_series = time_series.clone();
        time_series.set_label("issuer", "T-Bank");
        return MigratedTimeSeries::Changed(time_series);
    }

    MigratedTimeSeries::Unchanged
}

#[allow(dead_code)]
fn migrate_example(time_series: &TimeSeries) -> MigratedTimeSeries {
    if time_series.name().starts_with("backup_") && time_series.label("job") == "node" && time_series.label("name") == "job" {
        return MigratedTimeSeries::Deleted;
    }

    if time_series.label("service").contains("-org.fedoraproject.SetroubleshootPrivileged@") {
        return MigratedTimeSeries::Deleted;
    }

    if time_series.label("job") == "node" && time_series.name() == "node_systemd_unit_state" {
        return MigratedTimeSeries::Deleted;
    }

    if time_series.label("job") == "node" && time_series.label("instance") != "proxy" && time_series.label("device").ends_with("md126") {
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
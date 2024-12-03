#[macro_use] mod core;
mod processor;
mod stat;
mod types;

use std::io::{self, Write};
use std::process::ExitCode;

use clap::{Arg, ArgAction, Command, value_parser};
use easy_logging::LoggingConfig;
use log::{Level, error};
use url::Url;

use crate::core::GenericResult;

fn main() -> ExitCode {
    let config = match parse_args() {
        Ok(config) => config,
        Err(err) => {
            let _ = writeln!(io::stderr(), "{err}.");
            return ExitCode::FAILURE;
        }
    };

    if let Err(err) = LoggingConfig::new(module_path!(), config.log_level).minimal().build() {
        let _ = writeln!(io::stderr(), "Failed to initialize the logging: {err}.");
        return ExitCode::FAILURE;
    }

    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic_hook(info);
        std::process::abort();
    }));

    if let Err(err) = processor::process(&config.source, config.target.as_ref()) {
        error!("{err}.");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}


struct Config {
    source: Url,
    target: Option<Url>,
    log_level: Level,
}

fn parse_args() -> GenericResult<Config> {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))

        .dont_collapse_args_in_usage(true)
        .disable_help_subcommand(true)
        .help_expected(true)

        .args([
            Arg::new("verbose")
                .short('v').long("verbose")
                .action(ArgAction::Count)
                .help("Set verbosity level"),

            Arg::new("source")
                .value_name("SOURCE")
                .required(true)
                .value_parser(value_parser!(Url))
                .help("Source VictoriaMetrics URL"),

            Arg::new("target")
                .value_name("TARGET")
                .value_parser(value_parser!(Url))
                .help("Target VictoriaMetrics URL"),
        ])

        .get_matches();

    let log_level = match matches.get_count("verbose") {
        0 => Level::Info,
        1 => Level::Debug,
        2 => Level::Trace,
        _ => return Err!("Invalid verbosity level"),
    };

    Ok(Config {
        source: matches.get_one("source").cloned().unwrap(),
        target: matches.get_one("target").cloned(),
        log_level,
    })
}
use std::collections::HashMap;
use std::io::{self, ErrorKind};

use futures_util::TryStreamExt;
use reqwest::{self, Client, ClientBuilder, Response};
use serde_derive::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;
use url::Url;

use crate::core::{EmptyResult, GenericResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeSeries {
    metric: HashMap<String, String>,
    values: Vec<Option<f64>>,
    timestamps: Vec<u64>,
}

#[tokio::main]
pub async fn process(source_url: &Url, target_url: &Url) -> EmptyResult {
    let source_stream = get_source_stream(source_url).await.map_err(|e| format!(
        "Failed to establish connection to source VictoriaMetrics: {e}"))?
        .bytes_stream().map_err(|e| io::Error::new(ErrorKind::Other, e));

    let mut source_lines = StreamReader::new(source_stream).lines();

    loop {
        let source_line = source_lines.next_line().await.map_err(|e| format!(
            "Source VictoriaMetrics connection error: {e}"))?;

        let Some(source_line) = source_line else {
            break;
        };

        // XXX(konishchev): HERE
        let time_series: TimeSeries = serde_json::from_str(&source_line).map_err(|e| format!(
            "Got an invalid time series ({e}): {source_line}"))?;
    }

    Ok(())
}

async fn get_source_stream(url: &Url) -> GenericResult<Response> {
    let mut url = url.join("/api/v1/export").map_err(|e| format!(
        "Invalid URL: {e}"))?;

    url.query_pairs_mut()
        // .append_pair("match", r#"{__name__="server_kernel_errors"}"#) // FIXME(konishchev): HERE
        .append_pair("match", r#"{__name__="server:uptime"}"#) // FIXME(konishchev): HERE
        .append_pair("reduce_mem_usage", "1");

    let response = new_client()?.get(url).send().await?;

    let status = response.status();
    if !status.is_success() {
        let message = response.text().await.unwrap_or_else(|e| e.to_string());
        return Err!("The server returned an error ({}): {}", status, message.trim());
    }

    Ok(response)
}

fn new_client() -> GenericResult<Client> {
    Ok(ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .no_brotli()
        .no_deflate()
        .no_gzip()
        .no_zstd()
        .build()?)
}
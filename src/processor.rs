use std::io::{self, ErrorKind};

use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::TryStreamExt;
use reqwest::{self, Body, Client, ClientBuilder, Response};
use tokio::pin;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;
use url::Url;

use crate::core::{EmptyResult, GenericResult};
use crate::stat::Stat;
use crate::types::TimeSeries;

#[tokio::main(flavor = "current_thread")]
pub async fn process(source_url: &Url, target_url: Option<&Url>) -> EmptyResult {
    let import_stream = get_import_stream(source_url).await;

    let Some(target_url) = target_url else {
        pin!(import_stream);
        while import_stream.try_next().await?.is_some() {
        }
        return Ok(());
    };

    let import_url = target_url.join("/api/v1/import").map_err(|e| format!(
        "Invalid URL: {e}"))?;

    let response = new_client()?.post(import_url).body(Body::wrap_stream(import_stream)).send().await.map_err(|e| {
        if e.is_connect() {
            format!("Failed to establish connection to target VictoriaMetrics: {e}")
        } else if e.is_body() {
            e.to_string()
        } else {
            format!("Target VictoriaMetrics connection error: {e}")
        }
    })?;

    let status = response.status();
    if !status.is_success() {
        let message = response.text().await.unwrap_or_else(|e| e.to_string());
        return Err!("Target VictoriaMetrics returned an error ({status}): {}", message.trim());
    }

    Ok(())
}

async fn get_import_stream(source_url: &Url) -> impl Stream<Item = GenericResult<Vec<u8>>> {
    let source_url = source_url.clone();

    try_stream! {
        let export_stream = get_export_stream(&source_url).await.map_err(|e| format!(
            "Failed to establish connection to source VictoriaMetrics: {e}"))?
            .bytes_stream().map_err(|e| io::Error::new(ErrorKind::Other, e));

        let mut stat = Stat::new();
        let mut export_lines = StreamReader::new(export_stream).lines();

        loop {
            let Some(export_line) = export_lines.next_line().await.map_err(|e| format!(
                "Source VictoriaMetrics connection error: {e}"
            ))? else {
                break;
            };

            let time_series: TimeSeries = serde_json::from_str(&export_line).map_err(|e| format!(
                "Got an invalid time series ({e}): {export_line}"))?;
            stat.add(&time_series);

            let mut buf = export_line.into_bytes();
            buf.truncate(0);

            serde_json::to_writer(&mut buf, &time_series).map_err(|e| format!(
                "Failed to serialize time series: {e}"))?;
            buf.push(b'\n');

            yield buf;
        }

        stat.print();
    }
}

async fn get_export_stream(source_url: &Url) -> GenericResult<Response> {
    let mut export_url = source_url.join("/api/v1/export").map_err(|e| format!(
        "Invalid URL: {e}"))?;

    use std::time::SystemTime;
    let start = format!("{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() - 60 * 60);

    export_url.query_pairs_mut()
        .append_pair("start", &start)
        // .append_pair("match", r#"{__name__="server_kernel_errors"}"#) // FIXME(konishchev): HERE
        // .append_pair("match", r#"{__name__="server:uptime"}"#) // FIXME(konishchev): HERE
        .append_pair("match", r#"{__name__!=""}"#) // FIXME(konishchev): HERE
        .append_pair("reduce_mem_usage", "1");

    let response = new_client()?.get(export_url).send().await?;

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
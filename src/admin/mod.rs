use crate::*;
use ratelimit::Ratelimiter;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::SystemTime;
use tiny_http::Method;
use tiny_http::Response;

/// The HTTP admin server.
pub async fn http(config: Config, ratelimit: Option<Arc<Ratelimiter>>) {
    let http_server = tokio::task::spawn_blocking(|| http_server(config, ratelimit));

    http_server.await.unwrap();
}

pub fn http_server(config: Config, ratelimit: Option<Arc<Ratelimiter>>) {
    let addr = config
        .general()
        .admin()
        .to_socket_addrs()
        .expect("bad listen address")
        .next()
        .expect("couldn't determine listen address");

    let mut previous: HashMap<Histograms, metriken::histogram::Snapshot> = HashMap::new();
    let mut current: HashMap<Histograms, metriken::histogram::Snapshot> = HashMap::new();
    let mut snapshot_at = SystemTime::now();

    let server = tiny_http::Server::http(addr).unwrap();

    loop {
        let now = SystemTime::now();

        if now >= snapshot_at {
            previous = current.clone();
            for metric in metriken::metrics().iter() {
                let any = if let Some(any) = metric.as_any() {
                    any
                } else {
                    continue;
                };

                if let Some(histogram) = any.downcast_ref::<AtomicHistogram>() {
                    if let Ok(key) = Histograms::try_from(metric.name()) {
                        if let Some(snapshot) = histogram.snapshot() {
                            current.insert(key, snapshot);
                        }
                    }
                }
            }

            snapshot_at = now + core::time::Duration::from_secs(1);
        }

        if let Some(request) = match server.try_recv() {
            Ok(rq) => rq,
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        } {
            let url = request.url();
            let parts: Vec<&str> = url.split('?').collect();
            let url = parts[0];

            match request.method() {
                Method::Get => match url {
                    "/metrics" => {
                        let _ = request
                            .respond(Response::from_string(prometheus_stats(&previous, &current)));
                    }
                    "/ratelimit" => {
                        let _ = request.respond(Response::empty(404));
                    }
                    "/vars" => {
                        let _ = request
                            .respond(Response::from_string(human_stats(&previous, &current)));
                    }
                    "/vars.json" | "/admin/metrics.json" | "/metrics.json" => {
                        let _ =
                            request.respond(Response::from_string(json_stats(&previous, &current)));
                    }
                    _ => {
                        let _ = request.respond(Response::empty(404));
                    }
                },
                Method::Put => {
                    if url.starts_with("/ratelimit/") {
                        if let Some(ref r) = ratelimit {
                            let parts: Vec<&str> = url.split('/').collect();

                            if parts.len() != 3 {
                                let _ = request.respond(Response::empty(500));
                            } else if let Ok(rate) = parts[2].parse::<u64>() {
                                let amount = (rate as f64 / 1_000_000.0).ceil() as u64;

                                // even though we might not have nanosecond level clock resolution,
                                // by using a nanosecond level duration, we achieve more accurate
                                // ratelimits.
                                let interval =
                                    Duration::from_nanos(1_000_000_000 / (rate / amount));

                                let capacity = std::cmp::max(100, amount);

                                r.set_max_tokens(capacity)
                                    .expect("failed to set max tokens");
                                r.set_refill_interval(interval)
                                    .expect("failed to set refill interval");
                                r.set_refill_amount(amount)
                                    .expect("failed to set refill amount");

                                let _ = request.respond(Response::empty(200));
                            } else {
                                let _ = request.respond(Response::empty(500));
                            }
                        } else {
                            let _ = request.respond(Response::empty(404));
                        }
                    } else {
                        let _ = request.respond(Response::empty(404));
                    }
                }
                _ => {
                    let _ = request.respond(Response::empty(500));
                }
            }
        }
    }
}

/// Produces Prometheus / OpenMetrics text format metrics. All metrics have
/// type information, some have descriptions as well. Percentiles read from
/// heatmaps are exposed with a `percentile` label where the value
/// corresponds to the percentile in the range of 0.0 - 100.0.
///
/// See: https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md
///
/// ```text
/// # TYPE some_counter counter
/// # HELP some_counter An unsigned 64bit monotonic counter.
/// counter 0
/// # TYPE some_gauge gauge
/// # HELP some_gauge A signed 64bit gauge.
/// some_gauge 0
/// # TYPE some_distribution{percentile="50.0"} gauge
/// some_distribution{percentile="50.0"} 0
/// ```
pub fn prometheus_stats(
    previous: &HashMap<Histograms, metriken::histogram::Snapshot>,
    current: &HashMap<Histograms, metriken::histogram::Snapshot>,
) -> String {
    let mut data = Vec::new();

    for metric in &metriken::metrics() {
        if metric.name().starts_with("log_") {
            continue;
        }

        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                continue;
            }
        };

        let name = metric.name();

        if let Some(counter) = any.downcast_ref::<Counter>() {
            let value = counter.value();
            if let Some(description) = metric.description() {
                data.push(format!(
                    "# TYPE {name} counter\n# HELP {name} {description}\n{name} {value}"
                ));
            } else {
                data.push(format!("# TYPE {name} counter\n{name} {value}"));
            }
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            let value = gauge.value();

            if let Some(description) = metric.description() {
                data.push(format!(
                    "# TYPE {name} gauge\n# HELP {name} {description}\n{name} {value}"
                ));
            } else {
                data.push(format!("# TYPE {name} gauge\n{name} {value}"));
            }
        } else if any.downcast_ref::<AtomicHistogram>().is_some() {
            let key = if let Ok(h) = Histograms::try_from(metric.name()) {
                h
            } else {
                continue;
            };

            let delta = match (current.get(&key), previous.get(&key)) {
                (Some(current), Some(previous)) => current.wrapping_sub(previous).unwrap(),
                (Some(current), None) => current.clone(),
                _ => {
                    continue;
                }
            };

            let percentiles: Vec<f64> = PERCENTILES.iter().map(|p| p.1).collect();

            let result = delta.percentiles(&percentiles).unwrap();

            let result: Vec<(&'static str, f64, u64)> = PERCENTILES
                .iter()
                .zip(result.iter())
                .map(|((label, percentile), (_, value))| (*label, *percentile, value.end()))
                .collect();

            for (_label, percentile, value) in result {
                if let Some(description) = metric.description() {
                    data.push(format!(
                        "# TYPE {name} gauge\n# HELP {name} {description}\n{name}{{percentile=\"{:02}\"}} {value}",
                        percentile,
                    ));
                } else {
                    data.push(format!(
                        "# TYPE {name} gauge\n{name}{{percentile=\"{:02}\"}} {value}",
                        percentile,
                    ));
                }
            }
        }
    }

    data.sort();
    let mut content = data.join("\n");
    content += "\n";
    let parts: Vec<&str> = content.split('/').collect();
    parts.join("_")
}

/// Produces JSON formatted metrics following the conventions of Finagle /
/// TwitterServer. Percentiles read from heatmaps will have a percentile
/// label appended to the metric name in the form `/p999` which would be the
/// 99.9th percentile.
///
/// ```text
/// {"get/ok": 0,"client/request/p999": 0, ... }
/// ```
pub fn json_stats(
    previous: &HashMap<Histograms, metriken::histogram::Snapshot>,
    current: &HashMap<Histograms, metriken::histogram::Snapshot>,
) -> String {
    let mut data = Vec::new();

    for metric in &metriken::metrics() {
        if metric.name().starts_with("log_") {
            continue;
        }

        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                continue;
            }
        };

        let name = metric.name();

        if let Some(counter) = any.downcast_ref::<Counter>() {
            let value = counter.value();

            data.push(format!("\"{name}\": {value}"));
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            let value = gauge.value();

            data.push(format!("\"{name}\": {value}"));
        } else if any.downcast_ref::<AtomicHistogram>().is_some() {
            let key = if let Ok(h) = Histograms::try_from(metric.name()) {
                h
            } else {
                continue;
            };

            let delta = match (current.get(&key), previous.get(&key)) {
                (Some(current), Some(previous)) => current.wrapping_sub(previous).unwrap(),
                (Some(current), None) => current.clone(),
                _ => {
                    continue;
                }
            };

            let percentiles: Vec<f64> = PERCENTILES.iter().map(|p| p.1).collect();

            let result = delta.percentiles(&percentiles).unwrap();

            let result: Vec<(&'static str, f64, u64)> = PERCENTILES
                .iter()
                .zip(result.iter())
                .map(|((label, percentile), (_, value))| (*label, *percentile, value.end()))
                .collect();

            for (label, _percentile, value) in result {
                data.push(format!("\"{name}/{label}\": {value}",));
            }
        }
    }

    data.sort();
    let mut content = "{".to_string();
    content += &data.join(",");
    content += "}";

    content
}

/// Produces human readable stats. One metric per line with a `LF` as the
/// newline character (Unix-style). Percentiles will have percentile labels
/// appened with a `/` as a separator.
///
/// ```
/// get/ok: 0
/// client/request/latency/p50: 0,
/// ```
pub fn human_stats(
    previous: &HashMap<Histograms, metriken::histogram::Snapshot>,
    current: &HashMap<Histograms, metriken::histogram::Snapshot>,
) -> String {
    let mut data = Vec::new();

    for metric in &metriken::metrics() {
        if metric.name().starts_with("log_") {
            continue;
        }

        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                continue;
            }
        };

        let name = metric.name();

        if let Some(counter) = any.downcast_ref::<Counter>() {
            let value = counter.value();

            data.push(format!("\"{name}\": {value}"));
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            let value = gauge.value();

            data.push(format!("\"{name}\": {value}"));
        } else if any.downcast_ref::<AtomicHistogram>().is_some() {
            let key = if let Ok(h) = Histograms::try_from(metric.name()) {
                h
            } else {
                continue;
            };

            let delta = match (current.get(&key), previous.get(&key)) {
                (Some(current), Some(previous)) => current.wrapping_sub(previous).unwrap(),
                (Some(current), None) => current.clone(),
                _ => {
                    continue;
                }
            };

            let percentiles: Vec<f64> = PERCENTILES.iter().map(|p| p.1).collect();

            let result = delta.percentiles(&percentiles).unwrap();

            let result: Vec<(&'static str, f64, u64)> = PERCENTILES
                .iter()
                .zip(result.iter())
                .map(|((label, percentile), (_, value))| (*label, *percentile, value.end()))
                .collect();

            for (label, _percentile, value) in result {
                data.push(format!("\"{name}/{label}\": {value}",));
            }
        }
    }

    data.sort();
    let mut content = data.join("\n");
    content += "\n";
    content
}

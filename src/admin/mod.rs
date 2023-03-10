// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use crate::*;
use ratelimit::Ratelimiter;
use std::sync::Arc;

/// The HTTP admin server.
pub async fn http(ratelimit: Option<Arc<Ratelimiter>>) {
    let admin = filters::admin(ratelimit);

    warp::serve(admin).run(([0, 0, 0, 0], 9091)).await;
}

mod filters {
    use super::*;

    /// The combined set of admin endpoint filters
    pub fn admin(
        ratelimit: Option<Arc<Ratelimiter>>,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        prometheus_stats()
            .or(human_stats())
            .or(update_ratelimit(ratelimit))
    }

    /// GET /metrics
    pub fn prometheus_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics")
            .and(warp::get())
            .and_then(handlers::prometheus_stats)
    }

    /// GET /vars
    pub fn human_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("vars")
            .and(warp::get())
            .and_then(handlers::human_stats)
    }

    // TODO(bmartin): we should probably pass the rate in the body
    /// PUT /ratelimit/:rate
    pub fn update_ratelimit(
        ratelimit: Option<Arc<Ratelimiter>>,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("ratelimit" / u64)
            .and(warp::put())
            .and(with_ratelimit(ratelimit))
            .and_then(handlers::update_ratelimit)
    }

    fn with_ratelimit(
        ratelimit: Option<Arc<Ratelimiter>>,
    ) -> impl Filter<Extract = (Option<Arc<Ratelimiter>>,), Error = std::convert::Infallible> + Clone
    {
        warp::any().map(move || ratelimit.clone())
    }
}

mod handlers {
    use super::*;
    use core::convert::Infallible;
    use warp::http::StatusCode;

    pub async fn prometheus_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        for metric in &metriken::metrics() {
            let any = match metric.as_any() {
                Some(any) => any,
                None => {
                    continue;
                }
            };

            if metric.name().starts_with("log_") {
                continue;
            }

            if let Some(counter) = any.downcast_ref::<Counter>() {
                data.push(format!(
                    "# TYPE {} counter\n{} {}",
                    metric.name(),
                    metric.name(),
                    counter.value()
                ));
            } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
                data.push(format!(
                    "# TYPE {} gauge\n{} {}",
                    metric.name(),
                    metric.name(),
                    gauge.value()
                ));
            } else if let Some(heatmap) = any.downcast_ref::<Heatmap>() {
                for (_label, percentile) in PERCENTILES {
                    let value = heatmap
                        .percentile(*percentile)
                        .map(|b| b.high())
                        .unwrap_or(0);
                    data.push(format!(
                        "# TYPE {} gauge\n{}{{percentile=\"{:02}\"}} {}",
                        metric.name(),
                        metric.name(),
                        percentile,
                        value
                    ));
                }
            }
        }

        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        let parts: Vec<&str> = content.split('/').collect();
        Ok(parts.join("_"))
    }

    pub async fn human_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        for metric in &metriken::metrics() {
            let any = match metric.as_any() {
                Some(any) => any,
                None => {
                    continue;
                }
            };

            if metric.name().starts_with("log_") {
                continue;
            }

            if let Some(counter) = any.downcast_ref::<Counter>() {
                data.push(format!("{}: {}", metric.name(), counter.value()));
            } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
                data.push(format!("{}: {}", metric.name(), gauge.value()));
            } else if let Some(heatmap) = any.downcast_ref::<Heatmap>() {
                for (label, p) in PERCENTILES {
                    let percentile = heatmap.percentile(*p).map(|b| b.high()).unwrap_or(0);
                    data.push(format!("{}/{}: {}", metric.name(), label, percentile));
                }
            }
        }

        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        Ok(content)
    }

    pub async fn update_ratelimit(
        rate: u64,
        ratelimit: Option<Arc<Ratelimiter>>,
    ) -> Result<impl warp::Reply, Infallible> {
        if let Some(r) = ratelimit {
            r.set_rate(rate);
            Ok(StatusCode::OK)
        } else {
            Ok(StatusCode::NOT_FOUND)
        }
    }
}

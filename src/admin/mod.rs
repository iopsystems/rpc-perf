use crate::*;
use ratelimit::Ratelimiter;
use std::net::ToSocketAddrs;
use std::sync::Arc;

/// The HTTP admin server.
pub async fn http(config: Config, ratelimit: Option<Arc<Ratelimiter>>) {
    let admin = filters::admin(ratelimit);

    let addr = config
        .general()
        .admin()
        .to_socket_addrs()
        .expect("bad listen address")
        .next()
        .expect("couldn't determine listen address");

    warp::serve(admin).run(addr).await;
}

mod filters {
    use super::*;

    /// The combined set of admin endpoint filters
    pub fn admin(
        ratelimit: Option<Arc<Ratelimiter>>,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        prometheus_stats()
            .or(human_stats())
            .or(json_stats())
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

    /// GET /metrics.json
    /// GET /vars.json
    /// GET /admin/metrics.json
    pub fn json_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics.json")
            .and(warp::get())
            .and_then(handlers::json_stats)
            .or(warp::path!("vars.json")
                .and(warp::get())
                .and_then(handlers::json_stats))
            .or(warp::path!("admin" / "metrics.json")
                .and(warp::get())
                .and_then(handlers::json_stats))
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

pub enum Metric<'a> {
    Counter(&'a str, u64),
    Gauge(&'a str, i64),
    Percentiles(&'a str, Vec<(&'a str, f64, Option<u64>)>),
}

impl<'a> Metric<'a> {
    pub fn name(&self) -> &'a str {
        match self {
            Self::Counter(name, _value) => name,
            Self::Gauge(name, _value) => name,
            Self::Percentiles(name, _percentiles) => name,
        }
    }
}

impl<'a> TryFrom<&'a metriken::MetricEntry> for Metric<'a> {
    type Error = ();

    fn try_from(metric: &'a metriken::MetricEntry) -> Result<Self, ()> {
        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                return Err(());
            }
        };

        if let Some(counter) = any.downcast_ref::<Counter>() {
            Ok(Metric::Counter((*metric).name(), counter.value()))
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            Ok(Metric::Gauge(metric.name(), gauge.value()))
        } else if let Some(heatmap) = any.downcast_ref::<Heatmap>() {
            let percentiles = PERCENTILES
                .iter()
                .map(|(label, percentile)| {
                    let value = heatmap.percentile(*percentile).map(|b| b.high()).ok();

                    (*label, *percentile, value)
                })
                .collect();

            Ok(Metric::Percentiles(metric.name(), percentiles))
        } else {
            Err(())
        }
    }
}

mod handlers {
    use super::*;
    use core::convert::Infallible;
    use warp::http::StatusCode;

    pub async fn prometheus_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        for metric in metriken::metrics()
            .iter()
            .map(Metric::try_from)
            .filter_map(|m| m.ok())
        {
            if metric.name().starts_with("log_") {
                continue;
            }

            match metric {
                Metric::Counter(name, value) => {
                    data.push(format!("# TYPE {name} counter\n{name} {value}"));
                }
                Metric::Gauge(name, value) => {
                    data.push(format!("# TYPE {name} gauge\n{name} {value}"));
                }
                Metric::Percentiles(name, percentiles) => {
                    for (_label, percentile, value) in percentiles {
                        if let Some(value) = value {
                            data.push(format!(
                                "# TYPE {name} gauge\n{name}{{percentile=\"{:02}\"}} {value}",
                                percentile,
                            ));
                        }
                    }
                }
            }
        }

        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        let parts: Vec<&str> = content.split('/').collect();
        Ok(parts.join("_"))
    }

    pub async fn json_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        for metric in metriken::metrics()
            .iter()
            .map(Metric::try_from)
            .filter_map(|m| m.ok())
        {
            if metric.name().starts_with("log_") {
                continue;
            }

            match metric {
                Metric::Counter(name, value) => {
                    data.push(format!("\"{name}\": {value}"));
                }
                Metric::Gauge(name, value) => {
                    data.push(format!("\"{name}\": {value}"));
                }
                Metric::Percentiles(name, percentiles) => {
                    for (label, _percentile, value) in percentiles {
                        if let Some(value) = value {
                            data.push(format!("\"{name}_{label}\": {value}",));
                        }
                    }
                }
            }
        }

        data.sort();
        let mut content = "{".to_string();
        content += &data.join(",");
        content += "}";

        Ok(content)
    }

    pub async fn human_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        for metric in metriken::metrics()
            .iter()
            .map(Metric::try_from)
            .filter_map(|m| m.ok())
        {
            if metric.name().starts_with("log_") {
                continue;
            }

            match metric {
                Metric::Counter(name, value) => {
                    data.push(format!("{name}: {value}"));
                }
                Metric::Gauge(name, value) => {
                    data.push(format!("{name}: {value}"));
                }
                Metric::Percentiles(name, percentiles) => {
                    for (label, _percentile, value) in percentiles {
                        if let Some(value) = value {
                            data.push(format!("{name}/{label}: {value}",));
                        }
                    }
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

// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::config_file::OutputFormat;
use crate::metrics::*;
use crate::Arc;
use crate::Config;
use heatmap::Heatmap;
use ratelimit::Ratelimiter;
use ringlog::Drain;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::time::Instant;
use waterfall::WaterfallBuilder;

use std::net::SocketAddr;
use std::time::Duration;
use tiny_http::{Method, Response, Server};

pub struct Admin {
    config: Option<Arc<Config>>,
    snapshot: Snapshot,
    connect_heatmap: Option<Arc<Heatmap>>,
    reconnect_ratelimit: Option<Arc<Ratelimiter>>,
    request_heatmap: Option<Arc<Heatmap>>,
    request_ratelimit: Option<Arc<Ratelimiter>>,
    request_waterfall: Option<Arc<Heatmap>>,
    server: Option<Server>,
    log: Box<dyn Drain>,
}

impl Admin {
    pub fn new(config: Arc<Config>, log: Box<dyn Drain>) -> Self {
        let snapshot = Snapshot::new(None, None);
        let server = config
            .general()
            .admin()
            .map(|admin_addr| Server::http(admin_addr).unwrap());

        Self {
            config: Some(config),
            snapshot,
            connect_heatmap: None,
            reconnect_ratelimit: None,
            request_heatmap: None,
            request_ratelimit: None,
            request_waterfall: None,
            server,
            log,
        }
    }

    pub fn for_replay(admin_addr: Option<SocketAddr>, log: Box<dyn Drain>) -> Self {
        let snapshot = Snapshot::new(None, None);
        let server = admin_addr.map(|admin_addr| Server::http(admin_addr).unwrap());

        Self {
            config: None,
            snapshot,
            connect_heatmap: None,
            reconnect_ratelimit: None,
            request_heatmap: None,
            request_ratelimit: None,
            request_waterfall: None,
            server,
            log,
        }
    }

    pub fn set_connect_heatmap(&mut self, heatmap: Option<Arc<Heatmap>>) {
        self.connect_heatmap = heatmap;
    }

    pub fn set_reconnect_ratelimit(&mut self, ratelimiter: Option<Arc<Ratelimiter>>) {
        self.reconnect_ratelimit = ratelimiter;
    }

    pub fn set_request_heatmap(&mut self, heatmap: Option<Arc<Heatmap>>) {
        self.request_heatmap = heatmap;
    }

    pub fn set_request_ratelimit(&mut self, ratelimiter: Option<Arc<Ratelimiter>>) {
        self.request_ratelimit = ratelimiter;
    }

    pub fn set_request_waterfall(&mut self, heatmap: Option<Arc<Heatmap>>) {
        self.request_waterfall = heatmap;
    }

    pub fn run(mut self) {
        let mut next = Instant::now()
            + match self.config.as_ref() {
                Some(config) => config.general().interval(),
                None => Duration::from_secs(60),
            };
        let mut snapshot =
            Snapshot::new(self.connect_heatmap.as_ref(), self.request_heatmap.as_ref());

        loop {
            while Instant::now() < next {
                clocksource::refresh_clock();
                let _ = self.log.flush();
                snapshot =
                    Snapshot::new(self.connect_heatmap.as_ref(), self.request_heatmap.as_ref());
                if let Some(ref server) = self.server {
                    while let Ok(Some(mut request)) = server.try_recv() {
                        let url = request.url();
                        let parts: Vec<&str> = url.split('?').collect();
                        let url = parts[0];
                        match request.method() {
                            Method::Get => match url {
                                "/" => {
                                    debug!("Serving GET on index");
                                    let _ = request.respond(Response::from_string(format!(
                                        "Welcome to {}\nVersion: {}\n",
                                        crate::config::NAME,
                                        crate::config::VERSION,
                                    )));
                                }
                                "/metrics" => {
                                    debug!("Serving Prometheus compatible stats");
                                    let _ = request
                                        .respond(Response::from_string(self.snapshot.prometheus()));
                                }
                                "/metrics.json" | "/vars.json" | "/admin/metrics.json" => {
                                    debug!("Serving machine readable stats");
                                    let _ = request
                                        .respond(Response::from_string(self.snapshot.json()));
                                }
                                "/vars" => {
                                    debug!("Serving human readable stats");
                                    let _ = request
                                        .respond(Response::from_string(self.snapshot.human()));
                                }
                                url => {
                                    debug!("GET on non-existent url: {}", url);
                                    debug!("Serving machine readable stats");
                                    let _ = request
                                        .respond(Response::from_string(self.snapshot.json()));
                                }
                            },
                            Method::Put => match request.url() {
                                "/ratelimit/reconnect" => {
                                    let mut content = String::new();
                                    request.as_reader().read_to_string(&mut content).unwrap();
                                    if let Ok(rate) = content.parse() {
                                        if let Some(ref ratelimiter) = self.reconnect_ratelimit {
                                            ratelimiter.set_rate(rate);
                                            let _ = request.respond(Response::empty(200));
                                        } else {
                                            let _ = request.respond(Response::empty(400));
                                        }
                                    } else {
                                        let _ = request.respond(Response::empty(400));
                                    }
                                }
                                "/ratelimit/request" => {
                                    let mut content = String::new();
                                    request.as_reader().read_to_string(&mut content).unwrap();
                                    if let Ok(rate) = content.parse() {
                                        if let Some(ref ratelimiter) = self.request_ratelimit {
                                            ratelimiter.set_rate(rate);
                                            let _ = request.respond(Response::empty(200));
                                        } else {
                                            let _ = request.respond(Response::empty(400));
                                        }
                                    } else {
                                        let _ = request.respond(Response::empty(400));
                                    }
                                }
                                url => {
                                    debug!("PUT on non-existent url: {}", url);
                                    let _ = request.respond(Response::empty(404));
                                }
                            },
                            method => {
                                debug!("unsupported request method: {}", method);
                                let _ = request.respond(Response::empty(404));
                            }
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            next += match self.config.as_ref() {
                Some(config) => config.general().interval(),
                None => Duration::from_secs(60),
            };

            let window = WINDOW.value();
            let output_format = self
                .config
                .as_deref()
                .map(|config| config.general().output_format())
                .unwrap_or_default();

            match output_format {
                OutputFormat::Log => self.emit_log(window, &snapshot),
                OutputFormat::Json => self.emit_json(window, &snapshot),
            }

            WINDOW.increment();
            self.snapshot = snapshot.clone();

            if let Some(max_window) = self
                .config
                .as_ref()
                .and_then(|config| config.general().windows())
            {
                if window >= max_window as u64 {
                    if let Some(ref heatmap) = self.request_waterfall {
                        if let Some(file) = self.config.as_ref().and_then(|c| c.waterfall().file())
                        {
                            let config = self.config.as_ref().unwrap();
                            let scale = config.waterfall().scale();
                            let palette = config.waterfall().palette();

                            WaterfallBuilder::new(&file)
                                .label(100, "100ns")
                                .label(1000, "1us")
                                .label(10000, "10us")
                                .label(100000, "100us")
                                .label(1000000, "1ms")
                                .label(10000000, "10ms")
                                .label(100000000, "100ms")
                                .scale(scale)
                                .palette(palette)
                                .build(&**heatmap);
                        }
                    }
                    break;
                }
            }
        }
    }

    fn emit_log(&self, window: u64, snapshot: &Snapshot) {
        info!("-----");
        info!("Window: {}", window);
        info!(
            "Connections: Attempts: {} Opened: {} Errors: {} Timeouts: {} Open: {}",
            snapshot.delta_count(&self.snapshot, CONNECT.name()),
            snapshot.delta_count(&self.snapshot, SESSION.name()),
            snapshot.delta_count(&self.snapshot, CONNECT_EX.name()),
            snapshot.delta_count(&self.snapshot, CONNECT_TIMEOUT.name()),
            OPEN.value()
        );

        let request_rate = snapshot.rate(&self.snapshot, REQUEST.name());
        let response_rate = snapshot.rate(&self.snapshot, RESPONSE.name());
        let connect_rate = snapshot.rate(&self.snapshot, CONNECT.name());

        info!(
            "Rate: Request: {:.2} rps Response: {:.2} rps Connect: {:.2} cps",
            request_rate, response_rate, connect_rate
        );

        let request_success =
            snapshot.success_rate(&self.snapshot, REQUEST.name(), REQUEST_EX.name());
        let response_success =
            snapshot.success_rate(&self.snapshot, RESPONSE.name(), RESPONSE_EX.name());
        let connect_success =
            snapshot.success_rate(&self.snapshot, CONNECT.name(), CONNECT_EX.name());

        info!(
            "Success: Request: {:.2} % Response: {:.2} % Connect: {:.2} %",
            request_success, response_success, connect_success
        );

        let hit_rate = snapshot.hitrate(&self.snapshot, REQUEST_GET.name(), RESPONSE_HIT.name());

        info!("Hit-rate: {:.2} %", hit_rate);

        if let Some(ref heatmap) = self.connect_heatmap {
            let p25 = heatmap.percentile(25.0).map(|b| b.high()).unwrap_or(0);
            let p50 = heatmap.percentile(50.0).map(|b| b.high()).unwrap_or(0);
            let p75 = heatmap.percentile(75.0).map(|b| b.high()).unwrap_or(0);
            let p90 = heatmap.percentile(90.0).map(|b| b.high()).unwrap_or(0);
            let p99 = heatmap.percentile(99.0).map(|b| b.high()).unwrap_or(0);
            let p999 = heatmap.percentile(99.9).map(|b| b.high()).unwrap_or(0);
            let p9999 = heatmap.percentile(99.99).map(|b| b.high()).unwrap_or(0);
            info!(
                "Connect Latency (us): p25: {} p50: {} p75: {} p90: {} p99: {} p999: {} p9999: {}",
                p25, p50, p75, p90, p99, p999, p9999
            );
        }

        if let Some(ref heatmap) = self.request_heatmap {
            let p25 = heatmap.percentile(25.0).map(|b| b.high()).unwrap_or(0);
            let p50 = heatmap.percentile(50.0).map(|b| b.high()).unwrap_or(0);
            let p75 = heatmap.percentile(75.0).map(|b| b.high()).unwrap_or(0);
            let p90 = heatmap.percentile(90.0).map(|b| b.high()).unwrap_or(0);
            let p99 = heatmap.percentile(99.0).map(|b| b.high()).unwrap_or(0);
            let p999 = heatmap.percentile(99.9).map(|b| b.high()).unwrap_or(0);
            let p9999 = heatmap.percentile(99.99).map(|b| b.high()).unwrap_or(0);
            info!(
                "Response Latency (us): p25: {} p50: {} p75: {} p90: {} p99: {} p999: {} p9999: {}",
                p25, p50, p75, p90, p99, p999, p9999
            );
        }
    }

    fn emit_json(&self, window: u64, snapshot: &Snapshot) {
        #[derive(Serialize)]
        struct Bucket {
            value: u64,
            count: u32,
        }

        #[derive(Serialize)]
        struct Connections {
            attempts: u64,
            opened: u64,
            errors: u64,
            timeouts: u64,
            open: i64,
        }

        #[derive(Serialize)]
        struct JsonSnapshot {
            window: u64,
            interval: f64,
            connections: Connections,
            request_count: u64,
            request_errors: u64,
            response_count: u64,
            response_errors: u64,
            connect_count: u64,
            connect_errors: u64,
            get_count: u64,
            hit_count: u64,

            connect: Vec<Bucket>,
            request: Vec<Bucket>,
        }

        fn heatmap_to_buckets(heatmap: &Heatmap) -> Vec<Bucket> {
            heatmap
                .summary()
                .into_iter()
                // Only include buckets that actually contain values
                .filter(|bucket| bucket.count() != 0)
                .map(|bucket| Bucket {
                    value: bucket.high(),
                    count: bucket.count(),
                })
                .collect()
        }

        let json = JsonSnapshot {
            connections: Connections {
                attempts: snapshot.delta_count(&self.snapshot, CONNECT.name()),
                opened: snapshot.delta_count(&self.snapshot, SESSION.name()),
                errors: snapshot.delta_count(&self.snapshot, CONNECT_EX.name()),
                timeouts: snapshot.delta_count(&self.snapshot, CONNECT_TIMEOUT.name()),
                open: OPEN.value(),
            },
            window,
            interval: (snapshot.timestamp - self.snapshot.timestamp).as_secs_f64(),
            request_count: snapshot.delta_count(&self.snapshot, REQUEST.name()),
            request_errors: snapshot.delta_count(&self.snapshot, REQUEST_EX.name()),
            response_count: snapshot.delta_count(&self.snapshot, RESPONSE.name()),
            response_errors: snapshot.delta_count(&self.snapshot, RESPONSE_EX.name()),
            connect_count: snapshot.delta_count(&self.snapshot, CONNECT.name()),
            connect_errors: snapshot.delta_count(&self.snapshot, CONNECT_EX.name()),
            get_count: snapshot.delta_count(&self.snapshot, REQUEST_GET.name()),
            hit_count: snapshot.delta_count(&self.snapshot, REQUEST_GET.name()),

            connect: self
                .connect_heatmap
                .as_deref()
                .map(|heatmap| heatmap_to_buckets(heatmap))
                .unwrap_or_default(),
            request: self
                .request_heatmap
                .as_deref()
                .map(|heatmap| heatmap_to_buckets(heatmap))
                .unwrap_or_default(),
        };

        println!(
            "{}",
            serde_json::to_string(&json).expect("Failed to output to stdout")
        );
    }
}

#[derive(Clone)]
pub struct Snapshot {
    counters: HashMap<&'static str, SnapshotEntry<u64>>,
    gauges: HashMap<&'static str, SnapshotEntry<i64>>,
    timestamp: Instant,
    connect_percentiles: Vec<(String, u64)>,
    request_percentiles: Vec<(String, u64)>,
}

#[derive(Clone)]
pub struct SnapshotEntry<T> {
    description: Option<&'static str>,
    value: T,
}

impl Snapshot {
    fn new(connect_heatmap: Option<&Arc<Heatmap>>, request_heatmap: Option<&Arc<Heatmap>>) -> Self {
        let mut counters = HashMap::new();
        let mut gauges = HashMap::new();
        for metric in metriken::metrics().static_metrics() {
            let any = match metric.as_any() {
                Some(any) => any,
                None => continue,
            };

            if let Some(counter) = any.downcast_ref::<Counter>() {
                let entry = SnapshotEntry {
                    description: metric.description(),
                    value: counter.value(),
                };
                counters.insert(metric.name(), entry);
            } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
                let entry = SnapshotEntry {
                    description: metric.description(),
                    value: gauge.value(),
                };
                gauges.insert(metric.name(), entry);
            }
        }

        let percentiles = vec![
            ("p25", 25.0),
            ("p50", 50.0),
            ("p75", 75.0),
            ("p90", 90.0),
            ("p99", 99.0),
            ("p999", 99.9),
            ("p9999", 99.99),
        ];

        let mut connect_percentiles = Vec::new();
        if let Some(heatmap) = connect_heatmap {
            for (label, value) in &percentiles {
                connect_percentiles.push((
                    label.to_string(),
                    heatmap.percentile(*value).map(|b| b.high()).unwrap_or(0),
                ));
            }
        }

        let mut request_percentiles = Vec::new();
        if let Some(heatmap) = request_heatmap {
            for (label, value) in &percentiles {
                request_percentiles.push((
                    label.to_string(),
                    heatmap.percentile(*value).map(|b| b.high()).unwrap_or(0),
                ));
            }
        }

        Self {
            counters,
            gauges,
            timestamp: Instant::now(),
            connect_percentiles,
            request_percentiles,
        }
    }

    fn delta_count(&self, other: &Self, counter: &'static str) -> u64 {
        let this = self.counters.get(&counter).map(|v| v.value).unwrap_or(0);
        let other = other.counters.get(&counter).map(|v| v.value).unwrap_or(0);
        this - other
    }

    fn rate(&self, other: &Self, counter: &'static str) -> f64 {
        let delta = self.delta_count(other, counter) as f64;
        let time = (self.timestamp - other.timestamp).as_secs_f64();
        delta / time
    }

    fn success_rate(&self, other: &Self, total: &'static str, error: &'static str) -> f64 {
        let total = self.rate(other, total);
        let error = self.rate(other, error);
        if total > 0.0 {
            100.0 - (100.0 * error / total)
        } else {
            100.0
        }
    }

    fn hitrate(&self, other: &Self, total: &'static str, hit: &'static str) -> f64 {
        let total = self.rate(other, total);
        let hit = self.rate(other, hit);
        if total > 0.0 {
            100.0 * hit / total
        } else {
            0.0
        }
    }

    pub fn human(&self) -> String {
        let mut data = Vec::new();
        for (counter, entry) in &self.counters {
            data.push(format!("{}: {}", counter, entry.value));
        }
        for (gauge, entry) in &self.gauges {
            data.push(format!("{}: {}", gauge, entry.value));
        }
        for (label, entry) in &self.connect_percentiles {
            data.push(format!("connect_latency/{}: {}", label, entry));
        }
        for (label, entry) in &self.request_percentiles {
            data.push(format!("response_latency/{}: {}", label, entry));
        }
        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        content
    }

    pub fn json(&self) -> String {
        let head = "{".to_owned();

        let mut data = Vec::new();
        for (label, entry) in &self.counters {
            data.push(format!("\"{}\": {}", label, entry.value));
        }
        for (label, entry) in &self.gauges {
            data.push(format!("\"{}\": {}", label, entry.value));
        }
        for (label, entry) in &self.connect_percentiles {
            data.push(format!("\"connect_latency/{}\": {}", label, entry));
        }
        for (label, entry) in &self.request_percentiles {
            data.push(format!("\"response_latency/{}\": {}", label, entry));
        }
        data.sort();
        let body = data.join(",");
        let mut content = head;
        content += &body;
        content += "}";
        content
    }

    pub fn prometheus(&self) -> String {
        let mut data = Vec::new();
        for (counter, entry) in &self.counters {
            if let Some(description) = entry.description {
                data.push(format!(
                    "# HELP {} {}\n# TYPE {} counter\n{} {}",
                    counter, description, counter, counter, entry.value
                ));
            } else {
                data.push(format!(
                    "# TYPE {} counter\n{} {}",
                    counter, counter, entry.value
                ));
            }
        }
        for (gauge, entry) in &self.gauges {
            if let Some(description) = entry.description {
                data.push(format!(
                    "# HELP {} {}\n# TYPE {} gauge\n{} {}",
                    gauge, description, gauge, gauge, entry.value
                ));
            } else {
                data.push(format!("# TYPE {} gauge\n{} {}", gauge, gauge, entry.value));
            }
        }
        for (percentile, entry) in &self.connect_percentiles {
            let label = "connect_latency";
            data.push(format!(
                "# TYPE {} gauge\n{}{{percentile=\"{}\"}} {}",
                label, label, percentile, entry
            ));
        }
        for (percentile, entry) in &self.request_percentiles {
            let label = "response_latency";
            data.push(format!(
                "# TYPE {} gauge\n{}{{percentile=\"{}\"}} {}",
                label, label, percentile, entry
            ));
        }
        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        let parts: Vec<&str> = content.split('/').collect();
        parts.join("_")
    }
}

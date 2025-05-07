use super::*;
use axum::handler::Handler;
use clap::ArgMatches;
use http::StatusCode;
use http::Uri;
use notify::Watcher;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use tower_http::services::{ServeDir, ServeFile};
use tower_livereload::LiveReloadLayer;

use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::decompression::RequestDecompressionLayer;

// const PERCENTILES: &[f64] = &[50.0, 90.0, 99.0, 99.9, 99.99];

const PERCENTILES: &[f64] = &[99.0];

mod tsdb;

use tsdb::*;

pub fn command() -> Command {
    Command::new("view")
        .about("View an RPC-Perf artifact")
        .arg(
            clap::Arg::new("INPUT")
                .help("RPC-Perf parquet file")
                .value_parser(value_parser!(PathBuf))
                .action(clap::ArgAction::Set)
                .required(true)
                .index(1),
        )
        .arg(
            clap::Arg::new("VERBOSE")
                .long("verbose")
                .short('v')
                .help("Increase the verbosity")
                .action(clap::ArgAction::Count),
        )
        .arg(
            clap::Arg::new("LISTEN")
                .help("Viewer listen address")
                .action(clap::ArgAction::Set)
                .value_parser(value_parser!(SocketAddr))
                .required(true)
                .index(2),
        )
        .arg(
            clap::Arg::new("TESTING")
                .long("testing")
                .short('t')
                .help("Use testing data")
                .action(clap::ArgAction::SetTrue),
        )
}

pub struct Config {
    input: PathBuf,
    verbose: u8,
    listen: SocketAddr,
    testing: bool,
}

impl TryFrom<ArgMatches> for Config {
    type Error = String;

    fn try_from(
        args: ArgMatches,
    ) -> Result<Self, <Self as std::convert::TryFrom<clap::ArgMatches>>::Error> {
        Ok(Config {
            input: args.get_one::<PathBuf>("INPUT").unwrap().to_path_buf(),
            verbose: *args.get_one::<u8>("VERBOSE").unwrap_or(&0),
            listen: *args.get_one::<SocketAddr>("LISTEN").unwrap(),
            testing: *args.get_one::<bool>("TESTING").unwrap_or(&false),
        })
    }
}

/// Runs the Rezolus exporter tool which is a Rezolus client that pulls data
/// from the msgpack endpoint and exports summary metrics on a Prometheus
/// compatible metrics endpoint. This allows for direct collection of percentile
/// metrics and/or full histograms with counter and gauge metrics passed through
/// directly.
pub fn run(config: Config) {
    // load config from file
    let config: Arc<Config> = config.into();

    // configure debug log
    let debug_output: Box<dyn Output> = Box::new(Stderr::new());

    let level = match config.verbose {
        0 => Level::Info,
        1 => Level::Debug,
        _ => Level::Trace,
    };

    let debug_log = if level <= Level::Info {
        LogBuilder::new().format(ringlog::default_format)
    } else {
        LogBuilder::new()
    }
    .output(debug_output)
    .build()
    .expect("failed to initialize debug log");

    let mut log = MultiLogBuilder::new()
        .level_filter(level.to_level_filter())
        .default(debug_log)
        .build()
        .start();
    // initialize async runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_name("rpc-perf")
        .build()
        .expect("failed to launch async runtime");

    // spawn logging thread
    rt.spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = log.flush();
        }
    });

    ctrlc::set_handler(move || {
        std::process::exit(2);
    })
    .expect("failed to set ctrl-c handler");

    // code to load data from parquet will go here
    let mut state = AppState::new(config.clone());

    let data = Tsdb::load(&config.input)
        .map_err(|e| {
            eprintln!("failed to load data from parquet: {e}");
            std::process::exit(1);
        })
        .unwrap();

    // define our sections
    let sections = vec![
        Section {
            name: "Overview".to_string(),
            route: "/overview".to_string(),
        },
        Section {
            name: "Cache".to_string(),
            route: "/cache".to_string(),
        },
    ];

    // define views for each section
    let mut overview = View::new(sections.clone());
    let mut cache = View::new(sections.clone());

    // Cache

    // Cache KPIs

    let mut cache_overview = Group::new("Cache", "cache");
    let mut cache_kpi = Group::new("KPIs", "kpis");

    let plot = Plot::line(
        "Request/s",
        "request-rate",
        Unit::Rate,
        data.counters("client/request/ok", Labels::default())
            .map(|v| v.rate().sum()),
    );
    cache_overview.push(plot.clone());
    cache_kpi.push(plot);

    let opts = PlotOpts::scatter("Response Latency (p99)", "response-latency", Unit::Time)
        .with_axis_label("Latency")
        .with_unit_system("time")
        .with_log_scale(true);
    let series = data.percentiles("response_latency:buckets", Labels::default());
    cache_overview.scatter(opts.clone(), series.clone());
    cache_kpi.scatter(opts.clone(), series.clone());



    overview.groups.push(cache_overview);
    cache.groups.push(cache_kpi);

    // Finalize

    state.sections.insert(
        "overview.json".to_string(),
        serde_json::to_string(&overview).unwrap(),
    );
    state
        .sections
        .insert("cache.json".to_string(), serde_json::to_string(&cache).unwrap());

    // launch the HTTP listener
    let c = config.clone();
    rt.block_on(async move { serve(c, state).await });

    std::thread::sleep(Duration::from_millis(200));
}

async fn serve(config: Arc<Config>, state: AppState) {
    let livereload = LiveReloadLayer::new();
    let reloader = livereload.reloader();

    let mut watcher = notify::recommended_watcher(move |_| reloader.reload())
        .expect("failed to initialize watcher");
    watcher
        .watch(
            Path::new("src/viewer/assets"),
            notify::RecursiveMode::Recursive,
        )
        .expect("failed to watch assets folder");

    let app = app(livereload, state);

    let listener = TcpListener::bind(config.listen)
        .await
        .expect("failed to listen");

    axum::serve(listener, app)
        .await
        .expect("failed to run http server");
}

struct AppState {
    config: Arc<Config>,
    sections: HashMap<String, String>,
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            sections: Default::default(),
        }
    }
}

// NOTE: we're going to want to include the assets in the binary for release
// builds. For now, we just serve from the assets folder
fn app(livereload: LiveReloadLayer, state: AppState) -> Router {
    let state = Arc::new(state);

    Router::new()
        .route_service("/", ServeFile::new("src/viewer/assets/index.html"))
        .route("/about", get(about))
        .with_state(state.clone())
        .nest_service("/lib", ServeDir::new(Path::new("src/viewer/assets/lib")))
        .nest_service("/data", data.with_state(state))
        .fallback_service(ServeFile::new("src/viewer/assets/index.html"))
        .layer(
            ServiceBuilder::new()
                .layer(RequestDecompressionLayer::new())
                .layer(CompressionLayer::new())
                .layer(livereload),
        )
}

// Basic /about page handler
async fn about() -> String {
    let version = env!("CARGO_PKG_VERSION");
    format!("RPC-Perf {version} Viewer\nFor information, see: https://rpc-perf.com\n")
}

async fn data(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    uri: Uri,
) -> (StatusCode, String) {
    let path = uri.path();
    let parts: Vec<&str> = path.split('/').collect();

    (
        StatusCode::OK,
        state
            .sections
            .get(parts[1])
            .map(|v| v.to_string())
            .unwrap_or("{ }".to_string()),
    )
}

#[derive(Default, Serialize)]
pub struct View {
    groups: Vec<Group>,
    sections: Vec<Section>,
}

impl View {
    pub fn new(sections: Vec<Section>) -> Self {
        Self {
            groups: Vec::new(),
            sections,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Section {
    name: String,
    route: String,
}

#[derive(Serialize)]
pub struct Group {
    name: String,
    id: String,
    plots: Vec<Plot>,
}

impl Group {
    pub fn new<T: Into<String>, U: Into<String>>(name: T, id: U) -> Self {
        Self {
            name: name.into(),
            id: id.into(),
            plots: Vec::new(),
        }
    }

    pub fn push(&mut self, plot: Option<Plot>) {
        if let Some(plot) = plot {
            self.plots.push(plot);
        }
    }

    pub fn plot(&mut self, opts: PlotOpts, series: Option<UntypedSeries>) {
        if let Some(data) = series.map(|v| v.as_data()) {
            self.plots.push(Plot {
                opts,
                data,
                min_value: None,
                max_value: None,
                time_data: None,
                formatted_time_data: None,
                series_names: None,
            })
        }
    }

    // New method to use the ECharts optimized heatmap data format
    pub fn heatmap_echarts(&mut self, opts: PlotOpts, series: Option<Heatmap>) {
        if let Some(heatmap) = series {
            let echarts_data = heatmap.as_data();
            // Only add if there's data
            if !echarts_data.data.is_empty() {
                self.plots.push(Plot {
                    opts,
                    data: echarts_data.data,
                    min_value: Some(echarts_data.min_value),
                    max_value: Some(echarts_data.max_value),
                    time_data: Some(echarts_data.time),
                    formatted_time_data: Some(echarts_data.formatted_time),
                    series_names: None,
                })
            }
        }
    }

    pub fn scatter(&mut self, opts: PlotOpts, data: Option<Vec<UntypedSeries>>) {
        if data.is_none() {
            return;
        }

        let d = data.unwrap();

        let mut data = Vec::new();

        for series in &d {
            let d = series.as_data();

            if data.is_empty() {
                data.push(d[0].clone());
            }

            data.push(d[1].clone());
        }

        self.plots.push(Plot {
            opts,
            data,
            min_value: None,
            max_value: None,
            time_data: None,
            formatted_time_data: None,
            series_names: None,
        })
    }

    // New method to add a multi-series plot
    pub fn multi(&mut self, opts: PlotOpts, cgroup_data: Option<Vec<(String, UntypedSeries)>>) {
        if cgroup_data.is_none() {
            return;
        }

        let mut cgroup_data = cgroup_data.unwrap();

        let mut data = Vec::new();
        let mut labels = Vec::new();

        for (label, series) in cgroup_data.drain(..) {
            labels.push(label);
            let d = series.as_data();

            if data.is_empty() {
                data.push(d[0].clone());
            }

            data.push(d[1].clone());
        }

        self.plots.push(Plot {
            opts,
            data,
            min_value: None,
            max_value: None,
            time_data: None,
            formatted_time_data: None,
            series_names: Some(labels),
        });
    }
}

#[derive(Serialize, Clone)]
pub struct Plot {
    data: Vec<Vec<f64>>,
    opts: PlotOpts,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_data: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    formatted_time_data: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    series_names: Option<Vec<String>>,
}

impl Plot {
    pub fn line<T: Into<String>, U: Into<String>>(
        title: T,
        id: U,
        unit: Unit,
        series: Option<UntypedSeries>,
    ) -> Option<Self> {
        series.map(|series| Self {
            data: series.as_data(),
            opts: PlotOpts::line(title, id, unit),
            min_value: None,
            max_value: None,
            time_data: None,
            formatted_time_data: None,
            series_names: None,
        })
    }

    pub fn heatmap<T: Into<String>, U: Into<String>>(
        title: T,
        id: U,
        unit: Unit,
        series: Option<Heatmap>,
    ) -> Option<Self> {
        if let Some(heatmap) = series {
            let echarts_data = heatmap.as_data();
            if !echarts_data.data.is_empty() {
                return Some(Plot {
                    opts: PlotOpts::heatmap(title, id, unit),
                    data: echarts_data.data,
                    min_value: Some(echarts_data.min_value),
                    max_value: Some(echarts_data.max_value),
                    time_data: Some(echarts_data.time),
                    formatted_time_data: Some(echarts_data.formatted_time),
                    series_names: None,
                });
            }
        }

        None
    }
}

#[derive(Serialize, Clone)]
pub struct PlotOpts {
    title: String,
    id: String,
    style: String,
    // Unified configuration for value formatting, axis labels, etc.
    format: Option<FormatConfig>,
}

#[derive(Serialize, Clone)]
pub struct FormatConfig {
    // Axis labels
    x_axis_label: Option<String>,
    y_axis_label: Option<String>,

    // Value formatting
    unit_system: Option<String>, // e.g., "percentage", "time", "bitrate"
    precision: Option<u8>,       // Number of decimal places

    // Scale configuration
    log_scale: Option<bool>, // Whether to use log scale for y-axis
    min: Option<f64>,        // Min value for y-axis
    max: Option<f64>,        // Max value for y-axis

    // Additional customization
    value_label: Option<String>, // Label used in tooltips for the value
}

impl PlotOpts {
    // Basic constructors without formatting
    pub fn line<T: Into<String>, U: Into<String>>(title: T, id: U, unit: Unit) -> Self {
        Self {
            title: title.into(),
            id: id.into(),
            style: "line".to_string(),
            format: Some(FormatConfig::new(unit)),
        }
    }

    pub fn multi<T: Into<String>, U: Into<String>>(title: T, id: U, unit: Unit) -> Self {
        Self {
            title: title.into(),
            id: id.into(),
            style: "multi".to_string(),
            format: Some(FormatConfig::new(unit)),
        }
    }

    pub fn scatter<T: Into<String>, U: Into<String>>(title: T, id: U, unit: Unit) -> Self {
        Self {
            title: title.into(),
            id: id.into(),
            style: "scatter".to_string(),
            format: Some(FormatConfig::new(unit)),
        }
    }

    pub fn heatmap<T: Into<String>, U: Into<String>>(title: T, id: U, unit: Unit) -> Self {
        Self {
            title: title.into(),
            id: id.into(),
            style: "heatmap".to_string(),
            format: Some(FormatConfig::new(unit)),
        }
    }

    // Convenience methods
    pub fn with_unit_system<T: Into<String>>(mut self, unit_system: T) -> Self {
        if let Some(ref mut format) = self.format {
            format.unit_system = Some(unit_system.into());
        }

        self
    }

    pub fn with_axis_label<T: Into<String>>(mut self, y_label: T) -> Self {
        if let Some(ref mut format) = self.format {
            format.y_axis_label = Some(y_label.into());
        }

        self
    }

    pub fn with_log_scale(mut self, log_scale: bool) -> Self {
        if let Some(ref mut format) = self.format {
            format.log_scale = Some(log_scale);
        }

        self
    }
}

impl FormatConfig {
    pub fn new(unit: Unit) -> Self {
        Self {
            x_axis_label: None,
            y_axis_label: None,
            unit_system: Some(unit.to_string()),
            precision: Some(2),
            log_scale: None,
            min: None,
            max: None,
            value_label: None,
        }
    }
}

pub enum Unit {
    Count,
    Rate,
    Time,
    Bytes,
    Datarate,
    Bitrate,
    Percentage,
    Frequency,
}

impl std::fmt::Display for Unit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let s = match self {
            Self::Count => "count",
            Self::Rate => "rate",
            Self::Time => "time",
            Self::Bytes => "bytes",
            Self::Datarate => "datarate",
            Self::Bitrate => "bitrate",
            Self::Percentage => "percentage",
            Self::Frequency => "frequency",
        };

        write!(f, "{s}")
    }
}

use super::*;
use metriken_exposition::ParquetHistogramType;
use serde::{Deserialize, Serialize};

use crate::General;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Json,
    MsgPack,
    Parquet,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HistogramType {
    #[default]
    Standard,
    Sparse,
}

impl From<HistogramType> for ParquetHistogramType {
    fn from(h: HistogramType) -> Self {
        match h {
            HistogramType::Standard => ParquetHistogramType::Standard,
            HistogramType::Sparse => ParquetHistogramType::Sparse,
        }
    }
}

pub fn interval() -> String {
    "100ms".into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct Metrics {
    /// File for output metrics
    output: String,
    /// Format for output metrics
    format: Format,
    /// Batch size for parquet files
    batch_size: Option<usize>,
    /// Type of histogram stored: sparse or dense
    #[serde(default)]
    histogram: HistogramType,
    /// The reporting interval. Specify time along with unit; default to 100ms.
    #[serde(default = "interval")]
    interval: String,
}

impl Metrics {
    pub fn from_general(general: &General) -> Option<Self> {
        Some(Metrics {
            output: general.metrics_output()?,
            format: general.metrics_format(),
            batch_size: None,
            histogram: HistogramType::default(),
            interval: general.metrics_interval().clone(),
        })
    }

    pub fn validate(&self, general: &General) {
        if let Some(x) = self.batch_size {
            if self.format != Format::Parquet {
                eprintln!("batch size is only valid for parquet files");
                std::process::exit(1);
            }

            // Allow values between 100 and 10M
            if !(100..=10_000_000).contains(&x) {
                eprintln!("batch size should be in range [0, 10,000,000]");
                std::process::exit(1);
            }
        }

        let interval = self.interval.parse::<humantime::Duration>();
        if let Err(e) = interval {
            eprintln!("metrics_interval is not valid: {e}");
            std::process::exit(1);
        }

        let interval = interval.unwrap().as_millis();
        if interval < Duration::from_millis(10).as_millis() {
            eprintln!("metrics_interval should be larger than 10ms");
            std::process::exit(1);
        }

        // Compare values between general and metrics section and warn if they
        // differ. Use the values from the metrics section if both are present.
        if let Some(x) = general.metrics_output() {
            if x != self.output {
                eprintln!("Output file in metrics ({}) and general ({}) section differ; using metrics value", self.output, x);
            }

            if self.format != general.metrics_format() {
                eprintln!(
                    "Output format in metrics and general section differ; using metrics value"
                );
            }

            if self.interval != *general.metrics_interval() {
                eprintln!(
                    "Interval in metrics ({}) and general ({}) section differ; using metrics value",
                    self.interval,
                    general.metrics_interval()
                );
            }
        }
    }

    pub fn output(&self) -> &String {
        &self.output
    }

    pub fn format(&self) -> Format {
        self.format
    }

    pub fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    pub fn histogram(&self) -> HistogramType {
        self.histogram
    }

    pub fn interval(&self) -> Duration {
        self.interval.parse::<humantime::Duration>().unwrap().into()
    }
}

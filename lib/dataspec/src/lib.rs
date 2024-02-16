//! Format of JSON output from rpc-perf. These structures can be used
//! by any consumer of the produced data to parse the files.
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::FileMetaData;
use histogram::SparseHistogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;

const PERCENTILES: &[&str] = &["p25", "p50", "p75", "p90", "p99", "p999", "p9999", "max"];

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Connections))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Connections {
    /// number of current connections (gauge)
    pub current: i64,
    /// number of total connect attempts
    pub total: u64,
    /// number of connections established
    pub opened: u64,
    /// number of connect attempts that failed
    pub error: u64,
    /// number of connect attempts that hit timeout
    pub timeout: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Requests))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Requests {
    pub total: u64,
    pub ok: u64,
    pub reconnect: u64,
    pub unsupported: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Responses))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Responses {
    /// total number of responses
    pub total: u64,
    /// number of responses that were successful
    pub ok: u64,
    /// number of responses that were unsuccessful
    pub error: u64,
    /// number of responses that were missed due to timeout
    pub timeout: u64,
    /// number of read requests with a hit response
    pub hit: u64,
    /// number of read requests with a miss response
    pub miss: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::ClientStats))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ClientStats {
    pub connections: Connections,
    pub requests: Requests,
    pub responses: Responses,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_latency: Option<SparseHistogram>,
    pub response_latency_percentiles: Vec<(String, f64, u64)>,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::PubsubStats))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PubsubStats {
    pub publishers: Publishers,
    pub subscribers: Subscribers,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_latency: Option<SparseHistogram>,
    pub publish_latency_percentiles: Vec<(String, f64, u64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_latency: Option<SparseHistogram>,
    pub total_latency_percentiles: Vec<(String, f64, u64)>,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Publishers))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Publishers {
    // current number of publishers
    pub current: i64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Subscribers))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Subscribers {
    // current number of subscribers
    pub current: i64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::JsonSnapshot))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct WindowStatistics {
    pub window: u64,
    pub elapsed: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_qps: Option<f64>,
    pub client: ClientStats,
    pub pubsub: PubsubStats,
}

impl WindowStatistics {
    fn u64_list_field(name: String, nullable: bool) -> Field {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            nullable,
        )
    }

    fn histogram_fields(fields: &mut Vec<Field>, name: &str) {
        let gp_name = name.to_owned() + "_grouping_power";
        let max_power_name = name.to_owned() + "_max_value_power";

        fields.push(Field::new(gp_name, DataType::UInt8, true));
        fields.push(Field::new(max_power_name, DataType::UInt8, true));
        fields.push(Self::u64_list_field(name.to_owned() + "_indices", true));
        fields.push(Self::u64_list_field(name.to_owned() + "_counts", true));
    }

    fn columnize_histograms(
        histograms: Vec<Option<SparseHistogram>>,
        columns: &mut Vec<Arc<dyn Array>>,
    ) {
        let mut grouping_powers: Vec<Option<u8>> = Vec::with_capacity(histograms.len());
        let mut max_value_powers: Vec<Option<u8>> = Vec::with_capacity(histograms.len());
        let mut bindex = ListBuilder::new(UInt64Builder::new());
        let mut bcount = ListBuilder::new(UInt64Builder::new());

        for h in histograms {
            if let Some(x) = h {
                grouping_powers.push(Some(x.config.grouping_power()));
                max_value_powers.push(Some(x.config.max_value_power()));

                bindex.append_value(
                    x.index
                        .into_iter()
                        .map(|e| Some(e as u64))
                        .collect::<Vec<_>>(),
                );
                bcount.append_value(x.count.into_iter().map(Some).collect::<Vec<_>>());
            } else {
                grouping_powers.push(None);
                max_value_powers.push(None);
                bindex.append_value(vec![None]);
                bcount.append_value(vec![None]);
            }
        }

        columns.push(Arc::new(UInt8Array::from(grouping_powers)));
        columns.push(Arc::new(UInt8Array::from(max_value_powers)));
        columns.push(Arc::new(bindex.finish()));
        columns.push(Arc::new(bcount.finish()));
    }

    fn percentiles_fields(fields: &mut Vec<Field>, name: &str) {
        for percentile in PERCENTILES {
            let field_name = name.to_owned() + "_" + percentile;
            fields.push(Field::new(field_name, DataType::UInt64, true));
        }
    }

    fn percentiles_row_to_column(
        column_map: &mut BTreeMap<String, Vec<Option<u64>>>,
        percentiles: &Vec<(String, f64, u64)>,
    ) {
        if percentiles.len() == PERCENTILES.len() {
            for percentile in percentiles {
                column_map
                    .entry(percentile.0.clone())
                    .or_default()
                    .push(Some(percentile.2));
            }
        } else {
            for percentile in PERCENTILES {
                column_map
                    .entry(percentile.to_string())
                    .or_default()
                    .push(None);
            }

            if !percentiles.is_empty() {
                eprintln!("Mismatch in metrics and dataspec for tracked percentiles");
            }
        }
    }

    fn columnize_percentiles(
        mut column_map: BTreeMap<String, Vec<Option<u64>>>,
        columns: &mut Vec<Arc<dyn Array>>,
    ) {
        for percentile in PERCENTILES {
            let key = percentile.to_string();
            let column = column_map.remove(&key).unwrap();
            columns.push(Arc::new(UInt64Array::from(column)));
        }
    }

    pub fn parquet_schema() -> SchemaRef {
        let mut fields: Vec<Field> = Vec::with_capacity(64);

        fields.push(Field::new("window", DataType::UInt64, false));
        fields.push(Field::new("elapsed", DataType::Float64, false));
        fields.push(Field::new("target_qps", DataType::Float64, true));

        fields.push(Field::new("connection_current", DataType::Int64, false));
        fields.push(Field::new("connection_total", DataType::UInt64, false));
        fields.push(Field::new("connection_opened", DataType::UInt64, false));
        fields.push(Field::new("connection_error", DataType::UInt64, false));
        fields.push(Field::new("connection_timeout", DataType::UInt64, false));

        fields.push(Field::new("request_total", DataType::UInt64, false));
        fields.push(Field::new("request_ok", DataType::UInt64, false));
        fields.push(Field::new("request_reconnect", DataType::UInt64, false));
        fields.push(Field::new("request_unsupported", DataType::UInt64, false));

        fields.push(Field::new("response_total", DataType::UInt64, false));
        fields.push(Field::new("response_ok", DataType::UInt64, false));
        fields.push(Field::new("response_error", DataType::UInt64, false));
        fields.push(Field::new("response_timeout", DataType::UInt64, false));
        fields.push(Field::new("response_hit", DataType::UInt64, false));
        fields.push(Field::new("response_miss", DataType::UInt64, false));
        Self::histogram_fields(&mut fields, "response_latency");
        Self::percentiles_fields(&mut fields, "response_latency");

        fields.push(Field::new("publisher_count", DataType::Int64, false));
        fields.push(Field::new("subscriber_count", DataType::Int64, false));
        Self::histogram_fields(&mut fields, "publish_latency");
        Self::percentiles_fields(&mut fields, "publish_latency");
        Self::histogram_fields(&mut fields, "total_latency");
        Self::percentiles_fields(&mut fields, "total_latency");

        Arc::new(Schema::new(fields))
    }

    pub fn try_new_parquet(
        path: &str,
        schema: SchemaRef,
    ) -> Result<ArrowWriter<File>, ParquetError> {
        let file = File::create(path)?;
        let opts = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)?))
            .build();

        ArrowWriter::try_new(file, schema, Some(opts))
    }

    pub fn finalize_parquet(writer: ArrowWriter<File>) -> Result<FileMetaData, ParquetError> {
        writer.close()
    }

    pub fn write_parquet_recordbatch(
        writer: &mut ArrowWriter<File>,
        schema: SchemaRef,
        window_stats: Vec<WindowStatistics>,
    ) -> Result<(), ParquetError> {
        let n = window_stats.len();
        let mut windows: Vec<u64> = Vec::with_capacity(n);
        let mut elapsed: Vec<f64> = Vec::with_capacity(n);
        let mut target_qps: Vec<Option<f64>> = Vec::with_capacity(n);

        let mut conn_current: Vec<i64> = Vec::with_capacity(n);
        let mut conn_total: Vec<u64> = Vec::with_capacity(n);
        let mut conn_opened: Vec<u64> = Vec::with_capacity(n);
        let mut conn_error: Vec<u64> = Vec::with_capacity(n);
        let mut conn_timeout: Vec<u64> = Vec::with_capacity(n);

        let mut req_total: Vec<u64> = Vec::with_capacity(n);
        let mut req_ok: Vec<u64> = Vec::with_capacity(n);
        let mut req_reconnect: Vec<u64> = Vec::with_capacity(n);
        let mut req_unsupported: Vec<u64> = Vec::with_capacity(n);

        let mut rsp_total: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_ok: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_error: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_timeout: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_hit: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_miss: Vec<u64> = Vec::with_capacity(n);
        let mut rsp_latency: Vec<Option<SparseHistogram>> = Vec::with_capacity(n);
        let mut rsp_latency_percentiles: BTreeMap<String, Vec<Option<u64>>> = BTreeMap::new();

        let mut pub_cnt: Vec<i64> = Vec::with_capacity(n);
        let mut sub_cnt: Vec<i64> = Vec::with_capacity(n);
        let mut publish_latency: Vec<Option<SparseHistogram>> = Vec::with_capacity(n);
        let mut publish_latency_percentiles: BTreeMap<String, Vec<Option<u64>>> = BTreeMap::new();
        let mut total_latency: Vec<Option<SparseHistogram>> = Vec::with_capacity(n);
        let mut total_latency_percentiles: BTreeMap<String, Vec<Option<u64>>> = BTreeMap::new();

        for ws in window_stats {
            windows.push(ws.window);
            elapsed.push(ws.elapsed);
            target_qps.push(ws.target_qps);

            conn_current.push(ws.client.connections.current);
            conn_total.push(ws.client.connections.total);
            conn_opened.push(ws.client.connections.opened);
            conn_error.push(ws.client.connections.error);
            conn_timeout.push(ws.client.connections.timeout);

            req_total.push(ws.client.requests.total);
            req_ok.push(ws.client.requests.ok);
            req_reconnect.push(ws.client.requests.reconnect);
            req_unsupported.push(ws.client.requests.unsupported);

            rsp_total.push(ws.client.responses.total);
            rsp_ok.push(ws.client.responses.ok);
            rsp_error.push(ws.client.responses.error);
            rsp_timeout.push(ws.client.responses.timeout);
            rsp_hit.push(ws.client.responses.hit);
            rsp_miss.push(ws.client.responses.miss);
            rsp_latency.push(ws.client.response_latency);
            Self::percentiles_row_to_column(
                &mut rsp_latency_percentiles,
                &ws.client.response_latency_percentiles,
            );

            pub_cnt.push(ws.pubsub.publishers.current);
            sub_cnt.push(ws.pubsub.subscribers.current);
            publish_latency.push(ws.pubsub.publish_latency);
            Self::percentiles_row_to_column(
                &mut publish_latency_percentiles,
                &ws.pubsub.publish_latency_percentiles,
            );
            total_latency.push(ws.pubsub.total_latency);
            Self::percentiles_row_to_column(
                &mut total_latency_percentiles,
                &ws.pubsub.total_latency_percentiles,
            );
        }

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
        columns.push(Arc::new(UInt64Array::from(windows)));
        columns.push(Arc::new(Float64Array::from(elapsed)));
        columns.push(Arc::new(Float64Array::from(target_qps)));

        columns.push(Arc::new(Int64Array::from(conn_current)));
        columns.push(Arc::new(UInt64Array::from(conn_total)));
        columns.push(Arc::new(UInt64Array::from(conn_opened)));
        columns.push(Arc::new(UInt64Array::from(conn_error)));
        columns.push(Arc::new(UInt64Array::from(conn_timeout)));

        columns.push(Arc::new(UInt64Array::from(req_total)));
        columns.push(Arc::new(UInt64Array::from(req_ok)));
        columns.push(Arc::new(UInt64Array::from(req_reconnect)));
        columns.push(Arc::new(UInt64Array::from(req_unsupported)));

        columns.push(Arc::new(UInt64Array::from(rsp_total)));
        columns.push(Arc::new(UInt64Array::from(rsp_ok)));
        columns.push(Arc::new(UInt64Array::from(rsp_error)));
        columns.push(Arc::new(UInt64Array::from(rsp_timeout)));
        columns.push(Arc::new(UInt64Array::from(rsp_hit)));
        columns.push(Arc::new(UInt64Array::from(rsp_miss)));
        Self::columnize_histograms(rsp_latency, &mut columns);
        Self::columnize_percentiles(rsp_latency_percentiles, &mut columns);

        columns.push(Arc::new(Int64Array::from(pub_cnt)));
        columns.push(Arc::new(Int64Array::from(sub_cnt)));
        Self::columnize_histograms(publish_latency, &mut columns);
        Self::columnize_percentiles(publish_latency_percentiles, &mut columns);
        Self::columnize_histograms(total_latency, &mut columns);
        Self::columnize_percentiles(total_latency_percentiles, &mut columns);

        let batch = RecordBatch::try_new(schema, columns)?;
        writer.write(&batch)
    }
}

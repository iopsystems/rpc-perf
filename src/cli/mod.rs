use crate::*;

#[macro_export]
macro_rules! output {
    () => {
        let now = clocksource::DateTime::now();
        println!("{}", now.to_rfc3339_opts(clocksource::SecondsFormat::Millis, false));
    };
    ($($arg:tt)*) => {{
        let now = clocksource::DateTime::now();
        println!("{} {}", now.to_rfc3339_opts(clocksource::SecondsFormat::Millis, false), format_args!($($arg)*));
    }};
}

pub fn output(config: &Config) {
    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

    let mut prev = Instant::now();

    while duration > 0 {
        std::thread::sleep(Duration::from_millis(10));

        interval = interval.saturating_sub(10);
        duration = duration.saturating_sub(10);

        if interval == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(prev).as_secs_f64();
            prev = now;

            let connect_ok = Stat::ConnectOk.delta(&mut snapshot);
            let connect_ex = Stat::ConnectEx.delta(&mut snapshot);
            let connect_timeout = Stat::ConnectTimeout.delta(&mut snapshot);
            let connect_total = Stat::Connect.delta(&mut snapshot);

            let request_reconnect = Stat::RequestReconnect.delta(&mut snapshot);
            let request_ok = Stat::RequestOk.delta(&mut snapshot);
            let request_unsupported = Stat::RequestUnsupported.delta(&mut snapshot);
            let request_total = Stat::Request.delta(&mut snapshot);

            let response_ok = Stat::ResponseOk.delta(&mut snapshot);
            let response_ex = Stat::ResponseEx.delta(&mut snapshot);
            let response_timeout = Stat::ResponseTimeout.delta(&mut snapshot);

            output!("-----");
            output!("Window: {}", window_id);

            let connect_sr = connect_ok as f64 / connect_total as f64;

            output!(
                "Connection: Open: {} Success Rate: {:.2} %",
                CONNECT_CURR.value(),
                connect_sr
            );
            output!(
                "Connection Rates (/s): Attempt: {:.2} Opened: {:.2} Errors: {:.2} Timeout: {:.2} Closed: {:.2}",
                connect_total as f64 / elapsed,
                connect_ok as f64 / elapsed,
                connect_ex as f64 / elapsed,
                connect_timeout as f64 / elapsed,
                request_reconnect as f64 / elapsed,
            );

            let request_sr = 100.0 * request_ok as f64 / request_total as f64;
            let request_ur = 100.0 * request_unsupported as f64 / request_total as f64;

            output!(
                "Request: Success: {:.2} % Unsupported: {:.2} %",
                request_sr,
                request_ur,
            );
            output!(
                "Request Rate (/s): Ok: {:.2} Unsupported: {:.2}",
                request_ok as f64 / elapsed,
                request_unsupported as f64 / elapsed,
            );

            let response_total = response_ok + response_ex + response_timeout;

            let response_sr = 100.0 * response_ok as f64 / response_total as f64;
            let response_to = 100.0 * response_timeout as f64 / response_total as f64;

            output!(
                "Response: Success: {:.2} % Timeout: {:.2} %",
                response_sr,
                response_to
            );
            output!(
                "Response Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
                response_ok as f64 / elapsed,
                response_ex as f64 / elapsed,
                response_timeout as f64 / elapsed,
            );

            let mut latencies = "response latency (us):".to_owned();
            for (label, percentile) in PERCENTILES {
                let value = RESPONSE_LATENCY
                    .percentile(*percentile)
                    .map(|b| format!("{}", b.high() / 1000))
                    .unwrap_or_else(|_| "ERR".to_string());
                latencies.push_str(&format!(" {label}: {value}"))
            }

            output!("{latencies}");

            interval = config.general().interval().as_millis();
            window_id += 1;
        }
    }
}

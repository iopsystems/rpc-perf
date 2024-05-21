use super::*;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists;
use rdkafka::Message;

fn get_client_config(config: &Config) -> ClientConfig {
    let bootstrap_servers = config.target().endpoints().join(",");
    let pubsub_config = config.pubsub().unwrap();
    let connect_timeout = format!("{}", pubsub_config.connect_timeout().as_millis());
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set("socket.timeout.ms", connect_timeout)
        .set("socket.nagle.disable", "true");
    if let Some(tls) = config.tls() {
        client_config
            .set("security.protocol", "ssl")
            .set("enable.ssl.certificate.verification", "false");
        if let Some(ca_file) = tls.ca_file() {
            client_config.set("ssl.ca.location", ca_file);
        }
        if let Some(private_key) = tls.private_key() {
            client_config.set("ssl.key.location", private_key);
            if let Some(password) = tls.private_key_password() {
                client_config.set("ssl.key.password", password);
            }
        }
        if let Some(cert) = tls.certificate() {
            client_config.set("ssl.certificate.location", cert);
        }
    }
    client_config
}

fn get_kafka_producer(config: &Config) -> FutureProducer {
    let pubsub_config = config.pubsub().unwrap();
    let publish_timeout = format!("{}", pubsub_config.publish_timeout().as_millis());
    let mut client_config = get_client_config(config);
    client_config.set("message.timeout.ms", publish_timeout);
    if let Some(acks) = pubsub_config.kafka_acks() {
        client_config.set("acks", acks);
    }
    if let Some(request_timeout_ms) = pubsub_config.kafka_request_timeout_ms() {
        client_config.set("request.timeout.ms", request_timeout_ms);
    }
    if let Some(linger_ms) = pubsub_config.kafka_linger_ms() {
        client_config.set("linger.ms", linger_ms);
    }
    if let Some(batch_size) = pubsub_config.kafka_batch_size() {
        client_config.set("batch.size", batch_size);
    }
    if let Some(batch_num_messages) = pubsub_config.kafka_batch_num_messages() {
        client_config.set("batch.num.messages", batch_num_messages);
    }
    if let Some(queue_buffering_max_messages) = pubsub_config.kafka_queue_buffering_max_messages() {
        client_config.set("queue.buffering.max.messages", queue_buffering_max_messages);
    }
    if let Some(queue_buffering_max_kbytes) = pubsub_config.kafka_queue_buffering_max_kbytes() {
        client_config.set("queue.buffering.max.kbytes", queue_buffering_max_kbytes);
    }
    if let Some(enable_idempotence) = pubsub_config.kafka_enable_idempotence() {
        client_config.set("enable.idempotence", enable_idempotence);
    }
    if let Some(max_in_flight) = pubsub_config.kafka_max_in_flight_requests_per_connection() {
        client_config.set("max.in.flight.requests.per.connection", max_in_flight);
    }
    if let Some(compression_type) = pubsub_config.kafka_compression_type() {
        client_config.set("compression.type", compression_type);
    }
    debug!("Kafka producer config: {:?}", client_config);
    client_config.create().unwrap()
}

fn get_kafka_consumer(config: &Config, group_id: &str) -> StreamConsumer {
    let pubsub_config: &Pubsub = config.pubsub().unwrap();
    let mut client_config = get_client_config(config);
    client_config
        .set("group.id", group_id)
        .set("client.id", "rpcperf_subscriber")
        .set("enable.auto.commit", "false");
    if let Some(auto_offset_reset) = pubsub_config.kafka_auto_offset_reset() {
        client_config.set("auto.offset.reset", auto_offset_reset);
    }
    if let Some(fetch_message_max_bytes) = pubsub_config.kafka_fetch_message_max_bytes() {
        client_config.set("fetch.message.max.bytes", fetch_message_max_bytes);
    }
    debug!("Kafka consumer config: {:?}", client_config);
    client_config.create().unwrap()
}

fn get_kafka_admin(config: &Config) -> AdminClient<DefaultClientContext> {
    get_client_config(config).create().unwrap()
}

fn validate_topic(
    runtime: &mut Runtime,
    config: &Config,
    topic: &str,
    partitions: usize,
    replications: usize,
) {
    let _guard = runtime.enter();
    let consumer_client = get_kafka_consumer(config, "topic_validator");
    let timeout = Some(Duration::from_secs(10));
    let metadata = consumer_client
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| e.to_string())
        .unwrap();
    if metadata.topics().is_empty() {
        eprintln!("Kafka topic validation failure: empty topic in metadata");
        std::process::exit(1);
    }
    let topic_partitions = metadata.topics()[0].partitions();
    let topic_partitions_size = metadata.topics()[0].partitions().len();
    if topic_partitions_size != partitions {
        eprintln!(
            "Kafka topic validation failure: asked {} partitions found {} in topic {}\nPlease delete or recreate the topic {}",
            partitions, topic_partitions_size, topic, topic
        );
        std::process::exit(1);
    }
    for partition in topic_partitions {
        let replicas_size = partition.replicas().len();
        if partition.replicas().len() != replications {
            eprintln!(
                "Kafka topic validation failure: asked {} replications found {} in topic {}\n Please delete or recreate the topic {}",
                replications, replicas_size, topic, topic
            );
            std::process::exit(1);
        }
    }
}

pub fn create_topics(runtime: &mut Runtime, config: Config, workload_components: &[Component]) {
    let admin_client = get_kafka_admin(&config);
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let partitions = topics.partitions();
            let replications = topics.replications();
            for topic in topics.topics() {
                match runtime.block_on(admin_client.create_topics(
                    &[NewTopic::new(
                        topic,
                        partitions as i32,
                        TopicReplication::Fixed(replications as i32),
                    )],
                    &AdminOptions::new(),
                )) {
                    Ok(topic_results) => {
                        for r in topic_results {
                            match r {
                                Ok(_) => {}
                                Err(err) => {
                                    if err.1 == TopicAlreadyExists {
                                        validate_topic(
                                            runtime,
                                            &config,
                                            topic,
                                            partitions,
                                            replications,
                                        );
                                    } else {
                                        eprintln!(
                                            "Kafka: failed to create the topic {}:{} ",
                                            err.0, err.1
                                        );
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("Kafka: no response when creating the topic ({})", err);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

/// Launch tasks with one channel per task as Kafka connection is mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: &[Component],
) {
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let poolsize = topics.subscriber_poolsize();
            let concurrency = topics.subscriber_concurrency();

            for id in 0..poolsize {
                let client = {
                    let _guard = runtime.enter();
                    // set the group_id to 0 for all subscribers if using the single subscriber group
                    let group_id = if topics.kafka_single_subscriber_group() {
                        0
                    } else {
                        id
                    };
                    Arc::new(get_kafka_consumer(
                        &config,
                        &format!("rpcperf_subscriber_{group_id}"),
                    ))
                };
                for _ in 0..concurrency {
                    let mut sub_topics: Vec<String> = Vec::new();
                    for t in topics.topics() {
                        sub_topics.push(t.to_string().clone())
                    }

                    runtime.spawn(subscriber_task(client.clone(), sub_topics));
                }
            }
        }
    }
}

async fn subscriber_task(client: Arc<StreamConsumer>, topics: Vec<String>) {
    PUBSUB_SUBSCRIBE.increment();

    let sub_topics: Vec<&str> = topics.iter().map(AsRef::as_ref).collect();

    if client.subscribe(&sub_topics).is_ok() {
        PUBSUB_SUBSCRIBER_CURR.add(1);
        PUBSUB_SUBSCRIBE_OK.increment();

        let validator = MessageValidator::new();

        while RUNNING.load(Ordering::Relaxed) {
            match client.recv().await {
                Ok(message) => match message.payload_view::<[u8]>() {
                    Some(Ok(message)) => {
                        let _ = validator.validate(&mut message.to_owned());
                    }
                    Some(Err(e)) => {
                        error!("Error in deserializing the message:{:?}", e);
                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_EX.increment();
                    }
                    None => {
                        error!("Empty Message");
                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_EX.increment();
                    }
                },
                Err(e) => {
                    debug!("Kafka Message Error {}", e);
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_EX.increment();
                }
            }
        }
    } else {
        error!("Failed to create subscriber");
        PUBSUB_SUBSCRIBE_EX.increment();
    }
}

/// Launch tasks with one channel per task as Kafka connection is mux-enabled.
pub fn launch_publishers(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    for _ in 0..config.pubsub().unwrap().publisher_poolsize() {
        let client = {
            let _guard = runtime.enter();
            Arc::new(get_kafka_producer(&config))
        };

        PUBSUB_PUBLISHER_CONNECT.increment();

        for _ in 0..config.pubsub().unwrap().publisher_concurrency() {
            runtime.spawn(publisher_task(client.clone(), work_receiver.clone()));
        }
    }
}

async fn publisher_task(
    client: Arc<FutureProducer>,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    PUBSUB_PUBLISHER_CURR.add(1);

    let validator = MessageValidator::new();

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        let start = Instant::now();

        let result = match work_item {
            WorkItem::Publish {
                topic,
                key,
                mut message,
            } => {
                validator.stamp(&mut message);
                PUBSUB_PUBLISH.increment();
                client
                    .send(
                        FutureRecord {
                            topic: &topic,
                            payload: Some(&message),
                            key: key.as_ref(),
                            partition: None,
                            timestamp: None,
                            headers: None,
                        },
                        Duration::from_secs(0),
                    )
                    .await
            }
        };

        let stop = Instant::now();

        match result {
            Ok(_) => {
                let latency = stop.duration_since(start).as_nanos() as u64;
                PUBSUB_PUBLISH_OK.increment();
                let _ = PUBSUB_PUBLISH_LATENCY.increment(latency);
            }
            Err(e) => {
                debug!("Error in producing: {:?}", e);
                PUBSUB_PUBLISH_EX.increment();
            }
        }
    }

    PUBSUB_PUBLISHER_CURR.sub(1);

    Ok(())
}

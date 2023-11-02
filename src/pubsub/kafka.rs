use super::*;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists;
use rdkafka::Message;
use std::sync::Arc;

fn get_kafka_producer(config: &Config) -> FutureProducer {
    let bootstrap_servers = config.target().endpoints().join(",");
    let pubsub_config = config.pubsub().unwrap();
    let publish_timeout = format!("{}", pubsub_config.publish_timeout().as_millis());
    let connect_timeout = format!("{}", pubsub_config.connect_timeout().as_millis());
    let pubsub_config = config.pubsub().unwrap();
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set("socket.timeout.ms", connect_timeout)
        .set("message.timeout.ms", publish_timeout);
    if let Some(acks) = pubsub_config.kafka_acks() {
        client_config.set("acks", acks);
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
    if let Some(request_timeout_ms) = pubsub_config.kafka_request_timeout_ms() {
        client_config.set("request.timeout.ms", request_timeout_ms);
    }
    client_config.create().unwrap()
}

fn get_kafka_consumer(config: &Config, group_id: &str) -> StreamConsumer {
    let bootstrap_servers = config.target().endpoints().join(",");
    let pubsub_config: &Pubsub = config.pubsub().unwrap();
    let connect_timeout = format!("{}", pubsub_config.connect_timeout().as_millis());
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("client.id", "rpcperf_subscriber")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("socket.timeout.ms", connect_timeout);
    if let Some(fetch_message_max_bytes) = pubsub_config.kafka_fetch_message_max_bytes() {
        client_config.set("fetch_message_max_bytes", fetch_message_max_bytes);
    }
    client_config.create().unwrap()
}

fn get_kafka_admin(config: &Config) -> AdminClient<DefaultClientContext> {
    let bootstrap_servers = config.target().endpoints().join(",");
    let connect_timeout = format!("{}", config.pubsub().unwrap().connect_timeout().as_millis());
    ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("client.id", "rpcperf_admin")
        .set("socket.timeout.ms", connect_timeout)
        .create()
        .unwrap()
}

fn validate_topic(runtime: &mut Runtime, config: &Config, topic: &str, partitions: usize) {
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
    let topic_partitions = metadata.topics()[0].partitions().len();
    if topic_partitions != partitions {
        eprintln!(
            "Kafka topic validation failure: asked {} partitions found {} in topic {}\nPlease delete or recreate the topic {}",
            partitions, topic_partitions, topic, topic
        );
        std::process::exit(1);
    }
}

pub fn create_topics(runtime: &mut Runtime, config: Config, workload_components: &[Component]) {
    let admin_client = get_kafka_admin(&config);
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let partitions = topics.partitions();
            for topic in topics.topics() {
                match runtime.block_on(admin_client.create_topics(
                    &[NewTopic::new(
                        topic,
                        partitions as i32,
                        TopicReplication::Fixed(1),
                    )],
                    &AdminOptions::new(),
                )) {
                    Ok(topic_results) => {
                        for r in topic_results {
                            match r {
                                Ok(_) => {}
                                Err(err) => {
                                    if err.1 == TopicAlreadyExists {
                                        validate_topic(runtime, &config, topic, partitions);
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
                    Arc::new(get_kafka_consumer(&config, &format!("rpcperf_subscriber_{id}")))
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
                partition,
                key,
                mut message,
            } => {
                let timestamp = validator.stamp(&mut message);
                PUBSUB_PUBLISH.increment();
                client
                    .send(
                        FutureRecord {
                            topic: &topic,
                            payload: Some(&message),
                            key: Some(&key),
                            partition: Some(partition as i32),
                            timestamp: Some(timestamp as i64),
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

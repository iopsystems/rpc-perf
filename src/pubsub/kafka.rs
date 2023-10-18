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
    let timeout = format!("{}", config.pubsub().unwrap().publish_timeout().as_millis());
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", timeout)
        .set("client.id", "rpcperf_publisher")
        .create()
        .unwrap();
    return client;
}

fn get_kafka_consumer(config: &Config, group_id: &str) -> StreamConsumer {
    let bootstrap_servers = config.target().endpoints().join(",");
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("client.id", "rpcperf_subscriber")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .unwrap();
    return client;
}

fn get_kafka_admin(config: &Config) -> AdminClient<DefaultClientContext> {
    let bootstrap_servers = config.target().endpoints().join(",");
    let timeout = format!("{}", config.pubsub().unwrap().publish_timeout().as_millis());
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", timeout)
        .set("client.id", "rpcperf_admin")
        .create()
        .unwrap();
    return client;
}

fn validate_partition(config: &Config, topic: &str, partitions: usize) {
    let consumer_client = get_kafka_consumer(&config, "topic_validator");
    let timeout = Some(Duration::from_secs(1));
    let metadata = consumer_client
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| e.to_string())
        .unwrap();
    if metadata.topics().len() == 0 {
        error!("Invalidated topic");
        std::process::exit(1);
    }
    let topic_partitions = metadata.topics()[0].partitions().len();
    if topic_partitions != partitions {
        error!(
            "Invalidated partition: asked {} found {}\n Please delete or recreate the topic {}",
            partitions, topic_partitions, topic
        );
        std::process::exit(1);
    }
}

pub fn create_topics(runtime: &mut Runtime, config: Config, workload_components: &Vec<Component>) {
    debug!("Create Kafka topics");
    let admin_client = get_kafka_admin(&config);
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let partitions = topics.partitions();
            //let admin_client: AdminClient<_> = consumer_config("create_topic", &bootstrap_servers, None).create().unwrap();
            for topic in topics.topics() {
                let topic_results = runtime
                    .block_on(admin_client.create_topics(
                        &[NewTopic::new(
                            topic,
                            partitions as i32,
                            TopicReplication::Fixed(1),
                        )],
                        &AdminOptions::new(),
                    ))
                    .unwrap();
                for r in topic_results {
                    match r {
                        Ok(ret) => {
                            debug!("Created topic {} returns {}", topic, ret);
                        }
                        Err(err) => {
                            if err.1 == TopicAlreadyExists {
                                validate_partition(&config, topic, partitions);
                                debug!("Topic {} exists and has {} partitions", topic, partitions);
                            } else {
                                error!("Failed to create the topic {}:{} ", err.0, err.1);
                                std::process::exit(1);
                            }
                        }
                    }
                }
            }
        }
    }
    debug!("Created Kafka topics");
}

/// Launch tasks with one channel per task as Kafka connection is mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: &Vec<Component>, /*  */
) {
    debug!("launching Kafka subscriber tasks");
    let group_id = "rpc_subscriber";
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let poolsize = topics.subscriber_poolsize();
            let concurrency = topics.subscriber_concurrency();

            for _ in 0..poolsize {
                let client = {
                    let _guard = runtime.enter();
                    Arc::new(get_kafka_consumer(&config, &group_id))
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
    debug!("Subscribe to topics {:?}", sub_topics);
    if let Ok(_) = client.subscribe(&sub_topics) {
        debug!("Subscribed to topic {:?}", topics);
        PUBSUB_SUBSCRIBER_CURR.add(1);
        PUBSUB_SUBSCRIBE_OK.increment();

        let msg_stamp = MessageStamp::new();

        while RUNNING.load(Ordering::Relaxed) {
            match client.recv().await {
                Ok(m) => {
                    match m.payload_view::<[u8]>() {
                        Some(Ok(m)) => {
                            let mut v = Vec::new();
                            v.extend_from_slice(m);

                            match msg_stamp.validate_msg(&mut v) {
                                MessageValidator::UnexpectedMessage => {
                                    error!("pubsub: invalid message received");
                                    RESPONSE_EX.increment();
                                    PUBSUB_RECEIVE_INVALID.increment();
                                    continue;
                                }
                                MessageValidator::CorruptedMessage => {
                                    error!("pubsub: corrupt message received");
                                    PUBSUB_RECEIVE.increment();
                                    PUBSUB_RECEIVE_CORRUPT.increment();
                                    continue;
                                }
                                MessageValidator::ValidatedMessage(latency, then) => {
                                    let _ = PUBSUB_LATENCY.increment(latency.as_nanos());
                                    debug!(
                                        "Recive one message with latency:{:?} and then:{:?}",
                                        latency, then
                                    );

                                    PUBSUB_RECEIVE.increment();
                                    PUBSUB_RECEIVE_OK.increment();
                                }
                            }
                        }
                        Some(Err(e)) => {
                            debug!("Error in deserializing the message:{:?}", e);
                            // unexpected message
                            PUBSUB_RECEIVE.increment();
                            PUBSUB_RECEIVE_EX.increment();
                        }
                        None => {
                            debug!("Empty Message");
                            // unexpected message
                            PUBSUB_RECEIVE.increment();
                            PUBSUB_RECEIVE_EX.increment();
                        }
                    }
                }
                Err(e) => {
                    debug!("Kafka Message Error {}", e);
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_EX.increment();
                }
            }
        }
    } else {
        debug!("Failed to create subscriber");
        PUBSUB_SUBSCRIBE_EX.increment();
    }
}

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_publishers(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching Kafka protocol publishers");
    //?    let _guard = runtime.enter();
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
    debug!("Producer started");
    let msg_stamp = MessageStamp::new();

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
                let timestamp = msg_stamp.stamp_msg(&mut message);
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
            Ok(a) => {
                let latency = stop.duration_since(start).as_nanos();

                PUBSUB_PUBLISH_OK.increment();
                let _ = PUBSUB_PUBLISH_LATENCY.increment(latency);
                debug!("One producing request {} {:?}", latency, a);
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

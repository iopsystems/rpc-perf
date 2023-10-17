use super::*;
// use ::momento::preview::topics::{SubscriptionItem, TopicClient, ValueKind};
// use ::momento::CredentialProviderBuilder;
use ahash::RandomState;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists;
use rdkafka::Message;
use std::sync::Arc;
use tokio::time::timeout;

pub fn get_kafka_producer(config: &Config) -> FutureProducer {
    let bootstrap_servers = config.target().endpoints().join(",");
    let timeout = format!("{}", config.pubsub().unwrap().publish_timeout().as_millis());
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", timeout)
        .create()
        .unwrap();
    return client;
}

pub fn get_kafka_consumer(config: &Config, group_id: &str) -> StreamConsumer {
    let bootstrap_servers = config.target().endpoints().join(",");    
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)        
        .set("group.id", group_id)        
        .set("client.id", "rdkafka_integration_test_client")
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


pub fn get_kafka_admin(config: &Config) -> AdminClient<DefaultClientContext> {
    let bootstrap_servers = config.target().endpoints().join(",");
    let timeout = format!("{}", config.pubsub().unwrap().publish_timeout().as_millis());
    // initialize the Momento topic client
    let client = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", timeout)
        .create()
        .unwrap();
    return client;
}

pub fn create_topics(runtime: &mut Runtime, config: Config, workload_components: &Vec<Component>) {
    debug!("Create Kafka topics");
    let admin_client = get_kafka_admin(&config);
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let poolsize = topics.subscriber_poolsize();
            let concurrency = topics.subscriber_concurrency();
            let partitions = topics.partitions();
            //let admin_client: AdminClient<_> = consumer_config("create_topic", &bootstrap_servers, None).create().unwrap();
            for topic in topics.topics() {
                let topic_results = runtime
                    .block_on(admin_client.create_topics(
                        &[NewTopic::new(
                            "hello",
                            partitions as i32,
                            TopicReplication::Fixed(1),
                        )],
                        &AdminOptions::new(),
                    ))
                    .unwrap();
                for r in topic_results {
                    match (r) {
                        Ok(ret) => {
                            debug!("Created topic {} returns {}", topic, ret);
                        }
                        Err(err) => {
                            if err.1 == TopicAlreadyExists {
                                debug!("Topic {} exists", topic);
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

/// Launch tasks with one channel per task as gRPC is mux-enabled.
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
    if let Ok(mut subscription) = client.subscribe(&sub_topics) {
        debug!("Subscribed to topic {:?}", topics);
        PUBSUB_SUBSCRIBER_CURR.add(1);
        PUBSUB_SUBSCRIBE_OK.increment();

        // Create a new hasher state to validate the integrity of received
        // messages. Deterministic seeds are used so that multiple processes can
        // verify the messages.
        let hash_builder = RandomState::with_seeds(
            0xd5b96f9126d61cee,
            0x50af85c9d1b6de70,
            0xbd7bdf2fee6d15b2,
            0x3dbe88bb183ac6f4,
        );

        while RUNNING.load(Ordering::Relaxed) {
            match client.recv().await {
                Ok(m) => {
                    match m.payload_view::<[u8]>() {
                        Some(Ok(m)) => {
                            debug!("Receive one Kafka message");
                            let now = Instant::now();
                            let now_unix = UnixInstant::now();                            
                            let mut v = Vec::new();
                            v.extend_from_slice(m);
//                            let mut v = m.copy_from_slice(src)
                            if [v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]
                                != [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21]
                            {
                                // unexpected message
                                error!("pubsub: invalid message received");
                                RESPONSE_EX.increment();
                                PUBSUB_RECEIVE_INVALID.increment();
                                continue;
                            }
                            
                            // grab the checksum and zero it in the message
                            let csum = [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]];
                            [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]] = [0; 8];

                            if csum != hash_builder.hash_one(&v).to_be_bytes() {
                                // corrupted message
                                error!("pubsub: corrupt message received");
                                PUBSUB_RECEIVE.increment();
                                PUBSUB_RECEIVE_CORRUPT.increment();
                                continue;
                            }

                            let ts = u64::from_be_bytes([
                                v[16], v[17], v[18], v[19], v[20], v[21], v[22], v[23],
                            ]);

                            let latency = now_unix - UnixInstant::from_nanos(ts);
                            let then = now - latency;

                            let _ = PUBSUB_LATENCY.increment(then, latency.as_nanos());
                            debug!("Recive one message with latency:{:?} and then:{:?}", latency, then);

                            PUBSUB_RECEIVE.increment();
                            PUBSUB_RECEIVE_OK.increment();
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
pub fn launch_publishers(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<WorkItem>,
    workload_components: &Vec<Component>,
) {
    debug!("launching Kafka protocol publishers");
    //?    let _guard = runtime.enter();
    for component in workload_components {
        if let Component::Topics(topics) = component {
            let poolsize = topics.publisher_poolsize();
            let concurrency = topics.publisher_concurrency();

            for _ in 0..poolsize {
                let client = {
                    let _guard = runtime.enter();
                    Arc::new(get_kafka_producer(&config))
                };
                for _ in 0..concurrency {
                    runtime.spawn(publisher_task(
                        config.clone(),
                        client.clone(),
                        work_receiver.clone(),
                    ));
                }
            }
        }
    }
}

async fn publisher_task(
    config: Config,
    // cache_name: String,
    client: Arc<FutureProducer>,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    PUBSUB_PUBLISHER_CURR.add(1);
    debug!("Producer started");
    // Create a new hasher state to validate the integrity of received
    // messages. Deterministic seeds are used so that multiple processes can
    // verify the messages.
    let hash_builder = RandomState::with_seeds(
        0xd5b96f9126d61cee,
        0x50af85c9d1b6de70,
        0xbd7bdf2fee6d15b2,
        0x3dbe88bb183ac6f4,
    );

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let now_unix = UnixInstant::now();
        let result = match work_item {
            WorkItem::KafkaMessage {
                topic,
                partition,
                mut key,
                mut message,
            } => {
                let timestamp = (now_unix - UnixInstant::from_nanos(0)).as_nanos();
                let ts = timestamp.to_be_bytes();
                // write the current unix time into the message
                [
                    message[16],
                    message[17],
                    message[18],
                    message[19],
                    message[20],
                    message[21],
                    message[22],
                    message[23],
                ] = ts;

                // todo, write a sequence number into the message

                // checksum the message and put the checksum into the message
                [
                    message[8],
                    message[9],
                    message[10],
                    message[11],
                    message[12],
                    message[13],
                    message[14],
                    message[15],
                ] = hash_builder.hash_one(&message).to_be_bytes();

                PUBSUB_PUBLISH.increment();
                //match timeout(
                // match config.pubsub().unwrap().publish_timeout(),
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
            _ => {
                continue;
            }
        };

        let stop = Instant::now();

        match result {
            Ok(_) => {
                let latency = stop.duration_since(start).as_nanos();

                PUBSUB_PUBLISH_OK.increment();
                let _ = PUBSUB_PUBLISH_LATENCY.increment(start, latency);
                debug!("One producing request {}", latency);
            }
            Err(_) => {
                PUBSUB_PUBLISH_EX.increment();
            }
        }
    }

    PUBSUB_PUBLISHER_CURR.sub(1);

    Ok(())
}

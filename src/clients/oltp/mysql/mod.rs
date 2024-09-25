use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::MySql;
use sqlx::QueryBuilder;
use sqlx::Error;
use crate::workload::*;
use crate::*;
use async_channel::Receiver;
use std::io::{Result};
use tokio::runtime::Runtime;

/// Launch tasks with one conncetion per task as ping protocol is not mux-enabled.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<OltpRequest>>,
) {
    info!("launching mysql protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
                
            ));
        }
    }
    
}

// a task for ping servers (eg: Pelikan Pingserver)
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<OltpRequest>>,
    endpoint: String,
    _config: Config,
) -> Result<()> {
    let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            OLTP_CONNECT.increment();
            connection = match MySqlConnection::connect(&endpoint).await {
                Ok(c) => {
                    OLTP_CONNECT_OK.increment();
                    OLTP_CONNECT_CURR.increment();
                    Some(c)
                }
                Err(e) => {
                    error!("error acquiring connection from pool: {e}");
                    None
                }
            };

            continue;
        };

        let mut c = connection.take().unwrap();

        let work_item = match work_receiver.recv().await {
            Ok(w) => w,
            Err(e) => {
                error!("error while attempting to receive work item: {e}");
                continue;
            }
        };

        match work_item {
            workload::ClientWorkItemKind::Request { request, .. } => match request {
                workload::OltpRequest::PointSelect(request) => {

                    OLTP_REQUEST_OK.increment();

                    let result = QueryBuilder::<MySql>::new(
                            &format!("SELECT c FROM {} WHERE id = {}", request.table, request.id)
                        )
                        .build()
                        .fetch_one(&mut c).await;

                    match result {
                        Ok(_) => {
                            OLTP_RESPONSE_OK.increment();
                        }
                        Err(Error::RowNotFound) => {
                            OLTP_RESPONSE_OK.increment();
                        }
                        Err(e) => {
                            debug!("request error: {e}");

                            OLTP_RESPONSE_EX.increment();
                            OLTP_CONNECT_CURR.decrement();
                            continue;
                        }
                    }
                }
            }
            workload::ClientWorkItemKind::Reconnect => {
                OLTP_CONNECT_CURR.decrement();
                continue;
            }
        }

        connection = Some(c);
    }

    Ok(())
}

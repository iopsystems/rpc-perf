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
    debug!("launching mysql protocol tasks");

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
            connection = match MySqlConnection::connect(&endpoint).await {
                Ok(c) => Some(c),
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

                    REQUEST_OK.increment();

                    let result = QueryBuilder::<MySql>::new(
                            "SELECT c FROM ? WHERE id = ?"
                        )
                        .push_bind(request.table)
                        .push_bind(request.id)
                        .build()
                        .fetch_one(&mut c).await;

                    match result {
                        Ok(_) => {
                            RESPONSE_OK.increment();
                        }
                        Err(Error::RowNotFound) => {
                            RESPONSE_OK.increment();
                        }
                        _ => {
                            continue;
                        }
                    }
                }
            }
            workload::ClientWorkItemKind::Reconnect => {
                continue;
            }
        }

        connection = Some(c);
    }

    Ok(())
}

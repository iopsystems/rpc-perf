use crate::workload::ClientWorkItemKind;
use crate::workload::OltpRequest;
use crate::*;
use async_channel::Receiver;
use tokio::runtime::Runtime;

mod mysql;

pub fn launch(
    config: &Config,
    work_receiver: Receiver<ClientWorkItemKind<OltpRequest>>,
) -> Option<Runtime> {
    if config.oltp().is_none() {
        debug!("No storage configuration specified");
        return None;
    }

    debug!("Launching clients...");

    config.client()?;

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.client().unwrap().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Mysql => mysql::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        protocol => {
            eprintln!("oltp is not supported for the {:?} protocol", protocol);
            std::process::exit(1);
        }
    }

    Some(client_rt)
}

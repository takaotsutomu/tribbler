use std::net::ToSocketAddrs;
use tonic::transport::Server;

use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

use crate::lab1::client::StorageClient;
use crate::lab1::server::StorageServer;

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let mut addr = config.addr.to_socker_addrs();
    if let Err(error) = addr {
        if let Some(ready) = config.ready {
            ready.send(false).unwrap();
        }
        return Err(Box::new(error)) // Box
    }
    let kvsrv = TribStorageServer::new(
        StorageServer {
            storage: config.storage
        }
    );
    Server::builder()
        .add_service(kvsrv)
        .serve_with_shutdown()
    if let Some(ready) = config.ready {
        ready.send(true).unwrap();
    }
    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    Ok(Box::new(StorageClient {
        addr: addr.to_string()
        client: Arc::new(tokio::sync::Mutex::new(None)),
     }))
}

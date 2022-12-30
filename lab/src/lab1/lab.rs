use std::net::ToSocketAddrs;
use tonic::transport::Server;

use tribbler::{
    config::BackConfig,
    err::TribResult,
    storage::Storage,
};

use crate::lab1::{
    client::StorageClient,
    server::StorageServer,
};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    // Resolve the address string to a SocketAddr value
    let mut addr_iter = match config.addr.to_socket_addrs() {
        Ok(addr_iter) => addr_iter,
        Err(error) => {
            if let Some(tx) = config.ready {
                tx.send(false).unwrap();
            }
            return Box::new(error);
        }
    } // TODO: use unwrap or else
    let addr = match addr_iter.next() {
        Some(addr) => addr,
        None => {
            if let Some(tx) = config.ready {
                tx.send(false).unwrap();
            }
            return Box::new(error);
        }
    }

    // Construct a new key-value service
    let kvsrv = TribStorageServer::new(
        StorageServer {
            storage: config.storage,
        }
    );

    // Expose the service at the SocketAddr address
    Server::builder()
        .add_service(kvsrv)
        .serve_with_shutdown(
            addr,
            async {
                if let Some(rx) = config.shutdown {
                    rx.recv().await;
                }
            },
        )
        .await;
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
        addr: addr.to_string(),
        client: Arc::new(tokio::sync::Mutex::new(None)),
     }))
}

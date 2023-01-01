use std::net::ToSocketAddrs;
use std::{error::Error, sync::Arc};
use tonic::transport::Server;

use tribbler::{
    config::BackConfig, err::TribResult, rpc::trib_storage_server::TribStorageServer,
    storage::Storage,
};

use crate::lab1::{client::StorageClient, server::StorageServer};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    // Resolve the address string to a SocketAddr value
    let mut addr_iter = match config.addr.to_socket_addrs() {
        Ok(addr_iter) => addr_iter,
        Err(error) => {
            if let Some(tx) = config.ready {
                if let Err(error) = tx.send(false) {
                    return Err(Box::new(error));
                }
            }
            return Err(Box::new(error));
        }
    };
    let addr = match addr_iter.next() {
        Some(addr) => addr,
        None => {
            if let Some(tx) = config.ready {
                if let Err(error) = tx.send(false) {
                    return Err(Box::new(error));
                }
            }
            return Err(Box::<dyn Error + Send + Sync>::from(
                "Error: Bad address".to_string(),
            ));
        }
    };

    // Create a new key-value server instance
    let kvsrv = TribStorageServer::new(StorageServer {
        storage: config.storage,
    });

    // Expose the service at the SocketAddr value
    match Server::builder()
        .add_service(kvsrv)
        .serve_with_shutdown(addr.clone(), async {
            if let Some(mut rx) = config.shutdown {
                rx.recv().await;
            }
        })
        .await
    {
        Ok(_) => {
            // Notify that the kv store is ready to serve
            if let Some(tx) = config.ready {
                if let Err(error) = tx.send(true) {
                    return Err(Box::new(error));
                }
            }
            println!("build up succeed! address,{} ", addr);
            Ok(())
        }
        Err(error) => {
            if let Some(tx) = config.ready {
                if let Err(error) = tx.send(false) {
                    return Err(Box::new(error));
                }
            }
            println!("build up failed! address,{} ", addr);
            Err(Box::new(error))
        }
    }
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

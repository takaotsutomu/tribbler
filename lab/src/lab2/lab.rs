use std::{
    error::Error,
    sync::{Arc, Mutex},
};
use tokio::{select, time};

use tribbler::{
    config::KeeperConfig,
    err::TribResult,
    storage::{BinStorage, Storage},
    trib::Server,
};

use crate::lab1::client::StorageClient;
use crate::lab2::{binstorage::BinStorageClient, frontserver::FrontServer};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient { backs }))
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let storages: Vec<StorageClient> = Vec::new();
    kc.backs.into_iter().for_each(|back| {
        storages.push(StorageClient {
            addr: format!("http://{}", back),
            client: Arc::new(tokio::sync::Mutex::new(None)),
        });
    });
    if let Some(tx) = kc.ready.clone() {
        if let Err(error) = tx.send(true) {
            return Err(Box::new(error));
        }
    }
    select! {
        _ = async {
            let max_timestamp: u64 = 0;
            loop {
                for stor in storages {
                    let clock = match stor.clock(max_timestamp).await {
                        Ok(clock) => clock,
                        Err(error) => {
                            if let Some(tx) = kc.ready.clone() {
                                if let Err(error) = tx.send(false) {
                                    return Err(Box::new(error));
                                }
                            }
                            return Err(Box::<dyn Error + Send + Sync>::from(error));
                        }
                    };
                    if clock > max_timestamp {
                        max_timestamp = clock;
                    }
                }
                // max_timestamp += 1;
                time::sleep(time::Duration::from_secs(1)).await;
            }
        } => {}
        _ = async {
            if let Some(mut rx) = kc.shutdown {
                rx.recv().await;
            }
        } => {}
    }
    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontServer {
        bin_storage,
        users_cache: Mutex::<Vec<String>>::new(vec![]),
    }))
}

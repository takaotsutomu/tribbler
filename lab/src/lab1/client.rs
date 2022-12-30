use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use tribbler::storage::{
    KeyString, 
    KeyList, 
    Storage, 
    KeyValue, 
    Pattern, 
    List
};
use tribbler::rpc::{
    trib_storage_client::TribStorageClient,
    Key, 
    KeyValue as RpcKeyValue,
    Pattern as RpcPattern,
    Clock,
};

pub struct StorageClient {
    pub(crate) addr: String,
    pub(crate) client: Arc<Mutex<<Option<TribStorageClient<Channel>>>>,
}

#[async_trait]
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let client = Arc::clone(&self.client) // for exp
        let cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .get(Key {key: key.to_string()})
            .await?;
        let value = response
            .into_inner()
            .value;
        if value.chars().count() == 0 {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .set(RpcKeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        Ok(response.into_inner().value)
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .keys(RpcPattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        Ok(List(response.into_inner().list))
    }
}

#[async_trait]
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .list_get(Key {
                key: key.to_string()
            })
            .await?;
        Ok(List(response.into_inner().list))
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .list_append(RpcKeyValue {
                key: key.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        Ok(response.into_inner().value)
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .list_remove(RpcKeyValue {
                key: key.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        Ok(response.into_inner().removed)
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .list_keys(RpcPattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        Ok(List(response.into_inner().list))
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let client = Arc::clone(&self.client)
        let mut cl_inner = client.lock().await;
        if cl_inner.is_none() {
            *cl_inner = Some(
                TribStorageClient::connect(self.addr.clone()).await?
            );
        }
        let response = cl_inner
            .as_mut()
            .unwrap()
            .clock(Clock {
                timestamp: at_least,
            })
            .await?;
        Ok(response.into_inner().timestamp)
    }
}
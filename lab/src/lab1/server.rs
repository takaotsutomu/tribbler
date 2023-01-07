use async_trait::async_trait;
use tonic;

use tribbler::rpc::{
    trib_storage_server::TribStorage, Bool as RpcBool, Clock as RpcClock, Key as RpcKey,
    KeyValue as RpcKeyValue, ListRemoveResponse as RpcListRemoveResponse, Pattern as RpcPattern,
    StringList as RpcStringList, Value as RpcValue,
};
use tribbler::storage::{KeyValue, List, Pattern, Storage};

pub struct StorageServer {
    pub(crate) storage: Box<dyn Storage>,
}

#[async_trait]
impl TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<RpcKey>,
    ) -> Result<tonic::Response<RpcValue>, tonic::Status> {
        match self.storage.get(&request.into_inner().key).await {
            Ok(Some(value)) => Ok(tonic::Response::new(RpcValue { value: value })),
            Ok(None) => Ok(tonic::Response::new(RpcValue {
                value: String::from(""),
            })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<RpcKeyValue>,
    ) -> Result<tonic::Response<RpcBool>, tonic::Status> {
        let rpc_kv = request.into_inner();
        match self
            .storage
            .set(&KeyValue {
                key: rpc_kv.key,
                value: rpc_kv.value,
            })
            .await
        {
            Ok(value) => Ok(tonic::Response::new(RpcBool { value: value })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn keys(
        &self,
        request: tonic::Request<RpcPattern>,
    ) -> Result<tonic::Response<RpcStringList>, tonic::Status> {
        let rpc_pat = request.into_inner();
        match self
            .storage
            .keys(&Pattern {
                prefix: rpc_pat.prefix,
                suffix: rpc_pat.suffix,
            })
            .await
        {
            Ok(List(list)) => Ok(tonic::Response::new(RpcStringList { list: list })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn list_get(
        &self,
        request: tonic::Request<RpcKey>,
    ) -> Result<tonic::Response<RpcStringList>, tonic::Status> {
        match self.storage.list_get(&request.into_inner().key).await {
            Ok(List(list)) => Ok(tonic::Response::new(RpcStringList { list: list })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn list_append(
        &self,
        request: tonic::Request<RpcKeyValue>,
    ) -> Result<tonic::Response<RpcBool>, tonic::Status> {
        let rpc_kv = request.into_inner();
        match self
            .storage
            .list_append(&KeyValue {
                key: rpc_kv.key,
                value: rpc_kv.value,
            })
            .await
        {
            Ok(value) => Ok(tonic::Response::new(RpcBool { value: value })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn list_remove(
        &self,
        request: tonic::Request<RpcKeyValue>,
    ) -> Result<tonic::Response<RpcListRemoveResponse>, tonic::Status> {
        let rpc_kv = request.into_inner();
        match self
            .storage
            .list_remove(&KeyValue {
                key: rpc_kv.key,
                value: rpc_kv.value,
            })
            .await
        {
            Ok(value) => Ok(tonic::Response::new(RpcListRemoveResponse {
                removed: value,
            })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn list_keys(
        &self,
        request: tonic::Request<RpcPattern>,
    ) -> Result<tonic::Response<RpcStringList>, tonic::Status> {
        let rpc_pat = request.into_inner();
        match self
            .storage
            .list_keys(&Pattern {
                prefix: rpc_pat.prefix,
                suffix: rpc_pat.suffix,
            })
            .await
        {
            Ok(List(list)) => Ok(tonic::Response::new(RpcStringList { list: list })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }

    async fn clock(
        &self,
        request: tonic::Request<RpcClock>,
    ) -> Result<tonic::Response<RpcClock>, tonic::Status> {
        match self.storage.clock(request.into_inner().timestamp).await {
            Ok(value) => Ok(tonic::Response::new(RpcClock { timestamp: value })),
            Err(error) => Err(tonic::Status::unknown(format!("Error: {}", error))),
        }
    }
}

use async_trait::async_trait;
use tonic;

use tribbler::rpc::{
    trib_storage_server::TribStorage,
    Key,
    Value,
    KeyValue,
    Bool,
    Pattern,
    StringList,
};
use tribbler::storage::{Storage, List};

pub struct StorageServer {
    pub(crate) storage: Box<dyn Storage>,
}

#[async_trait]
impl TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<Value>, tonic::Status> {
        match self
            .storage
            .get(request.into_inner().key)
            .await
        {
            Ok(Some(value)) => Ok(tonic::Response::new(
                Value {
                    value: value,
                }
            )),
            Ok(None) => Ok(tonic::Response::new(
                Value {
                    value: String::from(""),
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        match self
            .storage
            .set(&request.into_inner())
            .await
        {
            Ok(value) => Ok(tonic::Response::new(
                Bool {
                    value: value,
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        match self
            .storage
            .keys(&request.into_inner())
            .await
        {
            Ok(List(list)) => Ok(tonic::Response::new(
                StringList {
                    list: list, 
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn list_get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        match self
            .storage
            .list_get(&request.into_inner().key)
            .await
        {
            Ok(List(list)) => Ok(tonic::Response::new(
                StringList {
                    list: list,
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        };
    }

    async fn list_append(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        match self
            .storage
            .list_append(&request.into_inner())
            .await
        {
            Ok(value) => Ok(tonic::Response::new(
                Bool {
                    value: value,
            })),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn list_remove(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<ListRemoveResponse>, tonic::Status> {
        match self
            .storage
            .list_remove(&request.into_inner())
            .await
        {
            Ok(value) => Ok(tonic::Response::new(
                ListRemoveResponse {
                    removed: value,
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn list_keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        match self
            .storage
            .list_keys(&request.into_inner())
            .await
        {
            Ok(List(list)) => Ok(tonic::Response::new(
                StringList {
                    list: list,
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }

    async fn clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Clock>, tonic::Status> {
        match self
            .storage
            .clock(&request.into_inner().timestamp)
            .await
        {
            Ok(value) => Ok(tonic::Response::new(
                Clock {
                    timestamp: value,
                }
            )),
            Err(error) => tonic::Status::unknown(
                format!("Error: {}}", error),
            ),
        }
    }
}
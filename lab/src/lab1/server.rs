use async_trait::async_trait;
use tonic;

use tribbler::rpc::trib_storage_server::TribStorage;
use tribbler::rpc::{Key, Value, KeyValue, Bool, Pattern, StringList}
use tribbler::storage::Storage;

pub struct StorageServer {
    kvstore: Box<dyn Storage>,
}

impl StorageServer {
    pub fn new(storage: Box<dyn Storage>) -> StorageServer {
        StorageServer {
            kvstore: storage,
        }
    }
}

#[async_trait]
impl TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<Value>, tonic::Status> {
        let result = match self
            .kvstore
            .get(request.into_inner().key)
            .await
        {
            Ok(value) => value,
            Err(error) => return tonic::Status::new(
                tonic::Code::Unknown,
                format!("Error: {}}", error),
            ),
        }
        let response = match result {
            Some(value) => Value {
                value: value
            },
            None => Value {
                value: String::from("")
            },
        }
        Ok(tonic::Response::new(respones))
    }

    async fn set(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        let result = match self
            .kvstore
            .set(&request.into_inner())
            .await
        {
            Ok(value) => value,
            Err(error) => return tonic::Status::new(
                tonic::Code::Unknown,
                format!("Error: {}", error)
            ),
        }
        Ok(tonic::Response::new(Bool {
            value: result,
        }))
    }

    async fn keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        todo!()
    }

    async fn list_get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        todo!()
    }

    async fn list_append(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        todo!()
    }

    async fn list_remove(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<ListRemoveResponse>, tonic::Status> {
        todo!()
    }

    async fn list_keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        todo!()
    }

    async fn clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Clock>, tonic::Status> {
        todo!()
    }
}
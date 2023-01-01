use async_trait::async_trait;

use tribbler::{err::TribResult, storage::{KeyString, KeyList, Storage};

#[derive(Clone)]
pub struct TakaoStorageClient {
    pub(crate) name: String
}

#[async_trait]
impl KeyString for TakaoStorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        todo!()
    }

    async fn set(&self, kv: &)
}
use async_trait::async_trait;
use tribbler::{
    err::TribResult,
    storage::BinStorage
};

pub struct TakaoStorageServer {
    pub(crate) back_addrs: Vec<String>,
}

#[async_trait]
impl BinStorage for TakaoStorageServer {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        todo!();
    }
}
use async_trait::async_trait;

use tribbler::{
    err::TribResult,
    storage::BinStorage
};

use crate::colon;

pub struct BinStorageClient {
    pub(crate) backs: Vec<String>,
}

#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let mut kprefix = colon::escape(name.to_string());
        kprefix.push_str("::".to_string);
    }
}

pub struct Bin {
    pub(crate) name: String,
    pub(crate) kprefix: String,
    pub(crate) storage: StorageClient,
    // When a caller calls get("some-key") on the bin "alice", 
    // for example, the bin storage client translates this into
    // a get("alice::some-key") RPC call on back-end 0.
}

#[async_trait]
impl KeyString for Bin {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        todo!();
    }

    /// Set kv.key to kv.value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        todo!();
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        todo!();
    }
}

#[async_trait]
impl KeyList for Bin {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        todo!();
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        todo!();
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        todo!();
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        todo!();
    }

}

#[async_trait]
impl Storage for Bin {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        todo!();
    }
}
use async_trait::async_trait;
use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::Arc};

use tribbler::{
    colon,
    err::TribResult,
    storage::{BinStorage, KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

use crate::lab1::client::StorageClient;

pub struct BinStorageClient {
    // Addresses of the backend servers
    pub(crate) backs: Vec<String>,
}

#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let name = name.to_string();

        // Create the prefix ("{name}::") to translate the key
        // into a fully qualified one in a form of "{name}::{key}"
        let mut prefix = colon::escape(name.clone());
        prefix.push_str(&"::".to_string());

        // Determine the backend node will this bin belongs to
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        let hashcode = hasher.finish();
        let bk = &self.backs[(hashcode % self.backs.len() as u64) as usize];

        // Create a new storage client instance
        let storage = StorageClient {
            addr: format!("http://{}", bk.clone()),
            client: Arc::new(tokio::sync::Mutex::new(None)),
        };
        Ok(Box::new(Bin {
            _name: name,
            prefix,
            storage,
        }))
    }
}

pub struct Bin {
    _name: String,
    prefix: String,
    storage: StorageClient,
}

#[async_trait]
impl KeyString for Bin {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let key_esc = colon::escape(key.to_string());
        let mut key_escfq = self.prefix.clone();
        key_escfq.push_str(&key_esc);
        Ok(self.storage.get(&key_escfq).await?)
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let key_esc = colon::escape(&kv.key.to_string());
        let mut key_escfq = self.prefix.clone();
        key_escfq.push_str(&key_esc);
        Ok(self
            .storage
            .set(&KeyValue {
                key: key_escfq,
                value: kv.value.clone(),
            })
            .await?)
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let prefix_esc = colon::escape(p.prefix.clone());
        let mut prefix_escfq = self.prefix.clone();
        prefix_escfq.push_str(&prefix_esc);
        let suffix_esc = colon::escape(p.suffix.clone());
        let mut suffix_escfq = self.prefix.clone();
        suffix_escfq.push_str(&suffix_esc);
        let List(keys_escfq) = self
            .storage
            .keys(&Pattern {
                prefix: prefix_escfq,
                suffix: suffix_escfq,
            })
            .await?;
        let mut keys: Vec<String> = Vec::new();
        keys_escfq.into_iter().for_each(|kescfq| {
            let key_esc = String::from(&kescfq[self.prefix.len()..]);
            let key = colon::escape(key_esc);
            keys.push(key);
        });
        Ok(List(keys))
    }
}

#[async_trait]
impl KeyList for Bin {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let key_esc = colon::escape(key.to_string());
        let mut key_escfq = self.prefix.clone();
        key_escfq.push_str(&key_esc);
        Ok(self.storage.list_get(&key_escfq).await?)
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let key_esc = colon::escape(kv.key.to_string());
        let mut key_escfq = self.prefix.clone();
        key_escfq.push_str(&key_esc);
        Ok(self
            .storage
            .list_append(&KeyValue {
                key: key_escfq,
                value: kv.value.clone(),
            })
            .await?)
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let key_esc = colon::escape(kv.key.to_string());
        let mut key_escfq = self.prefix.clone();
        key_escfq.push_str(&key_esc);
        Ok(self
            .storage
            .list_remove(&KeyValue {
                key: key_escfq,
                value: kv.value.clone(),
            })
            .await?)
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let prefix_esc = colon::escape(p.prefix.clone());
        let mut prefix_escfq = self.prefix.clone();
        prefix_escfq.push_str(&prefix_esc);
        let suffix_esc = colon::escape(p.suffix.clone());
        let mut suffix_escfq = self.prefix.clone();
        suffix_escfq.push_str(&suffix_esc);
        let List(keys_fq) = self
            .storage
            .list_keys(&Pattern {
                prefix: prefix_escfq,
                suffix: suffix_escfq,
            })
            .await?;
        let mut keys: Vec<String> = Vec::new();
        keys_fq.into_iter().for_each(|kescfq| {
            let key_esc = String::from(&kescfq[self.prefix.len()..]);
            let key = colon::escape(key_esc);
            keys.push(key);
        });
        Ok(List(keys))
    }
}

#[async_trait]
impl Storage for Bin {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        Ok(self.storage.clock(at_least).await?)
    }
}

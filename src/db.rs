use bytes::Bytes;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone)]
pub struct ShardedDb {
    inner: Arc<Vec<Mutex<InnerDb>>>,
}

struct InnerDb {
    db: HashMap<String, Bytes>,
}

impl ShardedDb {
    pub fn new() -> Self {
        Self::new_sized(8)
    }

    pub fn new_sized(num_shards: usize) -> Self {
        let mut db_shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            db_shards.push(Mutex::new(InnerDb { db: HashMap::new() }));
        }

        ShardedDb {
            inner: Arc::new(db_shards),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let guard = self.guard(key);
        guard.db.get(key).cloned()
    }

    pub fn insert(&mut self, key: &str, value: Bytes) -> Option<Bytes> {
        let mut guard = self.guard(key);
        guard.db.insert(key.to_string(), value)
    }

    fn guard(&self, key: &str) -> MutexGuard<InnerDb> {
        let shard = Self::shard(key, self.inner.len());
        self.inner[shard].lock().unwrap()
    }

    fn shard(key: &str, num_shards: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % num_shards as u64) as usize
    }
}

impl Default for ShardedDb {
    fn default() -> Self {
        Self::new()
    }
}

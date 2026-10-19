use std::hash::{DefaultHasher, Hash, Hasher};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::database::{
    db::DB,
    types::{DatabaseEvent, HasKey},
};

pub struct Conductor {
    shards: Vec<mpsc::Sender<DatabaseEvent>>,
    shard_count: usize,
}

impl Conductor {
    pub async fn init(num_of_shards: usize, cancel_token: CancellationToken) -> Self {
        let mut shards = Vec::new();
        for n in 1..=num_of_shards {
            let (sender, receiver) = mpsc::channel(100);
            DB::new().init(receiver, cancel_token.clone(), n).await;
            shards.push(sender);
        }
        Self {
            shards,
            shard_count: num_of_shards,
        }
    }
    fn hash(&self, key: &Vec<u8>) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() % (self.shard_count as u64)) as usize
    }

    pub async fn dispatch(&self, e: DatabaseEvent) -> Result<()> {
        let key = e.get_key();
        let index = self.hash(key);
        self.shards[index].send(e).await?;
        Ok(())
    }
}

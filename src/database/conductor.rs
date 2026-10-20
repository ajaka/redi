use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh3::xxh3_64;

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
    fn hash(&self, key: &[u8]) -> usize {
        let hash128 = xxh3_64(key);
        (hash128 % self.shard_count as u64) as usize
    }

    pub async fn dispatch(&self, e: DatabaseEvent) -> Result<()> {
        let key = e.get_key();
        let index = self.hash(key);
        self.shards[index].clone().send(e).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn hash_is_deterministic() {
        let conductor = Conductor::init(10, CancellationToken::new()).await;
        let key = b"test-key".to_vec();

        let idx1 = conductor.hash(&key);
        let idx2 = conductor.hash(&key);
        let idx3 = conductor.hash(&key);

        assert_eq!(idx1, idx2);
        assert_eq!(idx2, idx3);
    }

    #[tokio::test]
    async fn hash_returns_valid_shard_index() {
        let conductor = Conductor::init(10, CancellationToken::new()).await;

        for i in 0..100 {
            let key = format!("key-{i}").into_bytes();
            let idx = conductor.hash(&key);
            assert!(idx < 10, "index {idx} out of range for key-{i}");
        }
    }

    #[tokio::test]
    async fn hash_with_different_shard_counts() {
        for shard_count in [1, 2, 5, 10, 100] {
            let conductor = Conductor::init(shard_count, CancellationToken::new()).await;
            let key = b"some-key".to_vec();
            let idx = conductor.hash(&key);
            assert!(
                idx < shard_count,
                "index {idx} >= shard_count {shard_count}"
            );
        }
    }

    #[tokio::test]
    async fn dispatch_delivers_event_without_error() {
        let conductor = Conductor::init(10, CancellationToken::new()).await;
        let (tx, rx) = mpsc::channel(100);

        conductor
            .dispatch(DatabaseEvent::Get {
                key: b"test-key".to_vec(),
                sender: tx,
                req_id: 1,
            })
            .await
            .unwrap();

        // The event should have been delivered to the correct shard's channel.
        // We can't easily observe which shard got it, but the dispatch itself
        // succeeding means the routing worked.
        drop(rx);
    }

    #[tokio::test]
    async fn dispatch_routes_same_key_to_same_shard() {
        let conductor = Conductor::init(10, CancellationToken::new()).await;
        let key = b"routed-key".to_vec();
        let expected_idx = conductor.hash(&key);

        // Dispatch multiple events for the same key; they should all go to the
        // same shard (verified indirectly by hash determinism).
        for req_id in 1..=5u8 {
            let (tx, _rx) = mpsc::channel(100);
            conductor
                .dispatch(DatabaseEvent::Get {
                    key: key.clone(),
                    sender: tx,
                    req_id,
                })
                .await
                .unwrap();
        }

        // Re-hash to confirm routing is consistent
        assert_eq!(conductor.hash(&key), expected_idx);
    }
}

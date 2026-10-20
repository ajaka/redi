use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::{
    common::types::CmdType,
    database::types::{DatabaseEvent, DatabaseResponse, DatabaseResponseEnum},
};

struct Entry {
    value: Vec<u8>,
    ttl: u128,
}
pub struct DB {
    db: BTreeMap<Vec<u8>, Entry>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            db: BTreeMap::new(),
        }
    }

    pub async fn init(
        mut self,
        mut recv: Receiver<DatabaseEvent>,
        cancel_token: CancellationToken,
        id: usize,
    ) {
        println!("Database shard with id {} is starting", id);
        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    biased;
                    msg = recv.recv() => match msg {
                        Some(event) => event,
                        None => break,
                    },
                    _ = cancel_token.cancelled() => {
                        println!("Database shard {} shutting down", id);
                        break;
                    },
                };
                if let Err(e) = self.handle_event(event).await {
                    eprintln!("An Error occured {}", e)
                }
            }
        });
    }
    pub async fn handle_event(&mut self, e: DatabaseEvent) -> Result<()> {
        match e {
            DatabaseEvent::Set {
                key,
                value,
                ttl,
                sender,
                req_id,
            } => self.insert(key, value, ttl, sender, req_id).await?,
            DatabaseEvent::SetNX {
                key,
                value,
                ttl,
                sender,
                req_id,
            } => self.insert_or_not(key, value, ttl, sender, req_id).await?,
            DatabaseEvent::Del {
                key,
                sender,
                req_id,
            } => self.delete(key, sender, req_id).await?,
            DatabaseEvent::Get {
                key,
                sender,
                req_id,
            } => self.get(key, sender, req_id).await?,
        }
        Ok(())
    }

    pub async fn insert_or_not(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    ) -> Result<()> {
        if ttl == 0 {
            sender
                .send(DatabaseResponse {
                    resp_type: CmdType::SetNx,
                    payload: DatabaseResponseEnum::SetResponse { ok: false, req_id },
                })
                .await?;
            return Ok(());
        }
        let now = self.get_now()?;
        let live = self.db.get(&key).is_some_and(|e| now < e.ttl);
        let resp = if live {
            DatabaseResponse {
                resp_type: CmdType::SetNx,
                payload: DatabaseResponseEnum::SetResponse { ok: false, req_id },
            }
        } else {
            let expires_in = self.get_expiry(ttl)?;
            let new_entry = Entry {
                value,
                ttl: expires_in,
            };
            self.db.insert(key, new_entry);
            DatabaseResponse {
                resp_type: CmdType::SetNx,
                payload: DatabaseResponseEnum::SetResponse { ok: true, req_id },
            }
        };
        sender.send(resp).await?;
        Ok(())
    }
    pub async fn insert(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    ) -> Result<()> {
        if ttl == 0 {
            sender
                .send(DatabaseResponse {
                    resp_type: CmdType::Set,
                    payload: DatabaseResponseEnum::SetResponse { ok: false, req_id },
                })
                .await?;
            return Ok(());
        }

        let expires_in = self.get_expiry(ttl)?;
        let new_entry = Entry {
            value,
            ttl: expires_in,
        };
        self.db.insert(key, new_entry);
        let resp = DatabaseResponse {
            resp_type: CmdType::Set,
            payload: DatabaseResponseEnum::SetResponse { ok: true, req_id },
        };
        sender.send(resp).await?;
        Ok(())
    }

    fn get_expiry(&self, ttl: u128) -> Result<u128> {
        let exp = self.get_now()?.saturating_add(ttl);
        Ok(exp)
    }

    fn get_now(&self) -> Result<u128> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(now)
    }

    pub async fn delete(
        &mut self,
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    ) -> Result<()> {
        let resp = match self.db.remove(&key) {
            Some(_) => DatabaseResponse {
                resp_type: CmdType::Del,
                payload: DatabaseResponseEnum::DelResponse { ok: true, req_id },
            },
            None => DatabaseResponse {
                resp_type: CmdType::Del,
                payload: DatabaseResponseEnum::DelResponse { ok: false, req_id },
            },
        };
        sender.send(resp).await?;
        Ok(())
    }
    pub async fn get(
        &mut self,
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    ) -> Result<()> {
        let resp = match self.db.get(&key) {
            None => DatabaseResponse {
                resp_type: CmdType::Get,
                payload: DatabaseResponseEnum::GetResponse {
                    ok: false,
                    value: vec![0u8; 0],
                    req_id,
                },
            },
            Some(v) => {
                let now = self.get_now()?;
                if now.ge(&v.ttl) {
                    self.db.remove(&key);
                    DatabaseResponse {
                        resp_type: CmdType::Get,
                        payload: DatabaseResponseEnum::GetResponse {
                            ok: false,
                            value: vec![0u8; 0],
                            req_id,
                        },
                    }
                } else {
                    DatabaseResponse {
                        resp_type: CmdType::Get,
                        payload: DatabaseResponseEnum::GetResponse {
                            ok: true,
                            value: v.value.clone(),
                            req_id,
                        },
                    }
                }
            }
        };
        sender.send(resp).await?;
        Ok(())
    }
}

impl Default for DB {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// Creates a DB and a response channel for testing.
    fn setup() -> (
        DB,
        mpsc::Sender<DatabaseResponse>,
        mpsc::Receiver<DatabaseResponse>,
    ) {
        let db = DB::new();
        let (tx, rx) = mpsc::channel(100);
        (db, tx, rx)
    }

    // ============================= SET (insert) =============================

    #[tokio::test]
    async fn set_basic() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.insert(b"key".to_vec(), b"value".to_vec(), 1_000_000_000, tx, 1)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        assert_eq!(resp.resp_type, CmdType::Set);
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(ok);
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn set_with_zero_ttl_rejected() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.insert(b"key".to_vec(), b"value".to_vec(), 0, tx, 1)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(!ok, "SET with TTL=0 must fail");
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }

        // Verify nothing was inserted by doing a GET
        let (tx2, mut rx2) = mpsc::channel(100);
        db.get(b"key".to_vec(), tx2, 2).await.unwrap();
        let resp = rx2.recv().await.unwrap();
        assert!(
            matches!(
                resp.payload,
                DatabaseResponseEnum::GetResponse { ok: false, .. }
            ),
            "key must not exist after rejected SET"
        );
    }

    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert(
            b"key".to_vec(),
            b"first".to_vec(),
            1_000_000_000,
            tx.clone(),
            1,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap();

        db.insert(b"key".to_vec(), b"second".to_vec(), 1_000_000_000, tx, 2)
            .await
            .unwrap();
        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::SetResponse { ok: true, .. }
        ));

        // Verify the value was overwritten
        let (tx2, mut rx2) = mpsc::channel(100);
        db.get(b"key".to_vec(), tx2, 3).await.unwrap();
        let resp = rx2.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::GetResponse { ok, value, req_id } => {
                assert!(ok);
                assert_eq!(value, b"second");
                assert_eq!(req_id, 3);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn set_with_very_large_ttl_succeeds() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.insert(b"key".to_vec(), b"value".to_vec(), u128::MAX, tx, 1)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::SetResponse { ok: true, .. }
        ));
    }

    // ========================== SETNX (insert_or_not) ==========================

    #[tokio::test]
    async fn setnx_on_missing_key_succeeds() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.insert_or_not(b"key".to_vec(), b"value".to_vec(), 1_000_000_000, tx, 1)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(ok);
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn setnx_on_existing_live_key_fails() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert_or_not(
            b"key".to_vec(),
            b"first".to_vec(),
            1_000_000_000,
            tx.clone(),
            1,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap();

        db.insert_or_not(b"key".to_vec(), b"second".to_vec(), 1_000_000_000, tx, 2)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(!ok, "SETNX on live key must fail");
                assert_eq!(req_id, 2);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn setnx_on_expired_key_succeeds() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        // Insert with TTL=1 nanosecond (will expire almost immediately)
        db.insert_or_not(b"key".to_vec(), b"first".to_vec(), 1, tx.clone(), 1)
            .await
            .unwrap();
        rx.recv().await.unwrap();

        // Wait for expiry
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        db.insert_or_not(b"key".to_vec(), b"second".to_vec(), 1_000_000_000, tx, 2)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(ok, "SETNX on expired key must succeed");
                assert_eq!(req_id, 2);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn setnx_with_zero_ttl_rejected() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.insert_or_not(b"key".to_vec(), b"value".to_vec(), 0, tx, 1)
            .await
            .unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::SetResponse { ok, req_id } => {
                assert!(!ok, "SETNX with TTL=0 must fail even if key is absent");
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }
    }

    // ================================= GET =================================

    #[tokio::test]
    async fn get_missing_key() {
        let (db, tx, mut rx) = setup();
        let mut db = db;
        db.get(b"missing".to_vec(), tx, 1).await.unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::GetResponse { ok, value, req_id } => {
                assert!(!ok);
                assert!(value.is_empty());
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn get_existing_key() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert(
            b"key".to_vec(),
            b"value".to_vec(),
            1_000_000_000,
            tx.clone(),
            1,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap();

        db.get(b"key".to_vec(), tx, 2).await.unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::GetResponse { ok, value, req_id } => {
                assert!(ok);
                assert_eq!(value, b"value");
                assert_eq!(req_id, 2);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn get_expired_key_returns_not_found() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert(b"key".to_vec(), b"value".to_vec(), 1, tx.clone(), 1)
            .await
            .unwrap();
        rx.recv().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        db.get(b"key".to_vec(), tx, 2).await.unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::GetResponse { ok, value, req_id } => {
                assert!(!ok, "expired key must return ok:false");
                assert!(value.is_empty());
                assert_eq!(req_id, 2);
            }
            _ => panic!("wrong payload type"),
        }
    }

    #[tokio::test]
    async fn get_expired_key_removes_it() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert(b"key".to_vec(), b"value".to_vec(), 1, tx.clone(), 1)
            .await
            .unwrap();
        rx.recv().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // First GET: should return expired
        db.get(b"key".to_vec(), tx.clone(), 2).await.unwrap();
        let resp = rx.recv().await.unwrap();
        assert!(!matches!(
            resp.payload,
            DatabaseResponseEnum::GetResponse { ok: true, .. }
        ));

        // Second GET: should also return not-found (entry was removed)
        db.get(b"key".to_vec(), tx, 3).await.unwrap();
        let resp = rx.recv().await.unwrap();
        assert!(!matches!(
            resp.payload,
            DatabaseResponseEnum::GetResponse { ok: true, .. }
        ));
    }

    // ================================= DEL =================================

    #[tokio::test]
    async fn del_existing_key() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.insert(
            b"key".to_vec(),
            b"value".to_vec(),
            1_000_000_000,
            tx.clone(),
            1,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap();

        db.delete(b"key".to_vec(), tx, 2).await.unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::DelResponse { ok, req_id } => {
                assert!(ok);
                assert_eq!(req_id, 2);
            }
            _ => panic!("wrong payload type"),
        }

        // Verify it's gone
        let (tx2, mut rx2) = mpsc::channel(100);
        db.get(b"key".to_vec(), tx2, 3).await.unwrap();
        let resp = rx2.recv().await.unwrap();
        assert!(!matches!(
            resp.payload,
            DatabaseResponseEnum::GetResponse { ok: true, .. }
        ));
    }

    #[tokio::test]
    async fn del_missing_key() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.delete(b"missing".to_vec(), tx, 1).await.unwrap();

        let resp = rx.recv().await.unwrap();
        match resp.payload {
            DatabaseResponseEnum::DelResponse { ok, req_id } => {
                assert!(!ok);
                assert_eq!(req_id, 1);
            }
            _ => panic!("wrong payload type"),
        }
    }

    // ========================== handle_event dispatch ==========================

    #[tokio::test]
    async fn handle_event_set() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.handle_event(DatabaseEvent::Set {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ttl: 1_000_000_000,
            sender: tx,
            req_id: 1,
        })
        .await
        .unwrap();

        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::SetResponse { ok: true, .. }
        ));
    }

    #[tokio::test]
    async fn handle_event_get_missing() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.handle_event(DatabaseEvent::Get {
            key: b"key".to_vec(),
            sender: tx,
            req_id: 1,
        })
        .await
        .unwrap();

        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::GetResponse { ok: false, .. }
        ));
    }

    #[tokio::test]
    async fn handle_event_del_missing() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.handle_event(DatabaseEvent::Del {
            key: b"key".to_vec(),
            sender: tx,
            req_id: 1,
        })
        .await
        .unwrap();

        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::DelResponse { ok: false, .. }
        ));
    }

    #[tokio::test]
    async fn handle_event_setnx() {
        let (db, tx, mut rx) = setup();
        let mut db = db;

        db.handle_event(DatabaseEvent::SetNX {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ttl: 1_000_000_000,
            sender: tx,
            req_id: 1,
        })
        .await
        .unwrap();

        let resp = rx.recv().await.unwrap();
        assert!(matches!(
            resp.payload,
            DatabaseResponseEnum::SetResponse { ok: true, .. }
        ));
    }
}

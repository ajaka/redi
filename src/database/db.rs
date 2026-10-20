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
        if ttl <= 0 {
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

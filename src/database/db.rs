use std::{
    collections::HashMap,
    ops::Add,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::mpsc::{Receiver, Sender, error};

use crate::{
    common::types::CmdType,
    database::types::{DatabaseEvent, DatabaseResponse, DatabaseResponseEnum},
};

struct Entry {
    value: Vec<u8>,
    ttl: u128,
}
pub struct DB {
    db: HashMap<Vec<u8>, Entry>,
}

impl DB {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    pub async fn init(mut self, mut recv: Receiver<DatabaseEvent>) {
        tokio::spawn(async move {
            loop {
                match recv.try_recv() {
                    Ok(e) => {
                        if let Err(e) = self.handle_event(e).await {
                            eprintln!("An Error occured {}", e)
                        }
                    }
                    Err(e) => match e {
                        error::TryRecvError::Empty => continue,
                        _ => {
                            eprintln!("An Error occured {}", e)
                        }
                    },
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
        req_id: u16,
    ) -> Result<()> {
        let resp = if self.db.contains_key(&key) {
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
        req_id: u16,
    ) -> Result<()> {
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
        let exp = self.get_now()?.add(ttl);
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
        req_id: u16,
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
        req_id: u16,
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

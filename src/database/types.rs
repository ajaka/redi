use tokio::sync::mpsc::Sender;

use crate::common::types::CmdType;

pub trait HasKey {
    fn get_key(&self) -> &Vec<u8>;
}

pub enum DatabaseEvent {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        sender: Sender<DatabaseResponse>,
        req_id: u16,
    },
    SetNX {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        sender: Sender<DatabaseResponse>,
        req_id: u16,
    },
    Get {
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u16,
    },
    Del {
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u16,
    },
}

impl HasKey for DatabaseEvent {
    fn get_key(&self) -> &Vec<u8> {
        match self {
            Self::Del {
                key,
                sender: _,
                req_id: _,
            } => key,
            Self::Get {
                key,
                sender: _,
                req_id: _,
            } => key,
            Self::Set {
                key,
                value: _,
                ttl: _,
                sender: _,
                req_id: _,
            } => key,
            Self::SetNX {
                key,
                value: _,
                ttl: _,
                sender: _,
                req_id: _,
            } => key,
        }
    }
}

pub struct DatabaseResponse {
    pub resp_type: CmdType,
    pub payload: DatabaseResponseEnum,
}

pub enum DatabaseResponseEnum {
    SetResponse {
        ok: bool,
        req_id: u16,
    },
    GetResponse {
        ok: bool,
        value: Vec<u8>,
        req_id: u16,
    },
    DelResponse {
        ok: bool,
        req_id: u16,
    },
}

impl DatabaseResponseEnum {
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            Self::DelResponse { ok, req_id } => {
                let mut buf: Vec<u8> = Vec::new();
                buf.extend_from_slice(&req_id.to_be_bytes());
                buf.push(ok as u8);
                buf
            }
            Self::GetResponse { ok, value, req_id } => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&req_id.to_be_bytes());
                buf.push(ok as u8);
                buf.extend_from_slice(&value);
                buf
            }
            Self::SetResponse { ok, req_id } => {
                let mut buf: Vec<u8> = Vec::new();
                buf.extend_from_slice(&req_id.to_be_bytes());
                buf.push(ok as u8);
                buf
            }
        }
    }
}

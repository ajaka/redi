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
        req_id: u8,
    },
    SetNX {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    },
    Get {
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
    },
    Del {
        key: Vec<u8>,
        sender: Sender<DatabaseResponse>,
        req_id: u8,
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
        req_id: u8,
    },
    GetResponse {
        ok: bool,
        value: Vec<u8>,
        req_id: u8,
    },
    DelResponse {
        ok: bool,
        req_id: u8,
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
                buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // ========================== to_bytes serialization ==========================

    #[test]
    fn set_response_ok_to_bytes() {
        let resp = DatabaseResponseEnum::SetResponse {
            ok: true,
            req_id: 42,
        };
        assert_eq!(resp.to_bytes(), vec![42, 1]);
    }

    #[test]
    fn set_response_fail_to_bytes() {
        let resp = DatabaseResponseEnum::SetResponse {
            ok: false,
            req_id: 7,
        };
        assert_eq!(resp.to_bytes(), vec![7, 0]);
    }

    #[test]
    fn del_response_ok_to_bytes() {
        let resp = DatabaseResponseEnum::DelResponse {
            ok: true,
            req_id: 99,
        };
        assert_eq!(resp.to_bytes(), vec![99, 1]);
    }

    #[test]
    fn del_response_fail_to_bytes() {
        let resp = DatabaseResponseEnum::DelResponse {
            ok: false,
            req_id: 0,
        };
        assert_eq!(resp.to_bytes(), vec![0, 0]);
    }

    #[test]
    fn get_response_with_value_to_bytes() {
        let resp = DatabaseResponseEnum::GetResponse {
            ok: true,
            value: b"hello".to_vec(),
            req_id: 5,
        };
        let bytes = resp.to_bytes();
        // req_id:1 + ok:1 + value_len:4 + value:5 = 11 bytes
        assert_eq!(bytes.len(), 11);
        assert_eq!(bytes[0], 5);
        assert_eq!(bytes[1], 1);
        assert_eq!(&bytes[2..6], &5u32.to_be_bytes());
        assert_eq!(&bytes[6..], b"hello");
    }

    #[test]
    fn get_response_empty_value_to_bytes() {
        let resp = DatabaseResponseEnum::GetResponse {
            ok: false,
            value: vec![],
            req_id: 3,
        };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 6);
        assert_eq!(bytes[0], 3);
        assert_eq!(bytes[1], 0);
        assert_eq!(&bytes[2..6], &0u32.to_be_bytes());
    }

    #[test]
    fn get_response_large_value_to_bytes() {
        let value = vec![0xABu8; 1024];
        let resp = DatabaseResponseEnum::GetResponse {
            ok: true,
            value: value.clone(),
            req_id: 255,
        };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 1 + 1 + 4 + 1024);
        assert_eq!(bytes[0], 255);
        assert_eq!(bytes[1], 1);
        assert_eq!(&bytes[2..6], &1024u32.to_be_bytes());
        assert_eq!(&bytes[6..], &value[..]);
    }

    // ========================== HasKey trait ==========================

    #[test]
    fn has_key_for_set_event() {
        let (tx, _rx) = mpsc::channel(1);
        let key = b"my-key".to_vec();
        let event = DatabaseEvent::Set {
            key: key.clone(),
            value: b"v".to_vec(),
            ttl: 100,
            sender: tx,
            req_id: 1,
        };
        assert_eq!(event.get_key(), &key);
    }

    #[test]
    fn has_key_for_setnx_event() {
        let (tx, _rx) = mpsc::channel(1);
        let key = b"my-key".to_vec();
        let event = DatabaseEvent::SetNX {
            key: key.clone(),
            value: b"v".to_vec(),
            ttl: 100,
            sender: tx,
            req_id: 1,
        };
        assert_eq!(event.get_key(), &key);
    }

    #[test]
    fn has_key_for_get_event() {
        let (tx, _rx) = mpsc::channel(1);
        let key = b"my-key".to_vec();
        let event = DatabaseEvent::Get {
            key: key.clone(),
            sender: tx,
            req_id: 1,
        };
        assert_eq!(event.get_key(), &key);
    }

    #[test]
    fn has_key_for_del_event() {
        let (tx, _rx) = mpsc::channel(1);
        let key = b"my-key".to_vec();
        let event = DatabaseEvent::Del {
            key: key.clone(),
            sender: tx,
            req_id: 1,
        };
        assert_eq!(event.get_key(), &key);
    }

    #[test]
    fn has_key_returns_correct_key_among_multiple() {
        let (tx, _rx) = mpsc::channel(1);
        let key_a = b"key-a".to_vec();
        let key_b = b"key-b".to_vec();

        let event_a = DatabaseEvent::Get {
            key: key_a.clone(),
            sender: tx.clone(),
            req_id: 1,
        };
        let event_b = DatabaseEvent::Get {
            key: key_b.clone(),
            sender: tx,
            req_id: 2,
        };

        assert_eq!(event_a.get_key(), &key_a);
        assert_eq!(event_b.get_key(), &key_b);
        assert_ne!(event_a.get_key(), event_b.get_key());
    }
}

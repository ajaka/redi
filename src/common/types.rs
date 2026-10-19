pub enum CmdType {
    Set = 1,
    SetNx = 2,
    Get = 3,
    Del = 4,
}

// Sentinel denotes not implemented and also doubles as invalid command during prod
pub enum CmdAction {
    Sentinel,
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        req_id: u8,
    },
    SetNX {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u128,
        req_id: u8,
    },
    Get {
        key: Vec<u8>,
        req_id: u8,
    },
    Del {
        key: Vec<u8>,
        req_id: u8,
    },
    Conn,
}

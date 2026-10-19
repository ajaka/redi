pub enum ResponseSignal {
    Err = 0x01,
    Set = 0x02,
    SetNx = 0x03,
    Get = 0x04,
    Del = 0x05,
    KeyTooLong = 0x06,
    ValueTooLong = 0x07,
    InvalidCommand = 0x08,
}

impl ResponseSignal {
    pub fn to_bytes(self) -> [u8; 1] {
        (self as u8).to_be_bytes()
    }
}

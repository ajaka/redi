use std::time::Duration;

use redi::listen_on;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::sync::CancellationToken;

// Command bytes (mirroring get_action in the server)
pub const CMD_SET: u8 = 1;
pub const CMD_SETNX: u8 = 2;
pub const CMD_GET: u8 = 3;
pub const CMD_DEL: u8 = 4;
pub const CMD_CONN: u8 = 5;

// Response signal bytes (mirroring ResponseSignal in the server)
pub const RESP_SET: u8 = 0x02;
pub const RESP_SETNX: u8 = 0x03;
pub const RESP_GET: u8 = 0x04;
pub const RESP_DEL: u8 = 0x05;
pub const RESP_ERR: u8 = 0x01;
pub const RESP_KEY_TOO_LONG: u8 = 0x06;
pub const RESP_VALUE_TOO_LONG: u8 = 0x07;
pub const RESP_INVALID_COMMAND: u8 = 0x08;

/// A test client that speaks the wire protocol over a real socket.
///
/// The encoders here are written independently of the server's internal
/// serialization so a change to the server's format can't quietly make a
/// test agree with a bug.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> Self {
        Self::try_connect(addr).await.expect("connect")
    }

    /// Unlike [`Client::connect`], a refused connection is a result, not a
    /// panic, so tests can assert the server is no longer listening.
    pub async fn try_connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// `[cmd:1][req_id:1][key_len:2][value_len:4][key][value][ttl:16]`
    pub async fn set(&mut self, key: &[u8], value: &[u8], ttl: u128, req_id: u8) {
        let mut buf = Vec::new();
        buf.push(CMD_SET);
        buf.push(req_id);
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf.extend_from_slice(&ttl.to_be_bytes());
        self.stream.write_all(&buf).await.expect("write set");
    }

    /// `[cmd:1][req_id:1][key_len:2][value_len:4][key][value][ttl:16]`
    pub async fn setnx(&mut self, key: &[u8], value: &[u8], ttl: u128, req_id: u8) {
        let mut buf = Vec::new();
        buf.push(CMD_SETNX);
        buf.push(req_id);
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf.extend_from_slice(&ttl.to_be_bytes());
        self.stream.write_all(&buf).await.expect("write setnx");
    }

    /// `[cmd:1][req_id:1][key_len:2][key]`
    pub async fn get(&mut self, key: &[u8], req_id: u8) {
        let mut buf = Vec::new();
        buf.push(CMD_GET);
        buf.push(req_id);
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.extend_from_slice(key);
        self.stream.write_all(&buf).await.expect("write get");
    }

    /// `[cmd:1][req_id:1][key_len:2][key]`
    pub async fn del(&mut self, key: &[u8], req_id: u8) {
        let mut buf = Vec::new();
        buf.push(CMD_DEL);
        buf.push(req_id);
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.extend_from_slice(key);
        self.stream.write_all(&buf).await.expect("write del");
    }

    /// `[cmd:1]` — enter long-session mode.
    pub async fn conn(&mut self) {
        self.stream
            .write_all(&[CMD_CONN])
            .await
            .expect("write conn");
    }

    /// Writes raw bytes, for malformed-frame tests.
    pub async fn write_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write raw");
    }

    /// Reads one `[signal:1]` response frame. Returns the signal byte and
    /// the remaining payload.
    pub async fn read_response(&mut self) -> (u8, Vec<u8>) {
        let mut signal = [0u8; 1];
        self.stream
            .read_exact(&mut signal)
            .await
            .expect("read signal");

        let body = match signal[0] {
            RESP_SET | RESP_SETNX | RESP_DEL => {
                // [req_id:1][ok:1]
                let mut body = [0u8; 2];
                self.stream.read_exact(&mut body).await.expect("read body");
                body.to_vec()
            }
            RESP_GET => {
                // [req_id:1][ok:1][value_len:4][value]
                let mut fixed = [0u8; 6];
                self.stream
                    .read_exact(&mut fixed)
                    .await
                    .expect("read get head");
                let value_len = u32::from_be_bytes(fixed[2..6].try_into().unwrap()) as usize;
                let mut value = vec![0u8; value_len];
                self.stream
                    .read_exact(&mut value)
                    .await
                    .expect("read value");
                fixed.iter().copied().chain(value).collect()
            }
            RESP_ERR => {
                // [code:1]
                let mut code = [0u8; 1];
                self.stream
                    .read_exact(&mut code)
                    .await
                    .expect("read err code");
                code.to_vec()
            }
            RESP_KEY_TOO_LONG | RESP_VALUE_TOO_LONG | RESP_INVALID_COMMAND => Vec::new(),
            other => panic!("unknown response signal {other:#04x}"),
        };

        (signal[0], body)
    }

    /// True once the server has closed the connection.
    pub async fn is_closed(&mut self) -> bool {
        let mut buf = [0u8; 1];
        matches!(self.stream.read(&mut buf).await, Ok(0) | Err(_))
    }

    /// True if the connection is still usable. A read on an idle-but-open
    /// socket never returns, so absence of EOF within the window counts as open.
    pub async fn still_open(&mut self) -> bool {
        let mut buf = [0u8; 1];
        tokio::time::timeout(Duration::from_millis(300), self.stream.read(&mut buf))
            .await
            .is_err()
    }
}

/// Spins up a server on an OS-assigned port. Returns its address and a
/// cancel token that stops the server when cancelled.
///
/// Binds a temporary listener to get a free port from the OS, then starts
/// the server on that port. There is a small race between dropping the
/// temporary listener and the server binding, but it's acceptable for tests.
pub async fn spawn_server() -> (String, CancellationToken) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);

    let addr = format!("127.0.0.1:{port}");
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let server_addr = addr.clone();

    tokio::spawn(async move {
        let _ = listen_on(&server_addr, server_cancel).await;
    });

    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, cancel)
}

/// Bounds a future by a timeout, so a hung test fails fast instead of
/// hanging the whole test run. Returns the future's output.
pub async fn within<F: std::future::Future>(
    dur: std::time::Duration,
    what: &str,
    f: F,
) -> F::Output {
    tokio::time::timeout(dur, f)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish in time"))
}

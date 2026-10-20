use std::sync::Arc;

use anyhow::{Result, bail};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        limits::{MAX_KEY_LENGTH, MAX_VALUE_LENGTH},
        types::{CmdAction, CmdType},
    },
    database::{
        conductor::Conductor,
        types::{DatabaseEvent, DatabaseResponse},
    },
    network::types::ResponseSignal,
};

pub async fn listen(cancel_token: CancellationToken) -> Result<()> {
    listen_on("127.0.0.1:6380", cancel_token).await
}

pub async fn listen_on(addr: &str, cancel_token: CancellationToken) -> Result<()> {
    let conductor = Arc::new(Conductor::init(10, cancel_token.clone()).await);
    let listener = TcpListener::bind(addr).await?;
    println!("listening on {}", listener.local_addr().unwrap());
    loop {
        let cloned_token = cancel_token.clone();
        let socket = tokio::select! {
            biased;
            _ = cloned_token.cancelled() => {
                return Ok(())
            },
            result = listener.accept() => {
                let (socket,_) = result?;
                socket
            },
        };
        let cloned_conductor = conductor.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(socket, cloned_conductor, cloned_token).await {
                eprint!("Error occured {}", e)
            }
        });
    }
}

async fn handle_conn(
    socket: TcpStream,
    conductor: Arc<Conductor>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let action = get_action(&mut reader).await?;
    let (sender, receiver) = mpsc::channel(100);
    match action {
        CmdAction::Sentinel => {
            writer
                .write_all(&ResponseSignal::InvalidCommand.to_bytes())
                .await?
        }
        CmdAction::Get { key, req_id } => {
            let event = DatabaseEvent::Get {
                key,
                sender,
                req_id,
            };
            dispatch_quick(
                event,
                conductor,
                receiver,
                writer,
                &ResponseSignal::Get.to_bytes(),
            )
            .await?;
        }
        CmdAction::Set {
            key,
            value,
            ttl,
            req_id,
        } => {
            let event = DatabaseEvent::Set {
                key,
                value,
                ttl,
                sender,
                req_id,
            };
            dispatch_quick(
                event,
                conductor,
                receiver,
                writer,
                &ResponseSignal::Set.to_bytes(),
            )
            .await?
        }
        CmdAction::SetNX {
            key,
            value,
            ttl,
            req_id,
        } => {
            let event = DatabaseEvent::SetNX {
                key,
                value,
                ttl,
                sender,
                req_id,
            };
            dispatch_quick(
                event,
                conductor,
                receiver,
                writer,
                &ResponseSignal::SetNx.to_bytes(),
            )
            .await?
        }
        CmdAction::Del { key, req_id } => {
            let event = DatabaseEvent::Del {
                key,
                sender,
                req_id,
            };
            dispatch_quick(
                event,
                conductor,
                receiver,
                writer,
                &ResponseSignal::Del.to_bytes(),
            )
            .await?
        }
        CmdAction::Conn => {
            handle_long_session(writer, reader, conductor, sender, receiver, cancel_token).await?;
        }
    }
    Ok(())
}

async fn handle_long_session(
    mut writer: OwnedWriteHalf,
    mut reader: OwnedReadHalf,
    conductor: Arc<Conductor>,
    sender: mpsc::Sender<DatabaseResponse>,
    mut receiver: mpsc::Receiver<DatabaseResponse>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let child_token = cancel_token.child_token();
    let child_cancel_token = child_token.clone();
    tokio::spawn(async move {
        let mut buf = Vec::new();
        loop {
            let msg = tokio::select! {
                biased;
                msg = receiver.recv() => match msg {
                    Some(msg) => msg,
                    None => break,
                },
                _ = child_cancel_token.cancelled () => break,
            };
            let signal = match msg.resp_type {
                CmdType::Del => &ResponseSignal::Del.to_bytes(),
                CmdType::Get => &ResponseSignal::Get.to_bytes(),
                CmdType::Set => &ResponseSignal::Set.to_bytes(),
                CmdType::SetNx => &ResponseSignal::SetNx.to_bytes(),
            };
            buf.extend_from_slice(signal);
            buf.extend_from_slice(&msg.payload.to_bytes());
            if let Err(e) = writer.write_all(&buf).await {
                eprint!("Error whilst writing to client {}", e);
                break;
            }
            buf.clear();
        }
    });

    loop {
        let action = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                return Ok(())
            },
            action = get_action(&mut reader) => match action {
                Ok(action) => action,
                Err(e) => {
                    child_token.cancel();
                    return Err(e);
                },
            },
        };
        let event = match action {
            CmdAction::Get { key, req_id } => DatabaseEvent::Get {
                key,
                sender: sender.clone(),
                req_id,
            },
            CmdAction::Del { key, req_id } => DatabaseEvent::Del {
                key,
                sender: sender.clone(),
                req_id,
            },
            CmdAction::Set {
                key,
                value,
                ttl,
                req_id,
            } => DatabaseEvent::Set {
                key,
                value,
                ttl,
                sender: sender.clone(),
                req_id,
            },
            CmdAction::SetNX {
                key,
                value,
                ttl,
                req_id,
            } => DatabaseEvent::SetNX {
                key,
                value,
                ttl,
                sender: sender.clone(),
                req_id,
            },
            _ => continue,
        };
        conductor.dispatch(event).await?;
    }
}

async fn dispatch_quick(
    e: DatabaseEvent,
    conductor: Arc<Conductor>,
    mut recv: mpsc::Receiver<DatabaseResponse>,
    mut writer: OwnedWriteHalf,
    signal: &[u8; 1],
) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(signal);
    conductor.dispatch(e).await?;
    let resp = match recv.recv().await {
        Some(resp) => resp,
        None => bail!("Could not get response from database"),
    };
    buf.extend_from_slice(&resp.payload.to_bytes());
    writer.write_all(&buf).await?;
    Ok(())
}
enum SetType {
    Set,
    SetNX,
}
enum GetDel {
    Del,
    Get,
}
async fn get_action(reader: &mut OwnedReadHalf) -> Result<CmdAction> {
    let mut cmd_buf = vec![0u8; 1];
    reader.read_exact(&mut cmd_buf).await?;
    let cmd = cmd_buf[0];

    match cmd {
        1 => read_sets(reader, SetType::Set).await,
        2 => read_sets(reader, SetType::SetNX).await,
        3 => parse_get_and_del(reader, GetDel::Get).await,
        4 => parse_get_and_del(reader, GetDel::Del).await,
        5 => Ok(CmdAction::Conn),
        _ => Ok(CmdAction::Sentinel),
    }
}

async fn parse_get_and_del(reader: &mut OwnedReadHalf, action: GetDel) -> Result<CmdAction> {
    let mut req_id_buf = [0u8; 1];
    reader.read_exact(&mut req_id_buf).await?;
    let req_id = req_id_buf[0];

    let mut key_len_buf = [0u8; 2];
    reader.read_exact(&mut key_len_buf).await?;
    let key_len = u16::from_be_bytes(key_len_buf);
    validate_key_len(key_len)?;

    let mut key = vec![0u8; key_len as usize];
    reader.read_exact(&mut key).await?;

    let resp = match action {
        GetDel::Del => CmdAction::Del { key, req_id },
        GetDel::Get => CmdAction::Get { key, req_id },
    };
    Ok(resp)
}

async fn read_sets(reader: &mut OwnedReadHalf, action: SetType) -> Result<CmdAction> {
    let mut req_id_buf = [0u8; 1];
    reader.read_exact(&mut req_id_buf).await?;
    let req_id = req_id_buf[0];

    let mut key_len_buf = [0u8; 2];
    reader.read_exact(&mut key_len_buf).await?;
    let key_len = u16::from_be_bytes(key_len_buf);
    validate_key_len(key_len)?;

    let mut value_len_buf = [0u8; 4];
    reader.read_exact(&mut value_len_buf).await?;
    let value_len = u32::from_be_bytes(value_len_buf);
    validate_value_len(value_len)?;

    let mut key = vec![0u8; key_len as usize];
    reader.read_exact(&mut key).await?;

    let mut value = vec![0u8; value_len as usize];
    reader.read_exact(&mut value).await?;

    let mut ttl_buf = [0u8; 16];
    reader.read_exact(&mut ttl_buf).await?;
    let ttl = u128::from_be_bytes(ttl_buf);

    let resp = match action {
        SetType::Set => CmdAction::Set {
            key,
            value,
            ttl,
            req_id,
        },
        SetType::SetNX => CmdAction::SetNX {
            key,
            value,
            ttl,
            req_id,
        },
    };
    Ok(resp)
}

fn validate_key_len(key_len: u16) -> Result<()> {
    if key_len > MAX_KEY_LENGTH {
        bail!("The client provided key exceeds set limits");
    };
    Ok(())
}

fn validate_value_len(value_len: u32) -> Result<()> {
    if value_len > MAX_VALUE_LENGTH {
        bail!("The client provided value exceeds set limits");
    };
    Ok(())
}

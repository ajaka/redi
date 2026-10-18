// TBH, i look forward to seeing the next bullshit you will cookup this time hehe

use std::sync::Arc;

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};

use crate::{
    common::types::{CmdAction, CmdType},
    database::{
        conductor::Conductor,
        types::{DatabaseEvent, DatabaseResponse},
    },
    network::types::ResponseSignal,
};

pub async fn listen(addr: &str) -> Result<()> {
    let conductor = Arc::new(Conductor::init(10).await);
    let listener = TcpListener::bind(addr).await?;
    println!("listening on {}", listener.local_addr().unwrap());
    loop {
        let (socket, _) = listener.accept().await?;
        let cloned_conductor = conductor.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(socket, cloned_conductor).await {
                eprint!("Error occured {}", e)
            }
        });
    }
}

async fn handle_conn(socket: TcpStream, conductor: Arc<Conductor>) -> Result<()> {
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
            handle_long_session(writer, reader, conductor, sender, receiver).await?;
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
) -> Result<()> {
    tokio::spawn(async move {
        let mut buf = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(msg) => {
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
                }
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => continue,
                    _ => break,
                },
            };
        }
    });

    loop {
        let action = get_action(&mut reader).await?;
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
    let resp = recv.recv().await.unwrap();
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
        _ => Ok(CmdAction::Sentinel),
    }
}

async fn parse_get_and_del(reader: &mut OwnedReadHalf, action: GetDel) -> Result<CmdAction> {
    let mut req_id_buf = [0u8; 2];
    reader.read_exact(&mut req_id_buf).await?;
    let req_id = u16::from_be_bytes(req_id_buf);
    let mut key_length_buf = [0u8; 2];
    reader.read_exact(&mut key_length_buf).await?;
    let key_length = u16::from_be_bytes(key_length_buf) as usize;
    let mut key = vec![0u8; key_length];
    reader.read_exact(&mut key).await?;
    let resp = match action {
        GetDel::Del => CmdAction::Del { key, req_id },
        GetDel::Get => CmdAction::Get { key, req_id },
    };
    Ok(resp)
}

async fn read_sets(reader: &mut OwnedReadHalf, action: SetType) -> Result<CmdAction> {
    let mut req_id_buf = [0u8; 2];
    reader.read_exact(&mut req_id_buf).await?;
    let req_id = u16::from_be_bytes(req_id_buf);
    let mut key_length_buf = [0u8; 2];
    reader.read_exact(&mut key_length_buf).await?;
    let key_length = u16::from_be_bytes(key_length_buf) as usize;
    let mut value_length_buf = [0u8; 4];
    reader.read_exact(&mut value_length_buf).await?;
    let value_length = u32::from_be_bytes(value_length_buf) as usize;
    let mut key = vec![0u8; key_length];
    reader.read_exact(&mut key).await?;
    let mut value = vec![0u8; value_length];
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

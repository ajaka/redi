mod common;

use common::{Client, RESP_DEL, RESP_GET, RESP_INVALID_COMMAND, RESP_SET, RESP_SETNX, within};
use std::time::Duration;
use tokio::time::sleep;

const T: Duration = Duration::from_secs(5);

// ---------------------------------------------------------- happy path (quick mode)

#[tokio::test]
async fn set_returns_ok() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 1_000_000_000, 1).await;

    let (signal, body) = within(T, "set response", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    assert_eq!(body[0], 1); // req_id
    assert_eq!(body[1], 1); // ok:true
}

#[tokio::test]
async fn get_existing_key_returns_value() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 1_000_000_000, 1).await;
    within(T, "set", client.read_response()).await;
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.get(b"key", 2).await;
    let (signal, body) = within(T, "get response", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[0], 2); // req_id
    assert_eq!(body[1], 1); // ok:true
    let value_len = u32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
    assert_eq!(value_len, 5);
    assert_eq!(&body[6..6 + value_len], b"value");
}

#[tokio::test]
async fn del_existing_key_then_get_returns_not_found() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 1_000_000_000, 1).await;
    within(T, "set", client.read_response()).await;
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.del(b"key", 2).await;
    let (signal, body) = within(T, "del response", client.read_response()).await;
    assert_eq!(signal, RESP_DEL);
    assert_eq!(body[1], 1); // ok:true
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.get(b"key", 3).await;
    let (signal, body) = within(T, "get after del", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn setnx_on_missing_key_succeeds() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.setnx(b"key", b"value", 1_000_000_000, 1).await;

    let (signal, body) = within(T, "setnx response", client.read_response()).await;
    assert_eq!(signal, RESP_SETNX);
    assert_eq!(body[1], 1); // ok:true
}

#[tokio::test]
async fn setnx_on_existing_key_fails() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"first", 1_000_000_000, 1).await;
    within(T, "set", client.read_response()).await;
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.setnx(b"key", b"second", 1_000_000_000, 2).await;
    let (signal, body) = within(T, "setnx response", client.read_response()).await;
    assert_eq!(signal, RESP_SETNX);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn get_missing_key_returns_not_found() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.get(b"missing", 1).await;

    let (signal, body) = within(T, "get response", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn del_missing_key_returns_not_found() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.del(b"missing", 1).await;

    let (signal, body) = within(T, "del response", client.read_response()).await;
    assert_eq!(signal, RESP_DEL);
    assert_eq!(body[1], 0); // ok:false
}

// ---------------------------------------------------------------- TTL behavior

#[tokio::test]
async fn set_with_zero_ttl_is_rejected() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 0, 1).await;

    let (signal, body) = within(T, "set response", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn setnx_with_zero_ttl_is_rejected() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.setnx(b"key", b"value", 0, 1).await;

    let (signal, body) = within(T, "setnx response", client.read_response()).await;
    assert_eq!(signal, RESP_SETNX);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn key_expires_after_ttl() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // TTL of 500 milliseconds (in nanoseconds)
    client.set(b"key", b"value", 500_000_000, 1).await;
    within(T, "set", client.read_response()).await;
    drop(client);

    // Immediately getable
    let mut client = Client::connect(&addr).await;
    client.get(b"key", 2).await;
    let (signal, body) = within(T, "get immediate", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[1], 1); // ok:true
    drop(client);

    // Wait for expiry
    sleep(Duration::from_secs(1)).await;

    let mut client = Client::connect(&addr).await;
    client.get(b"key", 3).await;
    let (signal, body) = within(T, "get after expiry", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[1], 0); // ok:false
}

#[tokio::test]
async fn setnx_on_expired_key_succeeds() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // Insert with a 500ms TTL
    client.set(b"key", b"first", 500_000_000, 1).await;
    within(T, "set", client.read_response()).await;
    drop(client);

    // Wait for expiry
    sleep(Duration::from_secs(1)).await;

    let mut client = Client::connect(&addr).await;
    client.setnx(b"key", b"second", 1_000_000_000, 2).await;
    let (signal, body) = within(T, "setnx after expiry", client.read_response()).await;
    assert_eq!(signal, RESP_SETNX);
    assert_eq!(body[1], 1); // ok:true
}

#[tokio::test]
async fn set_overwrites_existing_key() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"first", 1_000_000_000, 1).await;
    within(T, "first set", client.read_response()).await;
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"second", 1_000_000_000, 2).await;
    within(T, "second set", client.read_response()).await;
    drop(client);

    let mut client = Client::connect(&addr).await;
    client.get(b"key", 3).await;
    let (_, body) = within(T, "get after overwrite", client.read_response()).await;
    let value_len = u32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
    assert_eq!(&body[6..6 + value_len], b"second");
}

// ---------------------------------------------------------------- protocol errors

#[tokio::test]
async fn unknown_command_returns_invalid_command() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.write_raw(&[0xFF]).await;

    let (signal, _) = within(T, "invalid cmd", client.read_response()).await;
    assert_eq!(signal, RESP_INVALID_COMMAND);
}

#[tokio::test]
async fn oversized_key_closes_connection() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // Key length of 1025 (MAX_KEY_LENGTH + 1)
    let mut buf = vec![common::CMD_SET, 1];
    buf.extend_from_slice(&1025u16.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&0u128.to_be_bytes());
    client.write_raw(&buf).await;

    // Server closes connection without sending a response for oversized keys
    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn oversized_value_closes_connection() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    let key = b"key";
    let mut buf = vec![common::CMD_SET, 1];
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(&((1024 * 1024 + 1) as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&0u128.to_be_bytes());
    client.write_raw(&buf).await;

    // Server closes connection without sending a response for oversized values
    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn key_at_exact_limit_is_accepted() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    let key = vec![b'k'; 1024];
    client.set(&key, b"value", 1_000_000_000, 1).await;

    let (signal, body) = within(T, "set at limit", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    assert_eq!(body[1], 1); // ok:true
}

#[tokio::test]
async fn value_at_exact_limit_is_accepted() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    let value = vec![b'v'; 1024 * 1024];
    client.set(b"key", &value, 1_000_000_000, 1).await;

    let (signal, body) = within(T, "set at limit", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    assert_eq!(body[1], 1); // ok:true
}

// ---------------------------------------------------------------- long session mode

#[tokio::test]
async fn long_session_multiple_commands() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.conn().await;

    // SET
    client.set(b"key1", b"value1", 1_000_000_000, 1).await;
    let (signal, body) = within(T, "session set", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    assert_eq!(body[0], 1);
    assert_eq!(body[1], 1);

    // GET
    client.get(b"key1", 2).await;
    let (signal, body) = within(T, "session get", client.read_response()).await;
    assert_eq!(signal, RESP_GET);
    assert_eq!(body[0], 2);
    assert_eq!(body[1], 1);

    // DEL
    client.del(b"key1", 3).await;
    let (signal, body) = within(T, "session del", client.read_response()).await;
    assert_eq!(signal, RESP_DEL);
    assert_eq!(body[0], 3);
    assert_eq!(body[1], 1);

    // Connection should still be open
    assert!(client.still_open().await);
}

#[tokio::test]
async fn long_session_responses_match_req_id() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.conn().await;

    // Send multiple commands with different req_ids
    client.set(b"a", b"1", 1_000_000_000, 10).await;
    client.set(b"b", b"2", 1_000_000_000, 20).await;
    client.set(b"c", b"3", 1_000_000_000, 30).await;

    // Responses may arrive in any order; verify the set of req_ids.
    let mut req_ids = Vec::new();
    for _ in 0..3 {
        let (signal, body) = within(T, "session resp", client.read_response()).await;
        assert_eq!(signal, RESP_SET);
        req_ids.push(body[0]);
    }
    req_ids.sort_unstable();
    assert_eq!(req_ids, vec![10, 20, 30]);
}

// ---------------------------------------------------------------- connection lifecycle

#[tokio::test]
async fn quick_mode_connection_closes_after_response() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 1_000_000_000, 1).await;
    within(T, "set", client.read_response()).await;

    // In quick mode, the server should close the connection after one response
    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn multiple_concurrent_connections() {
    let (addr, _cancel) = common::spawn_server().await;

    let mut clients = Vec::new();
    for i in 0..5u8 {
        let mut client = Client::connect(&addr).await;
        client
            .set(
                format!("key{i}").as_bytes(),
                format!("value{i}").as_bytes(),
                1_000_000_000,
                i,
            )
            .await;
        clients.push(client);
    }

    for (i, client) in clients.iter_mut().enumerate() {
        let (signal, body) = within(T, "concurrent set", client.read_response()).await;
        assert_eq!(signal, RESP_SET);
        assert_eq!(body[0], i as u8);
        assert_eq!(body[1], 1);
    }
}

// ---------------------------------------------------------------- shutdown

#[tokio::test]
async fn shutdown_stops_server() {
    let (addr, cancel) = common::spawn_server().await;

    // Verify server is up
    let mut client = Client::connect(&addr).await;
    client.set(b"key", b"value", 1_000_000_000, 1).await;
    let (signal, _) = within(T, "pre-shutdown set", client.read_response()).await;
    assert_eq!(signal, RESP_SET);
    drop(client);

    // Shutdown
    cancel.cancel();

    // Server should no longer accept connections
    sleep(Duration::from_millis(100)).await;
    let result = tokio::time::timeout(Duration::from_millis(500), Client::try_connect(&addr)).await;
    if let Ok(Ok(_)) = result {
        panic!("connect should not succeed after shutdown");
    }
}

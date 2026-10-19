pub mod common;
pub mod database;
pub mod network;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

pub async fn start(cancel_token: CancellationToken) -> Result<()> {
    network::net::listen("127.0.0.1:6380", cancel_token).await?;
    Ok(())
}

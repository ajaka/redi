pub mod common;
pub mod database;
pub mod network;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

pub use network::net::listen_on;

pub async fn start(cancel_token: CancellationToken) -> Result<()> {
    network::net::listen(cancel_token).await?;
    Ok(())
}

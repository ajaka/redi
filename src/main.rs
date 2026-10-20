use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let cancel_token = CancellationToken::new();
    let cloned_token = cancel_token.clone();

    // Simple shutdown procedure
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("shutting down");
            cloned_token.cancel();
        }
    });
    if let Err(e) = redi::start(cancel_token).await {
        eprintln!("{}", e)
    }
}

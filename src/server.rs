use crate::geyser_subscription::GeyserSubscription;
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::{RpcModule, SubscriptionMessage};
use tokio::sync::broadcast::error::RecvError;
use tokio::{select, signal};
use tracing::{debug, debug_span, error, info, Instrument};


pub async fn run_rpc_server(sub: GeyserSubscription, port: u16) -> anyhow::Result<()> {
    let module = build_rpc_module(sub);
    
    let config = ServerConfig::builder()
        .set_message_buffer_capacity(5)
        .max_response_body_size(32 * 1024 * 1024)
        .build();

    let server = Server::builder()
        .set_config(config)
        .build(("0.0.0.0", port))
        .await?;

    let addr = server.local_addr()?;
    let handle = server.start(module);
    info!("server is listening on port {}", addr.port());
    
    shutdown_signal().await;
    
    if handle.stop().is_ok() {
        handle.stopped().await;    
    }
    
    Ok(())
}


async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}


fn build_rpc_module(sub: GeyserSubscription) -> RpcModule<GeyserSubscription> {
    let mut rpc = RpcModule::new(sub);
    rpc.register_subscription_raw(
        "geyser_blockSubscribe",
        "geyser_blockNotification",
        "geyser_blockUnsubscribe",
        |_, pending, ctx, _| {
            let span = debug_span!("block_subscription", connection_id = pending.connection_id().0);
            tokio::spawn(async move {
                let sink = match pending.accept().await {
                    Ok(sink) => sink,
                    Err(_) => {
                        debug!("closed before acceptance");
                        return
                    }
                };
                debug!("accepted");
                let mut rx = ctx.subscribe();
                loop {
                    select! {
                        biased;
                        _ = sink.closed() => debug!("closed"),
                        event = rx.recv() => {
                            match event {
                                Ok(block) => {
                                    let msg = SubscriptionMessage::new(
                                        sink.method_name(),
                                        sink.subscription_id(),
                                        &block
                                    ).expect(
                                        "serialization is infallible"
                                    );
                                    if sink.send(msg).await.is_err() {
                                        debug!("closed");
                                        return 
                                    }
                                    debug!(slot = block.slot, "block sent");
                                },
                                Err(RecvError::Lagged(skipped)) => {
                                    debug!(skipped = skipped, "lagging behind");
                                    continue
                                },
                                Err(RecvError::Closed) => {
                                    error!("geyser termination");
                                    let eof = SubscriptionMessage::new(
                                        sink.method_name(),
                                        sink.subscription_id(),
                                        &serde_json::value::Value::Null
                                    ).expect(
                                        "serialization is infallible"
                                    );
                                    let _ = sink.send(eof).await;
                                    return
                                }
                            }
                        }
                    }
                }
            }.instrument(span));
        }
    ).unwrap();
    rpc
}
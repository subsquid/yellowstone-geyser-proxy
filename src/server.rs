use crate::geyser_subscription::GeyserSubscription;
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::{RpcModule, SubscriptionMessage};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use tokio::signal;
use tokio::sync::broadcast::error::RecvError;
use tracing::info;


pub async fn run_rpc_server(sub: GeyserSubscription, port: u16) -> anyhow::Result<()> {
    let module = build_rpc_module(sub);
    
    let config = ServerConfig::builder()
        .set_message_buffer_capacity(5)
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

    tokio::select! {
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
            tokio::spawn(async move {
                let sink = match pending.accept().await {
                    Ok(sink) => sink,
                    Err(_) => {
                        return
                    }
                };
                let mut rx = ctx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(block) => {
                            let msg = SubscriptionMessage::new(
                                sink.method_name(),
                                sink.subscription_id(),
                                &RawJson::new(&block)
                            ).expect(
                                "serialization is infallible"
                            );
                            if sink.send(msg).await.is_err() {
                                return 
                            }
                        },
                        Err(RecvError::Lagged(_)) => {
                            continue
                        },
                        Err(RecvError::Closed) => {
                            return
                        }
                    }
                }
            });
        }
    ).unwrap();
    rpc
}


struct RawJson<'a> {
    json: &'a str
}


impl<'a> RawJson<'a> {
    pub fn new(json: &'a str) -> Self {
        Self { json }
    }
}


impl<'a> Serialize for RawJson<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("$serde_json::private::RawValue", 1)?;
        s.serialize_field("$serde_json::private::RawValue", &self.json)?;
        s.end()
    }
}
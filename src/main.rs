use crate::auth_interceptor::AuthInterceptor;
use crate::cli::CLI;
use crate::geyser_subscription::GeyserSubscription;
use crate::server::run_rpc_server;
use anyhow::Context;
use clap::Parser;
use std::sync::atomic::Ordering;
use tonic::transport::{ClientTlsConfig, Endpoint};


mod auth_interceptor;
mod cli;
mod geyser;
mod geyser_subscription;
mod json_builder;
mod server;
mod mapping;


#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


fn main() -> anyhow::Result<()> {
    let args = CLI::parse();

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or("info".to_string()),
    );

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .compact()
        .init();

    if let Some(threads) = args.mapping_threads {
        geyser_subscription::MAPPING_THREADS.store(threads, Ordering::SeqCst);
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let channel = Endpoint::from_shared(args.geyser_url)?
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .context("failed to configure TLS for geyser endpoint")?
                .connect()
                .await
                .context("failed to connect to geyser plugin")?;

            let auth = AuthInterceptor {
                x_token: args.geyser_x_token,
                x_access_token: args.geyser_x_access_token
            };

            let sub = GeyserSubscription::start(channel, auth)
                .await
                .context("failed to subscribe to block updates")?;

            run_rpc_server(sub, args.port).await
        })?;

    Ok(())
}



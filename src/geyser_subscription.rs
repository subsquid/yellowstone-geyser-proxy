use crate::auth_interceptor::AuthInterceptor;
use crate::geyser::api::geyser_client::GeyserClient;
use crate::geyser::api::subscribe_update::UpdateOneof;
use crate::geyser::api::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeUpdate};
use crate::mapping;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;
use tonic::codegen::{CompressionEncoding, InterceptedService};
use tonic::transport::Channel;
use tonic::{Status, Streaming};
use tracing::{error, info, instrument, Instrument};


pub type BlockJson = Arc<str>;


type Client = GeyserClient<InterceptedService<Channel, AuthInterceptor>>;


#[derive(Clone)]
pub struct GeyserSubscription {
    tx: tokio::sync::broadcast::Sender<BlockJson>,
    subscribed: tokio::sync::watch::Sender<()>,
}


impl GeyserSubscription {
    pub async fn start(channel: Channel, auth: AuthInterceptor, with_votes: bool) -> anyhow::Result<Self> {
        let mut client = GeyserClient::with_interceptor(channel, auth)
            .max_decoding_message_size(32 * 1024 * 1024)
            .accept_compressed(CompressionEncoding::Zstd);

        let updates = subscribe(&mut client).await?;

        let tx = tokio::sync::broadcast::Sender::new(2);
        let (subscribed_tx, subscribed_rx) = tokio::sync::watch::channel(());

        tokio::spawn(
            control_loop(client, updates, subscribed_rx, tx.clone(), with_votes).in_current_span()
        );

        Ok(Self {
            tx,
            subscribed: subscribed_tx
        })
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BlockJson> {
        self.subscribed.send(()).expect("receive end must be alive");
        self.tx.subscribe()
    }
}


#[instrument(name="geyser", skip_all)]
async fn control_loop(
    client: Client,
    updates: Streaming<SubscribeUpdate>,
    mut subscribed_rx: tokio::sync::watch::Receiver<()>,
    tx: tokio::sync::broadcast::Sender<BlockJson>,
    with_votes: bool
) {
    enum State {
        Running(BoxFuture<'static, ()>),
        WillPauseSoon(BoxFuture<'static, ()>, Instant),
        Paused
    }

    let mut state = State::WillPauseSoon(
        run_subscription(client.clone(), Some(updates), tx.clone(), with_votes).boxed(),
        Instant::now() + Duration::from_secs(30)
    );

    loop {
        match state {
            State::Running(mut sub) => {
                select! {
                    biased;
                    _ = tx.closed() => {
                        subscribed_rx.borrow_and_update();
                        if tx.receiver_count() == 0 {
                            state = State::WillPauseSoon(
                                sub,
                                Instant::now() + Duration::from_secs(10)
                            )
                        } else {
                            state = State::Running(sub)
                        }
                    },
                    _ = sub.as_mut() => {
                        unreachable!()
                    }
                }
            },
            State::WillPauseSoon(mut sub, time) => {
                select! {
                    biased;
                    new_sub_result = subscribed_rx.changed() => {
                        match new_sub_result {
                            Ok(_) => {
                                state = State::Running(sub)
                            },
                            Err(_) => {
                                break
                            }
                        }
                    },
                    _ = tokio::time::sleep_until(time) => {
                        info!("pausing subscription, because there are no subscribers");
                        state = State::Paused
                    },
                    _ = sub.as_mut() => {
                        unreachable!()
                    }
                }
            }
            State::Paused => {
                match subscribed_rx.changed().await {
                    Ok(_) => {
                        state = State::Running(
                            run_subscription(client.clone(), None, tx.clone(), with_votes).boxed()
                        )
                    },
                    Err(_) => {
                        break
                    }
                }
            }
        }
    }

    info!("terminated")
}


async fn run_subscription(
    mut client: Client,
    mut updates: Option<Streaming<SubscribeUpdate>>,
    mut tx: tokio::sync::broadcast::Sender<BlockJson>,
    with_votes: bool
) {
    let mut errors = 0;
    let backoff_ms = [0, 0, 200, 500, 1000, 2000, 5000];
    loop {
        let pause = backoff_ms[errors.min(backoff_ms.len() - 1)];
        if pause > 0 {
            info!("paused for {} ms", pause);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        let updates = if let Some(updates) = updates.take() {
            updates
        } else {
            match subscribe(&mut client).await {
                Ok(updates) => updates,
                Err(status) => {
                    error!(grpc_status =? status, "failed to subscribe");
                    errors += 1;
                    continue
                }
            }
        };
        info!("subscribed");
        match receive_updates(updates, &mut tx, &mut errors, with_votes).await {
            Ok(_) => error!("unexpected end of update stream"),
            Err(status) => error!(grpc_status =? status, "subscription error"),
        }
        errors += 1;
    }
}


async fn subscribe(client: &mut Client) -> Result<Streaming<SubscribeUpdate>, Status> {
    let req = SubscribeRequest {
        blocks: HashMap::from([
            ("blocks".to_string(), SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                ..SubscribeRequestFilterBlocks::default()
            })
        ]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..SubscribeRequest::default()
    };

    client.subscribe(tokio_stream::once(req))
        .await
        .map(|res| res.into_inner())
}


async fn receive_updates(
    mut updates: Streaming<SubscribeUpdate>,
    tx: &mut tokio::sync::broadcast::Sender<BlockJson>,
    errors: &mut usize,
    with_votes: bool
) -> Result<(), Status>
{
    while let Some(upd) = updates.message().await? {
        if let Some(upd) = upd.update_oneof {
            if let UpdateOneof::Block(block) = upd {
                *errors = 0;

                let block_time = block
                    .block_time
                    .expect("recent blocks must have time")
                    .timestamp as u128;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is below unix epoch")
                    .as_millis();

                let block_age = current_time - block_time * 1000;
                let slot = block.slot;

                info!(slot = slot, block_age = block_age as u64, "new block");

                let (mapping_tx, mapping_rx) = tokio::sync::oneshot::channel();

                MAPPING_POOL.spawn(move || {
                    let result = mapping::render_block(&block, with_votes);
                    let _ = mapping_tx.send(result);
                });

                match mapping_rx.await {
                    Ok(Ok(json)) => {
                        let _ = tx.send(json.into());
                    },
                    Ok(Err(err)) => {
                        error!(slot = slot, error =? err, "invalid block");
                    },
                    Err(err) => {
                        error!("mapping task failed - {}", err)
                    }
                }
            }
        }
    }
    Ok(())
}


pub static MAPPING_THREADS: AtomicUsize = AtomicUsize::new(0);


static MAPPING_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    let mut threads = MAPPING_THREADS.load(Ordering::SeqCst);
    if threads == 0 {
        threads = std::thread::available_parallelism()
            .map(|n| std::cmp::min(n.get(), 4))
            .unwrap_or(4);
    }
    info!("will use {} thread(s) for mapping", threads);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
});
use crate::auth_interceptor::AuthInterceptor;
use crate::geyser::api::geyser_client::GeyserClient;
use crate::geyser::api::subscribe_update::UpdateOneof;
use crate::geyser::api::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};
use crate::json_builder::{safe_prop, JsonBuilder};
use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::{Status, Streaming};
use tracing::{error, info, instrument, Instrument};
use crate::geyser::solana::storage::confirmed_block::TokenBalance;


pub type BlockJson = Arc<str>;


type Client = GeyserClient<InterceptedService<Channel, AuthInterceptor>>;


#[derive(Clone)]
pub struct GeyserSubscription {
    tx: tokio::sync::broadcast::Sender<BlockJson>,
    subscribed: tokio::sync::watch::Sender<()>,
}


impl GeyserSubscription {
    pub async fn start(channel: Channel, auth: AuthInterceptor) -> anyhow::Result<Self> {
        let mut client = GeyserClient::with_interceptor(channel, auth);

        let updates = subscribe(&mut client).await?;

        let tx = tokio::sync::broadcast::Sender::new(2);
        let (subscribed_tx, subscribed_rx) = tokio::sync::watch::channel(());

        tokio::spawn(
            control_loop(client, updates, subscribed_rx, tx.clone()).in_current_span()
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
    tx: tokio::sync::broadcast::Sender<BlockJson>
) {
    enum State {
        Running(BoxFuture<'static, ()>),
        WillPauseSoon(BoxFuture<'static, ()>, Instant),
        Paused
    }

    let mut state = State::WillPauseSoon(
        run_subscription(client.clone(), Some(updates), tx.clone()).boxed(),
        Instant::now().add(Duration::from_secs(30))
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
                                Instant::now().add(Duration::from_secs(10))
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
                            run_subscription(client.clone(), None, tx.clone()).boxed()
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
    mut tx: tokio::sync::broadcast::Sender<BlockJson>
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
        match receive_updates(updates, &mut tx, &mut errors).await {
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
    errors: &mut usize
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

                let delay = current_time - block_time * 1000;

                info!(slot = block.slot, delay_ms = delay, "new block");

                match render_block(&block) {
                    Ok(json) => {
                        // info!("{}", json);
                        let _ = tx.send(json.into());
                    },
                    Err(_) => {

                    }
                }
            }
        }
    }
    Ok(())
}


fn render_block(block: &SubscribeUpdateBlock) -> anyhow::Result<String> {
    let mut json = JsonBuilder::new();
    json.begin_object();
    safe_prop!(json, "slot", json.number(block.slot));
    safe_prop!(json, "block", {
        json.begin_object();
        safe_prop!(json, "blockhash", json.safe_str(&block.blockhash));
        safe_prop!(json, "previousBlockhash", json.safe_str(&block.parent_blockhash));
        safe_prop!(json, "parentSlot", json.number(block.parent_slot));
        safe_prop!(json, "blockHeight", if let Some(h) = block.block_height {
            json.number(h.block_height)
        } else {
            json.null();
        });
        safe_prop!(json, "blockTime", if let Some(h) = block.block_time {
            json.number(h.timestamp)
        } else {
            json.null()
        });
        safe_prop!(json, "transactions", {
            let mut order: Vec<_> = (0..block.transactions.len()).collect();
            order.sort_by_key(|i| block.transactions[*i].index);
            json.begin_array();
            for i in order {
                let tx = &block.transactions[i];
                render_transaction(&mut json, tx)?;
                json.comma();
            }
            json.end_array();
        });
        json.end_object();
    });
    json.end_object();
    Ok(
        json.into_string()
    )
}


fn render_transaction(json: &mut JsonBuilder, tx: &SubscribeUpdateTransactionInfo) -> anyhow::Result<()> {
    let t = tx.transaction.as_ref()
        .ok_or_else(|| anyhow!(".transaction is missing from transaction record"))?;

    let msg = t.message.as_ref()
        .ok_or_else(|| anyhow!(".transaction.message is missing from transaction record"))?;

    let hdr = msg.header.as_ref()
        .ok_or_else(|| anyhow!(".transaction.message.header is missing from transaction record"))?;

    let meta = tx.meta.as_ref()
        .ok_or_else(|| anyhow!(".meta is missing from transaction record"))?;

    json.begin_object();

    safe_prop!(json, "version", if msg.versioned {
        json.number(1);
    } else {
        json.safe_str("legacy");
    });

    safe_prop!(json, "transaction", {
        json.begin_object();
        safe_prop!(json, "message", {
            json.begin_object();
            safe_prop!(json, "header", {
                json.begin_object();
                safe_prop!(json, "numRequiredSignatures", json.number(hdr.num_required_signatures));
                safe_prop!(json, "numReadonlySignedAccounts", json.number(hdr.num_readonly_signed_accounts));
                safe_prop!(json, "numReadonlyUnsignedAccounts", json.number(hdr.num_readonly_unsigned_accounts));
                json.end_object();
            });
            safe_prop!(json, "accountKeys", {
                json.begin_array();
                for acc in msg.account_keys.iter() {
                    json.base58(acc);
                    json.comma();
                }
                json.end_array();
            });
            safe_prop!(json, "instructions", {
                json.begin_array();
                for ins in msg.instructions.iter() {
                    json.begin_object();
                    safe_prop!(json, "programIdIndex", json.number(ins.program_id_index));
                    safe_prop!(json, "accounts", {
                        json.begin_array();
                        for i in ins.accounts.iter().copied() {
                            json.number(i);
                            json.comma();
                        }
                        json.end_array();
                    });
                    safe_prop!(json, "data", json.base58(&ins.data));
                    safe_prop!(json, "stackHeight", json.null());
                    json.end_object();
                    json.comma();
                }
                json.end_array();
            });
            safe_prop!(json, "addressTableLookups", {
                json.begin_array();
                for lookup in msg.address_table_lookups.iter() {
                    json.begin_object();
                    safe_prop!(json, "accountKey", json.base58(&lookup.account_key));
                    safe_prop!(json, "readonlyIndexes", {
                        json.begin_array();
                        for idx in lookup.readonly_indexes.iter().copied() {
                            json.number(idx);
                            json.comma();
                        }
                        json.end_array();
                    });
                    safe_prop!(json, "writableIndexes", {
                        json.begin_array();
                        for idx in lookup.writable_indexes.iter().copied() {
                            json.number(idx);
                            json.comma();
                        }
                        json.end_array();
                    });
                    json.end_object();
                    json.comma();
                }
                json.end_array();
            });
            safe_prop!(json, "recentBlockhash", json.base58(&msg.recent_blockhash));
            json.end_object();
        });
        safe_prop!(json, "signatures", {
            json.begin_array();
            for sig in t.signatures.iter() {
                json.base58(sig);
                json.comma();
            }
            json.end_array();
        });
        json.end_object();
    });

    safe_prop!(json, "meta", {
        json.begin_object();
        safe_prop!(json, "computeUnitsConsumed", if let Some(units) = meta.compute_units_consumed {
            json.number(units);
        } else {
            json.null();
        });
        safe_prop!(json, "err", if let Some(_err) = meta.err.as_ref() {
            // FIXME: decode error
            json.null();
        } else {
            json.null();
        });
        safe_prop!(json, "fee", json.number(meta.fee));
        safe_prop!(json, "preBalances", {
            json.begin_array();
            for b in meta.pre_balances.iter().copied() {
                json.number(b);
                json.comma();
            }
            json.end_array();
        });
        safe_prop!(json, "postBalances", {
            json.begin_array();
            for b in meta.post_balances.iter().copied() {
                json.number(b);
                json.comma();
            }
            json.end_array();
        });
        safe_prop!(json, "preTokenBalances", render_token_balances(json, &meta.pre_token_balances)?);
        safe_prop!(json, "postTokenBalances", render_token_balances(json, &meta.post_token_balances)?);
        // FIXME: map rest
        json.end_object();
    });

    json.end_object();
    Ok(())
}


fn render_token_balances(json: &mut JsonBuilder, balances: &[TokenBalance]) -> anyhow::Result<()> {
    json.begin_array();
    for b in balances.iter() {
        json.begin_object();
        safe_prop!(json, "accountIndex", json.number(b.account_index));
        safe_prop!(json, "mint", json.safe_str(&b.mint));
        safe_prop!(json, "owner", json.safe_str(&b.owner));
        safe_prop!(json, "programId", json.safe_str(&b.program_id));

        let ui = b.ui_token_amount.as_ref()
            .ok_or_else(|| anyhow!(".ui_token_amount is empty"))?;

        safe_prop!(json, "uiTokenAmount", {
            json.begin_object();
            safe_prop!(json, "amount", json.safe_str(&ui.amount));
            safe_prop!(json, "decimals", json.number(ui.decimals));
            safe_prop!(json, "uiAmount", json.number(ui.ui_amount));
            safe_prop!(json, "uiAmountString", json.safe_str(&ui.ui_amount_string));
            json.end_object();
        });
        json.end_object();
        json.comma();
    }
    json.end_array();
    Ok(())
}
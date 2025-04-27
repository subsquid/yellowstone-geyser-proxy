use crate::geyser::api::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};
use crate::geyser::solana::storage::confirmed_block::{Reward, TokenBalance};
use crate::json_builder::{safe_prop, JsonBuilder};
use anyhow::{anyhow, bail, Context};
use lexical_core::ToLexical;
use solana_transaction_error::TransactionError;


pub fn render_block(block: &SubscribeUpdateBlock) -> anyhow::Result<String> {
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
            use rayon::prelude::*;

            let mut order: Vec<_> = (0..block.transactions.len()).collect();
            order.sort_by_key(|i| block.transactions[*i].index);

            let transactions = order.par_iter().map(|&i| {
                let tx = &block.transactions[i];
                let mut json = JsonBuilder::new();
                render_transaction(&mut json, tx)?;
                Ok(json.into_string())
            }).collect::<anyhow::Result<Vec<_>>>()?;

            json.begin_array();
            for tx in transactions {
                json.raw(&tx);
                json.comma();
            }
            json.end_array();
        });
        if let Some(rewards) = block.rewards.as_ref() {
            safe_prop!(json, "rewards", render_rewards(&mut json, &rewards.rewards)?);
        }
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
            safe_prop!(json, "accountKeys", render_base58_list(json, &msg.account_keys));
            safe_prop!(json, "instructions", {
                json.begin_array();
                for ins in msg.instructions.iter() {
                    json.begin_object();
                    safe_prop!(json, "programIdIndex", json.number(ins.program_id_index));
                    safe_prop!(json, "accounts", render_number_list(json, &ins.accounts));
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
                    safe_prop!(json, "readonlyIndexes", render_number_list(json, &lookup.readonly_indexes));
                    safe_prop!(json, "writableIndexes", render_number_list(json, &lookup.writable_indexes));
                    json.end_object();
                    json.comma();
                }
                json.end_array();
            });
            safe_prop!(json, "recentBlockhash", json.base58(&msg.recent_blockhash));
            json.end_object();
        });
        safe_prop!(json, "signatures", render_base58_list(json, &t.signatures));
        json.end_object();
    });

    safe_prop!(json, "meta", {
        json.begin_object();
        safe_prop!(json, "computeUnitsConsumed", if let Some(units) = meta.compute_units_consumed {
            json.number(units);
        } else {
            json.null();
        });
        safe_prop!(json, "err", if let Some(err) = meta.err.as_ref() {
            let err: TransactionError = bincode::deserialize(&err.err).with_context(|| {
                anyhow!(
                    "failed to deserialize error of transaction {}",
                    bs58::encode(&t.signatures[0]).into_string()
                )
            })?;
            json.value(&err);
        } else {
            json.null();
        });
        safe_prop!(json, "fee", json.number(meta.fee));
        safe_prop!(json, "preBalances", render_number_list(json, &meta.pre_balances));
        safe_prop!(json, "postBalances", render_number_list(json, &meta.post_balances));
        safe_prop!(json, "preTokenBalances", render_token_balances(json, &meta.pre_token_balances)?);
        safe_prop!(json, "postTokenBalances", render_token_balances(json, &meta.post_token_balances)?);
        safe_prop!(json, "innerInstructions", {
            if meta.inner_instructions_none {
                json.null();
            } else {
                json.begin_array();
                for ins in meta.inner_instructions.iter() {
                    json.begin_object();
                    safe_prop!(json, "index", json.number(ins.index));
                    safe_prop!(json, "instructions", {
                        json.begin_array();
                        for inner in ins.instructions.iter() {
                            json.begin_object();
                            safe_prop!(json, "programIdIndex", json.number(inner.program_id_index));
                            safe_prop!(json, "accounts", render_number_list(json, &inner.accounts));
                            safe_prop!(json, "data", json.base58(&inner.data));
                            safe_prop!(json, "stackHeight", if let Some(h) = inner.stack_height {
                                json.number(h);
                            } else {
                                json.null();
                            });
                            json.end_object();
                            json.comma();
                        }
                        json.end_array();
                    });
                    json.end_object();
                    json.comma();
                }
                json.end_array();
            }
        });
        safe_prop!(json, "loadedAddresses", {
            json.begin_object();
            safe_prop!(json, "readonly", render_base58_list(json, &meta.loaded_readonly_addresses));
            safe_prop!(json, "writable", render_base58_list(json, &meta.loaded_writable_addresses));
            json.end_object();
        });
        safe_prop!(json, "logMessages", {
            json.begin_array();
            for msg in meta.log_messages.iter() {
                json.str(msg);
                json.comma();
            }
            json.end_array();
        });
        safe_prop!(json, "rewards", render_rewards(json, &meta.rewards)?);
        if let Some(data) = meta.return_data.as_ref() {
            safe_prop!(json, "returnData", {
                json.begin_object();
                safe_prop!(json, "programId", json.base58(&data.program_id));
                safe_prop!(json, "data", {
                    json.begin_array();
                    json.base64(&data.data);
                    json.comma();
                    json.safe_str("base64");
                    json.end_array();
                });
                json.end_object();
            });
        }
        json.end_object();
    });

    json.end_object();
    Ok(())
}


fn render_base58_list(json: &mut JsonBuilder, list: &[Vec<u8>]) {
    json.begin_array();
    for item in list.iter() {
        json.base58(item);
        json.comma();
    }
    json.end_array();
}


fn render_number_list<T: ToLexical>(json: &mut JsonBuilder, list: &[T]) {
    json.begin_array();
    for i in list.iter().copied() {
        json.number(i);
        json.comma();
    }
    json.end_array();
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


fn render_rewards(json: &mut JsonBuilder, rewards: &[Reward]) -> anyhow::Result<()> {
    json.begin_array();
    for reward in rewards.iter() {
        json.begin_object();
        safe_prop!(json, "pubkey", json.safe_str(&reward.pubkey));
        safe_prop!(json, "lamports", json.number(reward.lamports));
        safe_prop!(json, "postBalance", json.number(reward.post_balance));
        safe_prop!(json, "rewardType", {
            match reward.reward_type {
                0 => json.safe_str("Unspecified"),
                1 => json.safe_str("Fee"),
                2 => json.safe_str("Rent"),
                3 => json.safe_str("Staking"),
                4 => json.safe_str("Voting"),
                ty => bail!("unknown reward type - {}", ty)
            }
        });
        safe_prop!(json, "commission", {
            if reward.commission.is_empty() {
                json.null();
            } else {
                let commission: u8 = reward.commission.parse().context("invalid Reward.commission")?;
                json.number(commission);
            }
        });
        json.end_object();
        json.comma();
    }
    json.end_array();
    Ok(())
}
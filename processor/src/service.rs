use indexer_db::entity::{evm_logs::EvmLogs, usdt_transfers::UsdtTransfers};
use indexer_metrics::{
    PROCESSOR_LOGS_PROCESSED, PROCESSOR_LOGS_ERRORS, PROCESSOR_PROCESSING_DURATION,
    PROCESSOR_BATCH_SIZE, PROCESSOR_CLEANUP_REMOVED,
};
use sqlx::{Pool, Postgres};
use std::{collections::BTreeMap, env, error::Error};

use crate::{ContractHandler, contracts::ContractRegistry, defaults, error::AppError, utils};

/// Process logs with guaranteed ordering:
/// - Blocks processed in ascending order
/// - Transactions within block processed in order
/// - Logs within transaction processed by log_index
/// This prevents race conditions and ensures correct state derivation
pub async fn process_logs(db_pool: &Pool<Postgres>) -> Result<(), Box<dyn Error>> {
    let contract_registry = ContractRegistry::new()?;
    let batch_size: i32 = env::var("BATCH_SIZE")
        .or::<String>(Ok(defaults::BATCH_SIZE.into()))?
        .parse::<i32>()?;

    // Get chain_id from environment
    let chain_id_str = env::var("CHAIN_ID")
        .map_err(|_| "Missing CHAIN_ID environment variable")?;
    let chain_id: i64 = chain_id_str
        .parse()
        .map_err(|_| format!("Invalid CHAIN_ID: {}", chain_id_str))?;

    let unprocessed_logs = EvmLogs::find_unprocessed(batch_size, db_pool).await?;

    if unprocessed_logs.is_empty() {
        return Ok(());
    }

    PROCESSOR_BATCH_SIZE
        .with_label_values(&[&chain_id_str])
        .set(unprocessed_logs.len() as f64);

    // Group logs by block_number, then by transaction_hash, maintaining order
    // BTreeMap ensures sorted order by key
    let mut logs_by_block: BTreeMap<String, BTreeMap<String, Vec<EvmLogs>>> = BTreeMap::new();

    for log in unprocessed_logs {
        // Convert block_number (BigDecimal) to string for sorting
        let block_num_str = log.block_number.to_string();
        
        // Convert transaction_hash to hex string for grouping
        let tx_hash_hex = format!("0x{}", utils::vec_to_hex(log.transaction_hash.to_vec()));

        logs_by_block
            .entry(block_num_str)
            .or_insert_with(BTreeMap::new)
            .entry(tx_hash_hex)
            .or_insert_with(Vec::new)
            .push(log);
    }

    // Process in strict order: block → transaction → log
    let mut processed_count = 0;
    let mut error_count = 0;

    for (_block_num_str, txs) in logs_by_block {
        for (_tx_hash_hex, mut logs) in txs {
            // Sort logs by log_index within transaction
            logs.sort_by_key(|l| l.log_index);

            // Process logs sequentially within transaction
            for log in logs {
                let processor_result = contract_registry.get_processor(log.address);
                let contract_name = contract_registry.get_contract_name(log.address)
                    .unwrap_or_else(|| "unknown".to_string());
                let log_id = log.id; // Store ID before moving log

                match processor_result {
                    Ok(processor) => {
                        let timer = PROCESSOR_PROCESSING_DURATION
                            .with_label_values(&[&chain_id_str, &contract_name])
                            .start_timer();

                        match processor.process(log, db_pool, chain_id).await {
                            Ok(_) => {
                                if let Err(error) = EvmLogs::mark_as_processed(log_id, db_pool).await {
                                    timer.observe_duration();
                                    eprintln!("Failed to mark log {} as final: {}", log_id, error);
                                    error_count += 1;
                                    PROCESSOR_LOGS_ERRORS
                                        .with_label_values(&[&chain_id_str, &contract_name, "mark_processed"])
                                        .inc();
                                } else {
                                    timer.observe_duration();
                                    processed_count += 1;
                                    PROCESSOR_LOGS_PROCESSED
                                        .with_label_values(&[&chain_id_str, &contract_name])
                                        .inc();
                                }
                            }
                            Err(error) => {
                                timer.observe_duration();
                                eprintln!("Failed to process log {}: {}", log_id, error);
                                error_count += 1;
                                PROCESSOR_LOGS_ERRORS
                                    .with_label_values(&[&chain_id_str, &contract_name, "process"])
                                    .inc();
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("Error processing log {}: {:?}", log_id, error);
                        if let AppError::Sqlx { source } = &error {
                            eprintln!("  SQLx error details: {}", source);
                        }
                        error_count += 1;
                        PROCESSOR_LOGS_ERRORS
                            .with_label_values(&[&chain_id_str, &contract_name, "get_processor"])
                            .inc();
                    }
                }
            }
        }
    }

    if processed_count > 0 {
        println!("Processed {} logs in order ({} errors)", processed_count, error_count);
    }

    Ok(())
}

/// Cleanup derived data for logs that were processed but then marked as removed (reorgs)
pub async fn cleanup_removed_logs(db_pool: &Pool<Postgres>) -> Result<(), Box<dyn Error>> {
    let batch_size: i32 = env::var("BATCH_SIZE")
        .or::<String>(Ok(defaults::BATCH_SIZE.into()))?
        .parse::<i32>()?;

    let chain_id_str = env::var("CHAIN_ID")
        .unwrap_or_else(|_| "0".to_string());

    let removed_logs = EvmLogs::find_processed_but_removed(batch_size, db_pool).await?;

    if removed_logs.is_empty() {
        return Ok(());
    }

    let logs_count = removed_logs.len();
    println!("Found {} processed logs marked as removed, cleaning up derived data...", logs_count);

    let mut cleaned_count = 0;
    let mut error_count = 0;

    for log in removed_logs {
        // Delete corresponding usdt_transfers for this log
        // The log's transaction_hash and block_number link to usdt_transfers
        match UsdtTransfers::delete_by_tx_hash_and_block(
            &log.transaction_hash,
            &log.block_number,
            db_pool,
        ).await {
            Ok(deleted_count) => {
                if deleted_count > 0 {
                    cleaned_count += deleted_count;
                    println!(
                        "Cleaned up {} transfer(s) for removed log {} (tx_hash: {:?}, block: {})",
                        deleted_count,
                        log.id,
                        log.transaction_hash,
                        log.block_number
                    );
                }
                // Reset is_processed to false so the log can be reprocessed if needed
                // (or we can leave it as true to indicate it was processed before removal)
                // For now, we'll leave is_processed = true to track that it was processed
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Failed to cleanup transfers for log {}: {}", log.id, e);
            }
        }
    }

    if cleaned_count > 0 {
        PROCESSOR_CLEANUP_REMOVED
            .with_label_values(&[&chain_id_str])
            .inc_by(cleaned_count as f64);
        println!("Cleanup complete: Removed {} transfer records from {} logs ({} errors)", 
                 cleaned_count, logs_count, error_count);
    }

    Ok(())
}

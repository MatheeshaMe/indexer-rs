use std::{env, str::FromStr, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    rpc::types::{Filter, BlockTransactionsKind},
};
use indexer_db::entity::{evm_logs::EvmLogs, evm_sync_logs::EvmSyncLogs};
use sqlx::{Pool, Postgres};
use tokio::time::sleep;

use crate::error::AppError;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ListenerMode {
    Backfill,
    LiveWs,
}

pub struct ListenerService {
    pub chain_id: u64,
    pub address: String,
    pub db_pool: Pool<Postgres>,
    #[allow(dead_code)] // Used for future mode-specific logic
    pub mode: ListenerMode,
}

// Safe head calculation: latest - finality blocks
// Default finality: 12 blocks (Ethereum), configurable via env
const DEFAULT_FINALITY_BLOCKS: u64 = 12;

fn get_finality_blocks() -> u64 {
    env::var("FINALITY_BLOCKS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_FINALITY_BLOCKS)
}

/// Calculate safe head (latest - finality blocks)
pub async fn get_safe_head(provider: &impl Provider) -> Result<u64, AppError> {
    let latest = provider
        .get_block_number()
        .await
        .map_err(|e| AppError::RPCError(format!("get_block_number failed: {e}")))?;

    let finality = get_finality_blocks();
    let safe_head = latest.saturating_sub(finality);

    Ok(safe_head)
}


/// Get starting block for backfill
/// Supports BACKFILL_FROM_BLOCK env var to start from a specific block
/// If more than 100 blocks behind, starts from latest - 100 for faster backfill
pub async fn get_backfill_start_block(
    provider: &impl Provider,
    address: Address,
    sync_log: &EvmSyncLogs,
) -> Result<u64, AppError> {
    // Check if BACKFILL_FROM_BLOCK is set
    if let Ok(from_block_str) = env::var("BACKFILL_FROM_BLOCK") {
        if let Ok(from_block) = from_block_str.parse::<u64>() {
            println!("Using BACKFILL_FROM_BLOCK: {}", from_block);
            return Ok(from_block);
        }
    }

    // Get latest block
    let latest_block = provider
        .get_block_number()
        .await
        .map_err(|e| AppError::RPCError(format!("get_block_number failed: {e}")))?;

    // If already synced, check if we're more than 1500 blocks behind
    if sync_log.last_synced_block_number > 0 {
        let last_synced = sync_log.last_synced_block_number as u64;
        let blocks_behind = latest_block.saturating_sub(last_synced);
        
        // If more than 1500 blocks behind, start from latest - 1500 for faster backfill
        if blocks_behind > 100 {
            let start_block = latest_block.saturating_sub(100);
            println!(
                "Last synced block {} is {} blocks behind latest. Starting backfill from {} (latest - 1500) for faster completion.",
                last_synced,
                blocks_behind,
                start_block
            );
            return Ok(start_block);
        }
        
        // Otherwise, continue from last synced block
        return Ok(last_synced + 1);
    }

    // If never synced, start from latest - 1500 (don't backfill from deployment)
    let start_block = latest_block.saturating_sub(100);
    println!(
        "No previous sync found. Starting backfill from {} (latest {} - 100) for faster completion.",
        start_block,
        latest_block
    );
    Ok(start_block)
}

/// Check for reorg by comparing block hash
pub async fn check_reorg(
    provider: &impl Provider,
    block_number: u64,
    expected_hash: Option<[u8; 32]>,
) -> Result<bool, AppError> {
    if expected_hash.is_none() {
        return Ok(false); // No previous hash to compare
    }

    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number), BlockTransactionsKind::Full)
        .await
        .map_err(|e| AppError::RPCError(format!("get_block_by_number failed: {e}")))?;

    if let Some(block) = block {
        let block_hash = block.header.hash;
        let actual_hash: [u8; 32] = block_hash.into();
        let expected = expected_hash.unwrap();

        if actual_hash != expected {
            return Ok(true); // Reorg detected
        }
    }

    Ok(false)
}

/// Fetch and save logs (used by both backfill and WS)
pub async fn fetch_and_save_logs(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
    from_block: u64,
    to_block: u64,
) -> Result<u64, AppError> {
    let rpc_url: String =
        env::var("RPC_URL").map_err(|_| AppError::MissingEnvVar("RPC_URL".into()))?;

    let provider = ProviderBuilder::new()
        .on_builtin(&rpc_url)
        .await
        .map_err(|e| AppError::RPCError(format!("Provider Error Happened {}", e)))?;

    let contract_address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;

    let filter = Filter::new()
        .address(contract_address)
        .from_block(BlockNumberOrTag::Number(from_block))
        .to_block(BlockNumberOrTag::Number(to_block));

    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| AppError::RPCError(format!("Error in getting logs {}", e)))?;

    println!(
        "Fetched {} logs from RPC for blocks {} to {}",
        logs.len(),
        from_block,
        to_block
    );

    if logs.is_empty() {
        return Ok(to_block);
    }

    // Get block hash for reorg detection (optional - column may not exist yet)
    let block_hash = match provider
        .get_block_by_number(BlockNumberOrTag::Number(to_block), BlockTransactionsKind::Full)
        .await
    {
        Ok(block) => block.map(|b| -> [u8; 32] { b.header.hash.into() }),
        Err(e) => {
            eprintln!("Warning: Could not fetch block hash for reorg detection: {e}");
            None // Continue without block hash if RPC fails
        }
    };

    // Check for reorg at the block we're about to save
    let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
    if let Some(ref sync) = sync_log {
        if sync.last_synced_block_number == to_block as i64 {
            // We've already synced this block, check for reorg
            if let Some(ref expected_hash_vec) = sync.last_synced_block_hash {
                if expected_hash_vec.len() == 32 {
                    let mut expected_hash = [0u8; 32];
                    expected_hash.copy_from_slice(expected_hash_vec);
                    let reorg = check_reorg(&provider, to_block, Some(expected_hash)).await?;
                    if reorg {
                        return Err(AppError::ReorgDetected(to_block));
                    }
                }
            }
        }
    }

    let logs_count = logs.len();
    let mut tx = db_pool.begin().await?;
    let mut saved_count = 0;
    let mut error_count = 0;

    for log in logs {
        match EvmLogs::create(log, &mut *tx).await {
            Ok(_) => {
                saved_count += 1;
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Failed to save log: {:?}", e);
                // Continue processing other logs
            }
        }
    }

    println!(
        "Saved {}/{} logs successfully ({} errors)",
        saved_count,
        logs_count,
        error_count
    );

    // Update sync log with block number and hash
    if let Some(sync) = sync_log {
        sync.update_last_synced_block_number(to_block, &mut *tx).await
            .map_err(|e| AppError::EVMLog(format!("Error updating last_synced_block_number {}", e)))?;

        // Update block hash if available
        if let Some(hash) = block_hash {
            sqlx::query(
                "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2",
            )
            .bind(&hash[..])
            .bind(&sync.address[..])
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::Database(e))?;
        }
    } else {
        // Create new sync log
        let new_sync = EvmSyncLogs::create(&address, chain_id, Some(to_block as i64), &mut *tx)
            .await?;

        if let Some(hash) = block_hash {
            sqlx::query(
                "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2",
            )
            .bind(&hash[..])
            .bind(&new_sync.address[..])
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::Database(e))?;
        }
    }

    match tx.commit().await {
        Ok(_) => {
            println!(
                "Committed transaction: Saved {} logs for {address}, blocks: {from_block} to {to_block}",
                saved_count
            );
            Ok(to_block)
        }
        Err(e) => {
            eprintln!("Transaction commit failed: {:?}", e);
            Err(AppError::Database(e))
        }
    }
}

/// Run backfill once (returns when safe_head is reached or error)
pub async fn run_backfill_once(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
    #[allow(unused_variables)] block_time: u64, // Reserved for future rate limiting if needed
) -> Result<(), AppError> {
    let rpc_url: String =
        env::var("RPC_URL").map_err(|_| AppError::MissingEnvVar("RPC_URL".into()))?;

    let provider = ProviderBuilder::new()
        .on_builtin(&rpc_url)
        .await
        .map_err(|e| AppError::RPCError(format!("Provider Error Happened {}", e)))?;

    let contract_address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;

    let sync_log = EvmSyncLogs::find_or_create_by_address(&address, chain_id, &db_pool).await?;

    // Get latest block and safe head
    let latest_block = provider
        .get_block_number()
        .await
        .map_err(|e| AppError::RPCError(format!("get_block_number failed: {e}")))?;
    
    let safe_head = get_safe_head(&provider).await?;
    let finality = get_finality_blocks();
    
    // Get starting block
    let from_block = get_backfill_start_block(&provider, contract_address, &sync_log).await?;
    let mut current_block = from_block;
    
    // Calculate progress
    let total_blocks_to_sync = safe_head.saturating_sub(from_block) + 1;
    let blocks_synced = current_block.saturating_sub(from_block);
    
    println!("\n Backfill Progress for {address}:");
    println!("   Latest Block:        {}", latest_block);
    println!("   Safe Head:           {} (latest - {} finality blocks)", safe_head, finality);
    println!("   Starting Block:      {}", from_block);
    println!("   Current Block:       {}", current_block);
    println!("   Blocks Behind:       {} blocks", latest_block.saturating_sub(current_block));
    println!("   Blocks Remaining:    {} blocks", safe_head.saturating_sub(current_block) + 1);
    println!("   Total to Sync:       {} blocks", total_blocks_to_sync);
    if total_blocks_to_sync > 0 {
        let progress_pct = (blocks_synced as f64 / total_blocks_to_sync as f64 * 100.0).min(100.0);
        println!("   Progress:            {:.2}% ({}/{} blocks)", progress_pct, blocks_synced, total_blocks_to_sync);
    }
    println!();

    // Backfill loop
    loop {
        // Check if we've reached safe head
        if current_block > safe_head {
            println!("Backfill complete for {address}: reached safe_head={safe_head}");
            return Err(AppError::BackfillComplete);
        }

        // Calculate block range (fetch up to 10 blocks at a time)
        let to_block = std::cmp::min(current_block + 9, safe_head);

        // No sleep in backfill - maximize speed
        // Fetch and save logs
        match fetch_and_save_logs(chain_id, db_pool.clone(), address.clone(), current_block, to_block)
            .await
        {
            Ok(last_synced) => {
                // Update latest block for progress calculation
                let updated_latest = provider
                    .get_block_number()
                    .await
                    .unwrap_or(latest_block);
                
                // Calculate and display progress
                let blocks_behind = updated_latest.saturating_sub(last_synced);
                let blocks_remaining = safe_head.saturating_sub(last_synced);
                let total_synced = last_synced.saturating_sub(from_block) + 1;
                
                if total_blocks_to_sync > 0 {
                    let progress_pct = (total_synced as f64 / total_blocks_to_sync as f64 * 100.0).min(100.0);
                    println!(
                        "Synced blocks {}-{} | Behind latest: {} blocks | Remaining: {} blocks | Progress: {:.2}% ({}/{})",
                        current_block,
                        last_synced,
                        blocks_behind,
                        blocks_remaining,
                        progress_pct,
                        total_synced,
                        total_blocks_to_sync
                    );
                } else {
                    println!(
                        "Synced blocks {}-{} | Behind latest: {} blocks | Remaining: {} blocks",
                        current_block,
                        last_synced,
                        blocks_behind,
                        blocks_remaining
                    );
                }
                
                current_block = last_synced + 1;

                // If we've reached safe_head, we're done
                if last_synced >= safe_head {
                    println!("\n Backfill complete for {address}:");
                    println!("   Reached safe_head:     {}", safe_head);
                    println!("   Final block synced:    {}", last_synced);
                    println!("   Latest block:          {}", updated_latest);
                    println!("   Blocks behind latest:  {} blocks", updated_latest.saturating_sub(last_synced));
                    return Err(AppError::BackfillComplete);
                }
            }
            Err(AppError::ReorgDetected(block)) => {
                eprintln!("Reorg detected at block {block} for {address}, rolling back...");
                // Rollback: set last_synced_block_number to block - 1
                let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
                if let Some(sync) = sync_log {
                    let rollback_block = block.saturating_sub(1);
                    sync.update_last_synced_block_number(rollback_block, &db_pool)
                        .await
                        .map_err(|e| AppError::Database(e))?;
                    current_block = rollback_block + 1;
                    println!("Rolled back to block {rollback_block}, resuming from {}", current_block);
                }
            }
            Err(e) => {
                eprintln!("Error during backfill for {address}: {:?}", e);
                return Err(e);
            }
        }
    }
}

/// Run WebSocket live listener (never returns)
pub async fn run_ws_forever(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
) -> std::result::Result<std::convert::Infallible, AppError> {
    let ws_url = env::var("WS_URL")
        .or_else(|_| env::var("RPC_URL").map(|url| url.replace("https://", "wss://").replace("http://", "ws://")))
        .map_err(|_| AppError::MissingEnvVar("WS_URL or RPC_URL".into()))?;

    println!("Starting WebSocket listener for {address} on {ws_url}");

    // For now, fallback to HTTP polling if WS is not available
    // TODO: Implement proper WebSocket subscription with alloy
    // This is a placeholder that uses HTTP polling as a fallback
    let rpc_url: String =
        env::var("RPC_URL").map_err(|_| AppError::MissingEnvVar("RPC_URL".into()))?;

    let provider = ProviderBuilder::new()
        .on_builtin(&rpc_url)
        .await
        .map_err(|e| AppError::RPCError(format!("Provider Error Happened {}", e)))?;

    let sync_log = EvmSyncLogs::find_or_create_by_address(&address, chain_id, &db_pool).await?;
    let from_block = sync_log.last_synced_block_number as u64 + 1;
    let mut current_block = from_block;

    // Live polling loop (infinite)
    loop {
        let latest_block = provider
            .get_block_number()
            .await
            .map_err(|e| AppError::RPCError(format!("Failed to get block number {}", e)))?;

        if current_block > latest_block {
            // Wait for new blocks
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        let to_block = std::cmp::min(current_block + 9, latest_block);

        match fetch_and_save_logs(chain_id, db_pool.clone(), address.clone(), current_block, to_block)
            .await
        {
            Ok(last_synced) => {
                current_block = last_synced + 1;
                println!("Live sync: {address} synced to block {last_synced}");
            }
            Err(AppError::ReorgDetected(block)) => {
                eprintln!("Reorg detected at block {block} for {address}, rolling back...");
                let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
                if let Some(sync) = sync_log {
                    let rollback_block = block.saturating_sub(1);
                    sync.update_last_synced_block_number(rollback_block, &db_pool)
                        .await
                        .map_err(|e| AppError::Database(e))?;
                    current_block = rollback_block + 1;
                }
            }
            Err(e) => {
                eprintln!("Error during live sync for {address}: {:?}, retrying...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }

        // Small delay between polls
        // sleep(Duration::from_secs(1)).await;
    }
}

impl ListenerService {
    pub async fn run_backfill(&self) -> Result<(), AppError> {
        let evm_chain = indexer_db::entity::evm_chains::EvmChains::fetch_by_id(
            self.chain_id,
            &self.db_pool,
        )
        .await
        .map_err(|e| AppError::Database(e))?;

        run_backfill_once(
            self.chain_id,
            self.db_pool.clone(),
            self.address.clone(),
            evm_chain.block_time as u64,
        )
        .await
    }

    pub async fn run_ws(&self) -> std::result::Result<std::convert::Infallible, AppError> {
        run_ws_forever(
            self.chain_id,
            self.db_pool.clone(),
            self.address.clone(),
        )
        .await
    }
}

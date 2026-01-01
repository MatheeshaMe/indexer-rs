use std::{env, str::FromStr, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    rpc::types::{Filter, BlockTransactionsKind},
};
use futures_util::StreamExt;
use indexer_db::entity::{blocks::Block, evm_logs::EvmLogs, evm_sync_logs::EvmSyncLogs};
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
    #[allow(dead_code)] // Reserved for future use
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

/// Verify block hash continuity (detect reorgs by checking parent hash)
/// Returns (current_block_hash, parent_hash) if valid, or error if reorg detected
pub async fn verify_block_hash_continuity(
    provider: &impl Provider,
    block_number: u64,
    expected_parent_hash: Option<[u8; 32]>,
) -> Result<([u8; 32], [u8; 32]), AppError> {
    let block = provider
        .get_block_by_number(
            BlockNumberOrTag::Number(block_number),
            BlockTransactionsKind::Full,
        )
            .await
        .map_err(|e| AppError::RPCError(format!("Failed to get block {}: {e}", block_number)))?
        .ok_or_else(|| AppError::RPCError(format!("Block {} not found", block_number)))?;

    let current_hash: [u8; 32] = block.header.hash.into();
    let parent_hash: [u8; 32] = block.header.parent_hash.into();

    // If we have expected parent hash, verify continuity
    if let Some(expected) = expected_parent_hash {
        if parent_hash != expected {
            return Err(AppError::ReorgDetected(block_number));
        }
    }

    Ok((current_hash, parent_hash))
}


/// Get starting block for backfill
/// Supports BACKFILL_FROM_BLOCK env var to start from a specific block
/// If more than 100 blocks behind, starts from latest - 100 for faster backfill
pub async fn get_backfill_start_block(
    provider: &impl Provider,
    _address: Address, // Used for deployment block detection if needed
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

/// Fetch and save logs for backfill (no reorg detection needed, blocks are final)
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
        // Even if no logs, store the last block in blocks table for WS verification
        if let Ok(Some(block)) = provider
            .get_block_by_number(BlockNumberOrTag::Number(to_block), BlockTransactionsKind::Full)
            .await
        {
            let hash: [u8; 32] = block.header.hash.into();
            let parent: [u8; 32] = block.header.parent_hash.into();
            if let Err(e) = Block::create_or_update(to_block, hash, parent, &db_pool).await {
                eprintln!("Warning: Failed to store block {} in blocks table: {}", to_block, e);
            }
        }
        return Ok(to_block);
    }

    // Store all blocks in the range in blocks table (needed for WS verification)
    // This ensures continuity checks work correctly when WS starts
    for block_num in from_block..=to_block {
        if let Ok(Some(block)) = provider
            .get_block_by_number(BlockNumberOrTag::Number(block_num), BlockTransactionsKind::Full)
            .await
        {
            let hash: [u8; 32] = block.header.hash.into();
            let parent: [u8; 32] = block.header.parent_hash.into();
            if let Err(e) = Block::create_or_update(block_num, hash, parent, &db_pool).await {
                eprintln!("Warning: Failed to store block {} in blocks table: {}", block_num, e);
            }
        }
    }

    // Get block hash for tracking (not for reorg detection in backfill - blocks are final)
    let block_hash = match provider
        .get_block_by_number(BlockNumberOrTag::Number(to_block), BlockTransactionsKind::Full)
        .await
    {
        Ok(block) => block.map(|b| -> [u8; 32] { b.header.hash.into() }),
        Err(e) => {
            eprintln!("Warning: Could not fetch block hash: {e}");
            None
        }
    };

    let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;

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

/// Fetch and save logs for WebSocket mode (handles `removed` field for reorg detection)
/// Includes block hash continuity verification for robust reorg detection
pub async fn fetch_and_save_logs_ws(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
    from_block: u64,
    to_block: u64,
    provider: &impl Provider,
) -> Result<u64, AppError> {
    let contract_address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;

    // CRITICAL: Verify block hash continuity before processing logs
    // This catches reorgs that the `removed` field might miss
    let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
    let expected_parent_hash = sync_log
        .as_ref()
        .and_then(|s| s.last_synced_block_hash.as_ref())
        .and_then(|h| {
            if h.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(h);
                Some(arr)
            } else {
                None
            }
        });

    // Verify each block in range for hash continuity and track in blocks table
    let mut verified_hashes = Vec::new();
    for block_num in from_block..=to_block {
        let expected = if block_num == from_block {
            expected_parent_hash
        } else {
            // For subsequent blocks, use previous block's hash as expected parent
            verified_hashes.last().map(|(hash, _)| *hash)
        };

        match verify_block_hash_continuity(provider, block_num, expected).await {
            Ok((current_hash, parent_hash)) => {
                // Store block in blocks table for tracking and analytics
                if let Err(e) = Block::create_or_update(block_num, current_hash, parent_hash, &db_pool).await {
                    eprintln!("Warning: Failed to store block {} in blocks table: {}", block_num, e);
                    // Continue processing even if block storage fails
                }
                verified_hashes.push((current_hash, parent_hash));
            }
            Err(AppError::ReorgDetected(block)) => {
                eprintln!("Reorg detected at block {} via hash continuity check", block);
                // Mark all logs from this block onwards as removed
                let address_array: [u8; 20] = (*contract_address).into();
                let mut tx = db_pool.begin().await?;
                let marked = EvmLogs::mark_removed_by_block_range(
                    block,
                    to_block,
                    &address_array,
                    &mut *tx,
                )
                .await
                .map_err(|e| AppError::Database(e))?;
                tx.commit().await?;
                
                if marked > 0 {
                    println!("Marked {} logs as removed due to reorg at block {}", marked, block);
                }
                
                return Err(AppError::ReorgDetected(block));
            }
            Err(e) => return Err(e),
        }
    }

    let filter = Filter::new()
        .address(contract_address)
        .from_block(BlockNumberOrTag::Number(from_block))
        .to_block(BlockNumberOrTag::Number(to_block));

    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| AppError::RPCError(format!("Error in getting logs {}", e)))?;

    println!(
        "Fetched {} logs from WebSocket for blocks {} to {} (hash continuity verified)",
        logs.len(),
        from_block,
        to_block
    );

    if logs.is_empty() {
        // Still update sync log even if no logs, but with verified block hash
        let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
        if let Some(sync) = sync_log {
            sync.update_last_synced_block_number(to_block, &db_pool).await
                .map_err(|e| AppError::Database(e))?;
            
            // Update block hash
            if let Some((hash, _)) = verified_hashes.last() {
                sqlx::query(
                    "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2"
                )
                .bind(&hash[..])
                .bind(&sync.address[..])
                .execute(&db_pool)
                .await
                .map_err(|e| AppError::Database(e))?;
            }
        } else {
            EvmSyncLogs::create(&address, chain_id, Some(to_block as i64), &db_pool).await?;
        }
        return Ok(to_block);
    }

    let mut tx = db_pool.begin().await?;
    let mut saved_count = 0;
    let mut removed_count = 0;
    let mut error_count = 0;

    // Separate logs by removed status
    let mut logs_to_save = Vec::new();
    let mut blocks_with_removed_logs = std::collections::HashSet::new();

    for log in logs {
        if log.removed {
            // Log was removed due to reorg - mark existing logs for this block as removed
            if let Some(block_num) = log.block_number {
                blocks_with_removed_logs.insert(block_num);
            }
            removed_count += 1;
        } else {
            // Normal log - save it
            logs_to_save.push(log);
        }
    }

    // Mark logs as removed for blocks that had removed logs
    if !blocks_with_removed_logs.is_empty() {
        let address_array: [u8; 20] = (*contract_address).into();

        for block_num in &blocks_with_removed_logs {
            let marked = EvmLogs::mark_removed_by_block_range(
                *block_num,
                *block_num,
                &address_array,
                &mut *tx,
            ).await
            .map_err(|e| AppError::Database(e))?;
            
            if marked > 0 {
                println!("Marked {} logs as removed for block {} (reorg detected)", marked, block_num);
            }
        }
    }

    // Save new logs (removed = false)
    for log in logs_to_save {
        match EvmLogs::create(log, &mut *tx).await {
            Ok(_) => {
                saved_count += 1;
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Failed to save log: {:?}", e);
            }
        }
    }

    println!(
        "Saved {}/{} new logs, marked {} logs as removed ({} errors)",
        saved_count,
        saved_count + removed_count,
        removed_count,
        error_count
    );

    // Update sync log with verified block hash
    let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
    if let Some(sync) = sync_log {
        sync.update_last_synced_block_number(to_block, &mut *tx).await
            .map_err(|e| AppError::Database(e))?;
        
        // Store the verified block hash for next continuity check
        if let Some((hash, _)) = verified_hashes.last() {
            sqlx::query(
                "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2"
            )
            .bind(&hash[..])
            .bind(&sync.address[..])
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::Database(e))?;
        }
    } else {
        let new_sync = EvmSyncLogs::create(&address, chain_id, Some(to_block as i64), &mut *tx).await?;
        
        // Store the verified block hash
        if let Some((hash, _)) = verified_hashes.last() {
            sqlx::query(
                "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2"
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
                "Committed transaction: Saved {} logs, marked {} removed for {address}, blocks: {from_block} to {to_block}",
                saved_count, removed_count
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
            Err(e) => {
                eprintln!("Error during backfill for {address}: {:?}", e);
                return Err(e);
            }
        }
    }
}

// / Run WebSocket live listener (never returns)
pub async fn run_ws_forever(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
) -> std::result::Result<std::convert::Infallible, AppError> {
    // Get WebSocket URL
    let ws_rpc_url = env::var("WS_RPC")
        .map_err(|_| AppError::MissingEnvVar("WS_RPC is missing in env".into()))?;

    println!("Starting WebSocket listener for {address} on {ws_rpc_url}");

    let contract_address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;

    // Get sync state
    let sync_log = EvmSyncLogs::find_or_create_by_address(&address, chain_id, &db_pool).await?;
    let mut current_block = sync_log.last_synced_block_number as u64 + 1;

    // WebSocket connection loop with reconnection
    loop {
        match try_ws_connection(
            &ws_rpc_url,
            chain_id,
            db_pool.clone(),
            address.clone(),
            contract_address,
            &mut current_block,
        )
        .await
        {
            Ok(_) => {
                // Connection ended normally (shouldn't happen)
                eprintln!("WebSocket connection closed unexpectedly for {address}, reconnecting...");
            }
            Err(e) => {
                eprintln!("WebSocket error for {address}: {:?}, reconnecting in 5 seconds...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Try to establish WebSocket connection and process blocks
async fn try_ws_connection(
    ws_url: &str,
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
    _contract_address: Address, // Reserved for future use
    current_block: &mut u64,
) -> std::result::Result<std::convert::Infallible, AppError> {
    //  uses WebSocket transport, otherwise falls back to HTTP
    let provider = ProviderBuilder::new()
        .on_builtin(ws_url)
        .await
        .map_err(|e| AppError::WebSocketError(format!("Failed to connect to WebSocket: {e}")))?;

    println!("WebSocket connected for {address}, starting block monitoring...");

    // First, catch up to latest block if we're behind
    let latest_block = provider
        .get_block_number()
        .await
        .map_err(|e| AppError::RPCError(format!("Failed to get block number: {e}")))?;

    if *current_block <= latest_block {
        let catchup_blocks = latest_block.saturating_sub(*current_block) + 1;
        println!("Catching up: {} blocks behind, syncing from {} to {}", 
                 catchup_blocks, *current_block, latest_block);
        
        // Catch up in batches using WebSocket (with removed field detection)
        while *current_block <= latest_block {
            let to_block = std::cmp::min(*current_block + 9, latest_block);
            
            match fetch_and_save_logs_ws(chain_id, db_pool.clone(), address.clone(), *current_block, to_block, &provider)
                .await
            {
                Ok(last_synced) => {
                    *current_block = last_synced + 1;
                    if last_synced % 10 == 0 || last_synced == latest_block {
                        println!("Caught up to block {} ({} remaining)", 
                                 last_synced, latest_block.saturating_sub(last_synced));
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        
        println!("Catchup complete, now listening for new blocks via WebSocket...");
    }

    // Live mode: Use WebSocket subscriptions for push-based event processing
    // Subscribe to logs from the contract address
    println!("Subscribing to logs for contract {address} via WebSocket...");
    
    let contract_address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;
    
    // Create filter for contract address
    let filter = Filter::new()
        .address(contract_address)
        .from_block(BlockNumberOrTag::Latest);
    
    // Subscribe to logs (push-based, not polling)
    let subscription = provider
        .subscribe_logs(&filter)
        .await
        .map_err(|e| AppError::WebSocketError(format!("Failed to subscribe to logs: {e}")))?;
    
    println!("Successfully subscribed to logs via WebSocket (push-based)");
    
    // Convert subscription to stream
    let mut log_stream = subscription.into_stream();
    
    // Track last verified block to avoid repeated checks
    let mut last_verified_block: Option<u64> = None;
    let mut last_verified_hash: Option<[u8; 32]> = None;
    
    // Process logs as they arrive (push-based)
    while let Some(log) = log_stream.next().await {
        // Get block number from log
        let block_number = log.block_number
            .ok_or_else(|| AppError::RPCError("Log missing block number".into()))?;
        
        // CRITICAL: Skip logs from blocks this already rolled back from
        // This prevents processing stale logs from reorged blocks
        if block_number < *current_block {
            // This log is from a block already processed or rolled back from
            // Skip it - it's either stale or from a reorged block that already handled
            continue;
        }
        
        // Check if log is removed (reorg detection)
        if log.removed {
            // Mark existing logs for this block as removed
            let address_array: [u8; 20] = contract_address.into();
            let mut tx = db_pool.begin().await?;
            
            let marked = EvmLogs::mark_removed_by_block_range(
                block_number,
                block_number,
                &address_array,
                &mut *tx,
            )
            .await
            .map_err(|e| AppError::Database(e))?;
            
            tx.commit().await?;
            
            if marked > 0 {
                println!("Reorg detected: Marked {} logs as removed for block {} (via subscription removed field)", marked, block_number);
            }
            
            // Reset verification cache for this block
            if last_verified_block == Some(block_number) {
                last_verified_block = None;
                last_verified_hash = None;
            }
            
            continue; // Skip processing removed logs
        }
        
        // Normal log - verify block hash continuity (only once per block)
        let needs_verification = last_verified_block != Some(block_number);
        
        if needs_verification {
            // Check if parent block exists in blocks table
            let parent_block_num = block_number.saturating_sub(1);
            let parent_block_in_db = Block::find_by_number(parent_block_num, &db_pool).await.ok().flatten();
            
            // CRITICAL FIX: Only verify if parent block exists in blocks table
            // Do NOT fall back to last_synced_block_hash - it can be stale and cause false positives
            // This is because backfill doesn't store blocks in blocks table, only updates evm_sync_logs
            if let Some(parent_block) = &parent_block_in_db {
                // Parent block exists in DB - use its hash for verification (reliable)
                let expected_parent_hash = Some(parent_block.block_hash);
                // Verify block hash continuity
                match verify_block_hash_continuity(
                    &provider,
                    block_number,
                    expected_parent_hash,
                ).await {
                    Ok((current_hash, _parent_hash)) => {
                        // Verification successful - cache the result
                        last_verified_block = Some(block_number);
                        last_verified_hash = Some(current_hash);
                    }
                    Err(AppError::ReorgDetected(block)) => {
                        // Reorg detected - handle it properly
                        eprintln!("Reorg detected at block {} via hash continuity - handling reorg...", block);
                    
                    // Mark all logs from the reorged block onwards as removed
                    let address_array: [u8; 20] = contract_address.into();
                    let mut tx = db_pool.begin().await?;
                    
                    // Roll back to block - 1
                    let rollback_block = block.saturating_sub(1);
                    let marked = EvmLogs::mark_removed_by_block_range(
                        block,
                        block_number, // Mark all logs up to current block
                        &address_array,
                        &mut *tx,
                    )
                    .await
                    .map_err(|e| AppError::Database(e))?;
                    
                    // Update sync state to rollback block
                    let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
                    if let Some(sync) = sync_log {
                        sync.update_last_synced_block_number(rollback_block, &mut *tx).await
                            .map_err(|e| AppError::Database(e))?;
                        
                        // Clear block hash since we rolled back
                        sqlx::query(
                            "UPDATE evm_sync_logs SET last_synced_block_hash = NULL WHERE address = $1"
                        )
                        .bind(&sync.address[..])
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| AppError::Database(e))?;
                    }
                    
                    tx.commit().await?;
                    
                    if marked > 0 {
                        println!("Reorg handled: Marked {} logs as removed, rolled back to block {}", marked, rollback_block);
                    }
                    
                    // Reset verification cache
                    last_verified_block = None;
                    last_verified_hash = None;
                    *current_block = rollback_block + 1;
                    
                    println!("Reorg handled: Rolled back to block {}, skipping logs from blocks < {}", rollback_block, *current_block);
                    
                    // Skip this log - it's from a reorged block
                    // Future logs from this block will also be skipped by the block_number < current_block check
                    continue;
                }
                    Err(e) => {
                        eprintln!("Error verifying block hash continuity: {:?}", e);
                        continue; // Skip this log on error
                    }
                }
            } else {
                // Parent block not in blocks table - skip verification to avoid false positives
                // This can happen if:
                // 1. First WS block after backfill (backfill doesn't store blocks in blocks table)
                // 2. Out-of-order block delivery
                // We'll verify it when the parent block is stored (after it's processed)
            }
        }
        
        // Extract block_hash before moving log
        let log_block_hash = log.block_hash;
        
        // Store block in blocks table if not already stored (needed for future continuity checks)
        // This is important even if verification was skipped
        if last_verified_block != Some(block_number) {
            // Fetch block from RPC to get hash and parent hash
            if let Ok(Some(block)) = provider
                .get_block_by_number(BlockNumberOrTag::Number(block_number), BlockTransactionsKind::Full)
                .await
            {
                let current_hash: [u8; 32] = block.header.hash.into();
                let parent_hash: [u8; 32] = block.header.parent_hash.into();
                if let Err(e) = Block::create_or_update(block_number, current_hash, parent_hash, &db_pool).await {
                    eprintln!("Warning: Failed to store block {} in blocks table: {}", block_number, e);
                } else {
                    // Cache the hash for sync state update
                    last_verified_hash = Some(current_hash);
                }
            }
        }
                    
        // Save the log (block already verified or stored)
        match EvmLogs::create(log, &db_pool).await {
            Ok(_saved_log) => {
                // Update sync state (only once per block, use cached hash if available)
                // Update even if verification was skipped, as long as we have a hash
                let sync_log = EvmSyncLogs::find_by_address(&address, &db_pool).await?;
                if let Some(sync) = sync_log {
                    // Only update if we haven't already synced this block
                    if sync.last_synced_block_number < block_number as i64 {
                        sync.update_last_synced_block_number(block_number, &db_pool).await
                            .map_err(|e| AppError::Database(e))?;
                        
                        // Store block hash (use cached hash from verification or block fetch)
                        if let Some(hash) = last_verified_hash.or_else(|| log_block_hash.map(|h| h.into())) {
                            sqlx::query(
                                "UPDATE evm_sync_logs SET last_synced_block_hash = $1 WHERE address = $2"
                            )
                            .bind(&hash[..])
                            .bind(&sync.address[..])
                            .execute(&db_pool)
                            .await
                            .map_err(|e| AppError::Database(e))?;
                        }
                    }
                }
                
                println!("Received and saved log from block {} (via WebSocket subscription)", block_number);
                if *current_block <= block_number {
                    *current_block = block_number + 1;
                }
            }
            Err(e) => {
                eprintln!("Failed to save log from subscription: {:?}", e);
                // Continue processing other logs
            }
        }
    }
    
    // Stream ended (shouldn't happen, but handle gracefully)
    eprintln!("WebSocket subscription stream ended unexpectedly");
    Err(AppError::WebSocketError("Subscription stream ended".into()))
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

use std::{
    env,future::Future, pin::Pin, str::FromStr, task::{Context, Poll}
};


use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
};
use indexer_db::entity::{evm_logs::EvmLogs, evm_sync_logs::EvmSyncLogs};
use sqlx::{Pool, Postgres};
use tower::Service;

use crate::error::AppError;

pub struct ListenerService {
    pub chain_id: u64,
    pub address: String,
    pub db_pool: Pool<Postgres>,
}

pub async fn find_deployment_block(
    provider: &impl Provider,
    address: Address,
) -> Result<u64, AppError> {
    let latest = provider
        .get_block_number()
        .await
        .map_err(|e| AppError::RPCError(format!("get_block_number failed: {e}")))?;

    let mut low = 0u64;
    let mut high = latest;

    while low < high {
        let mid = (low + high) / 2;

        let code = provider
            .get_code_at(address)
            .block_id(mid.into())
            .await
            .map_err(|e| AppError::RPCError(format!("get_code_at failed: {e}")))?;

        if code.is_empty() {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    Ok(low)
}


impl Service<()> for ListenerService {
    type Response = ();
    type Error = AppError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let db_pool = self.db_pool.clone();
        let chain_id = self.chain_id;
        let address = self.address.clone();

        Box::pin(async move { fetch_and_save_logs(chain_id, db_pool, address).await })
    }
}

pub async fn fetch_and_save_logs(
    chain_id: u64,
    db_pool: Pool<Postgres>,
    address: String,
) -> Result<(), AppError> {
    let rpc_url: String = env::var("RPC_URL").map_err(|_| AppError::MissingEnvVar("RPC_URL".into()))?;

    let provider = ProviderBuilder::new().on_builtin(&rpc_url).await.map_err(|e| AppError::RPCError(format!("Provider Error Happened {}",e)))?;
    let sync_log = EvmSyncLogs::find_or_create_by_address(&address, chain_id, &db_pool).await?;

    let latest_block = provider.get_block_number().await.map_err(|e| AppError::RPCError(format!("Failed to get block number {}",e)))?;
    if latest_block == sync_log.last_synced_block_number as u64 {
        println!("Fully indexed address: {address}");
        return Ok(());
    }
    let from_block_number = if sync_log.last_synced_block_number == 0 {
        let address = Address::from_str(&address)
        .map_err(|e| AppError::InvalidAddress(format!("Invalid Address {e}")))?;
        let deployment_block = find_deployment_block(&provider, address).await?;
        deployment_block
    } else {
        sync_log.last_synced_block_number as u64 + 1
    };
    
    let to_block_number = std::cmp::min(
        from_block_number + 9,
        latest_block,
    );

    let filter = Filter::new()
        .address(Address::from_str(&address).map_err(|e| AppError::InvalidAddress(format!("Invalid Address {}",e)))?)
        .from_block(BlockNumberOrTag::Number(from_block_number))
        .to_block(BlockNumberOrTag::Number(to_block_number));

    let logs = provider.get_logs(&filter).await.map_err(|e| AppError::RPCError(format!("Error in getting logs {}",e)))?;
    println!("Fetched {} logs from RPC for blocks {} to {}", logs.len(), from_block_number, to_block_number);
    if logs.is_empty() {
        println!("No logs found in this block range - skipping save");
        // Still update sync log even if no logs
        let mut tx = db_pool.begin().await?;
        sync_log
            .update_last_synced_block_number(to_block_number, &mut *tx)
            .await.map_err(|e| AppError::EVMLog(format!("Error updating last_synced_block_number {}",e)))?;
        tx.commit().await?;
        return Ok(());
    }
    let mut tx = db_pool.begin().await?;
    let mut saved_count = 0;
    let mut error_count = 0;
    for log in logs.clone() {
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
    println!("Saved {}/{} logs successfully ({} errors)", saved_count, logs.len(), error_count);

    sync_log
        .update_last_synced_block_number(to_block_number, &mut *tx)
        .await.map_err(|e| AppError::EVMLog(format!("Error updating last_synced_block_number {}",e)))?;
      
        match tx.commit().await {
            Ok(_) => {
                println!("Committed transaction: Saved {} logs for {address}, blocks: {from_block_number} to {to_block_number}", saved_count);
            }
            Err(e) => {
                eprintln!("Transaction commit failed: {:?}", e);
                return Err(AppError::Database(e));
            }
        }
    Ok(())
}
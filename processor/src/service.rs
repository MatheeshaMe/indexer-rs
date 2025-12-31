use indexer_db::entity::evm_logs::EvmLogs;
use sqlx::{Pool, Postgres};
use std::{env, error::Error};
use tokio::task::JoinSet;

use crate::{ContractHandler, contracts::ContractRegistry, defaults, error::AppError};

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

    let mut futures = JoinSet::new();
    for log in unprocessed_logs {
        let processor_result = contract_registry.get_processor(log.address);

        match processor_result {
            Ok(processor) => {
                let service_db_pool = db_pool.clone();
                let log_id = log.id;
                let chain_id = chain_id;

                futures.spawn(async move {
                    match processor.process(log, &service_db_pool, chain_id).await {
                        Ok(_) => {
                            if let Err(error) = EvmLogs::mark_as_final(log_id, &service_db_pool).await {
                                eprintln!("Failed to mark log {} as final: {}", log_id, error)
                            }
                        }
                        Err(error) => eprintln!("Failed to process log {}: {}", log_id, error),
                    }
                });
            }
            Err(error) => {
                eprintln!("Error processing log {}: {:?}", log.id, error);
                // Also print the source error if it's a SQLx error
                if let AppError::Sqlx { source } = &error {
                    eprintln!("  SQLx error details: {}", source);
                }
            },
        }
    }

    futures.join_all().await;

    Ok(())
}

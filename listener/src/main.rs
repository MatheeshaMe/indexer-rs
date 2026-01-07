use std::{env, time::Duration};

use error::AppError;
use indexer_db::{entity::evm_chains::EvmChains, initialize_database};
use service::{ListenerMode, ListenerService};
use tokio::task::JoinSet;

mod error;
mod service;
mod metrics_server;

/// Run backfill phase for all addresses
async fn run_backfill(
    chain_id: u64,
    addresses: Vec<String>,
    db_pool: sqlx::Pool<sqlx::Postgres>,
    #[allow(unused_variables)] block_time: u64, // Reserved for future rate limiting if needed
) -> Result<(), AppError> {
    println!("Starting BACKFILL phase for {} addresses", addresses.len());

    let mut join_set = JoinSet::new();
    //future update to many addresses
    for address in addresses {
        let service = ListenerService {
            chain_id,
            address: address.clone(),
            db_pool: db_pool.clone(),
            mode: ListenerMode::Backfill,
        };

        let address_clone = address.clone();
        join_set.spawn(async move {
            loop {
                match service.run_backfill().await {
                    Ok(()) => {
                        // Continue backfilling (shouldn't happen, but safety)
                        // No sleep - maximum speed for backfill
                    }
                    Err(AppError::BackfillComplete) => {
                        println!("Backfill complete for {}", address_clone);
                        break; // Backfill finished for this address
                    }
                    Err(err) => {
                        eprintln!("Backfill error for {}: {:?}, retrying...", address_clone, err);
                        // Only sleep on error to avoid hammering RPC
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    // Wait for all backfill tasks to complete
    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            eprintln!("Backfill task panicked: {:?}", e);
        }
    }

    println!("BACKFILL phase complete for all addresses");
    Ok(())
}

/// Run live WebSocket phase for all addresses (never returns)
async fn run_ws(
    chain_id: u64,
    addresses: Vec<String>,
    db_pool: sqlx::Pool<sqlx::Postgres>,
) -> ! {
    println!("Starting LIVE WS phase for {} addresses", addresses.len());

    let mut join_set = JoinSet::new();

    for address in addresses {
        let service = ListenerService {
            chain_id,
            address: address.clone(),
            db_pool: db_pool.clone(),
            mode: ListenerMode::LiveWs,
        };

        let address_clone = address.clone();
        join_set.spawn(async move {
            println!("Starting WS listener for {}", address_clone);
            match service.run_ws().await {
                Ok(_) => unreachable!("run_ws never returns Ok"),
                Err(e) => {
                    eprintln!("WS listener error for {}: {:?}", address_clone, e);
                    // Restart WS listener
                    loop {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        match service.run_ws().await {
                            Ok(_) => unreachable!(),
                            Err(err) => {
                                eprintln!("WS listener retry failed for {}: {:?}", address_clone, err);
                            }
                        }
                    }
                }
            }
        });
    }

    // Keep running forever
    loop {
        if let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                eprintln!("WS task panicked: {:?}", e);
            }
            // Spawn new task if one died (shouldn't happen, but safety)
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_pool = initialize_database().await?;
    let chain_id_env =
        env::var("CHAIN_ID").map_err(|_| AppError::MissingEnvVar("CHAIN_ID".into()))?;
    let chain_id = chain_id_env
        .parse::<u64>()
        .map_err(|_| AppError::InvalidChainID(chain_id_env))?;
    let contract_addresses = env::var("CONTRACT_ADDRESSES")
        .map_err(|_| AppError::MissingEnvVar("CONTRACT_ADDRESSES".into()))?;

    let addresses: Vec<String> = contract_addresses
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if addresses.is_empty() {
        return Err("No contract addresses provided".into());
    }

    let evm_chain = EvmChains::fetch_by_id(chain_id, &db_pool).await?;

    println!("==========================================");
    println!("Indexer-RS Listener");
    println!("Chain ID: {}", chain_id);
    println!("Chain Name: {}", evm_chain.name);
    println!("Block Time: {}s", evm_chain.block_time);
    println!("Addresses: {}", addresses.join(", "));
    println!("==========================================");

    // Start metrics server
    let metrics_port = env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9091".to_string())
        .parse::<u16>()
        .unwrap_or(9091);
    
    tokio::spawn(async move {
        if let Err(e) = metrics_server::start_metrics_server(metrics_port).await {
            eprintln!("Metrics server error: {}", e);
        }
    });

    // PHASE 1: BACKFILL (finite, HTTP)
    println!("\nPHASE 1: BACKFILL (HTTP)");
    match run_backfill(
        chain_id,
        addresses.clone(),
        db_pool.clone(),
        evm_chain.block_time as u64,
    )
    .await
    {
        Ok(()) => {
            println!("Backfill phase completed successfully");
        }
        Err(e) => {
            eprintln!("Backfill phase error: {:?}", e);
            return Err(e.into());
        }
    }

    // PHASE 2: LIVE WS (infinite, never returns)
    println!("\nPHASE 2: LIVE WEBSOCKET");
    println!("Switching to live mode...");
    run_ws(chain_id, addresses, db_pool).await;
}


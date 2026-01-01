use contracts::contract_handler::ContractHandler;
use indexer_db::{entity::evm_logs::EvmLogs, initialize_database};
use service::{process_logs, cleanup_removed_logs};
use std::{env, error::Error};
use tokio::time::{sleep, Duration};

mod contracts;
mod error;
mod service;
mod utils;
mod metrics_server;

mod defaults {
    pub const POLL_INTERVAL: &str = "10";
    pub const BATCH_SIZE: &str = "25";
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_pool = initialize_database().await?;
    let poll_interval = env::var("POLL_INTERVAL")
        .or::<String>(Ok(defaults::POLL_INTERVAL.into()))?
        .parse::<u64>()?;

    let sleep_duration = Duration::from_secs(poll_interval);

    // Start metrics server
    let metrics_port = env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9092".to_string())
        .parse::<u16>()
        .unwrap_or(9092);
    
    tokio::spawn(async move {
        if let Err(e) = metrics_server::start_metrics_server(metrics_port).await {
            eprintln!("Metrics server error: {}", e);
        }
    });

    let chain_id = env::var("CHAIN_ID")
        .unwrap_or_else(|_| "0".to_string());

    loop {
        let unprocessed_count = match EvmLogs::count_unprocessed(&db_pool).await {
            Ok(count) => {
                // Update metrics
                if let Some(count) = count {
                    indexer_metrics::PROCESSOR_UNPROCESSED_LOGS
                        .with_label_values(&[&chain_id])
                        .set(count as f64);
                }
                count
            }
            Err(err) => {
                eprintln!(
                    "Error counting unprocessed logs: {err}. Sleeping for {} seconds...",
                    sleep_duration.as_secs()
                );

                sleep(sleep_duration).await;
                continue;
            }
        };

        // First, cleanup removed logs (reorgs)
        if let Err(err) = cleanup_removed_logs(&db_pool).await {
            eprintln!("Error cleaning up removed logs: {err}");
        }

        match unprocessed_count {
            Some(count) => {
                println!("Found {count} unprocessed logs. Starting processing...",);

                if let Err(err) = process_logs(&db_pool).await {
                    eprintln!("Error processing logs: {err}");
                }
            }
            None => {
                println!(
                    "No unprocessed logs found. Sleeping for {} seconds...",
                    sleep_duration.as_secs()
                );
                sleep(sleep_duration).await;
            }
        }
    }
}

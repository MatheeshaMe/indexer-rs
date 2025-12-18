use alloy::{json_abi::JsonAbi, primitives::Address, rpc::types::Log};
use crate::error::AppError;
use super::contract_handler::ContractHandler;
use sqlx::{types::{BigDecimal, chrono}, Postgres, Pool};
use indexer_db::entity::usdt_transfers::UsdtTransfers;
use std::str::FromStr;
pub struct USDTErc20 {
    #[allow(dead_code)]
    pub address:Address,
    pub abi:JsonAbi
}

impl ContractHandler for USDTErc20 {
    const NAME:&str = "usdt_erc20";

    fn new(address:&str) -> Result<Self,AppError> {
        let contract_address = address.parse::<Address>().map_err(|_| AppError::InvalidAddress(address.into()))?;
        let contract_abi = Self::get_abi_for_contract(Self::NAME)?;
        Ok(Self { address: contract_address, abi: contract_abi })
    }
    fn abi(&self) -> &JsonAbi {
        &self.abi
    }

    async fn handle_event(
        &self,
        event: &str,
        log: &Log,
        connection: &Pool<Postgres>,
        chain_id: i64,
    ) -> Result<(), AppError> {
        match event {
            "Transfer" => self.transfer(log, connection, chain_id).await,
            "Approval" => self.approval(log, connection, chain_id).await,
            other => Err(AppError::MissingEventHandler(Self::NAME.into(), other.into())),
        }
    }
}

impl USDTErc20 {
    /// Converts U256 to BigDecimal for database storage
    /// Handles large values that exceed u64 range by converting through string
    fn u256_to_bigdecimal(value: alloy::primitives::U256) -> BigDecimal {
        // Convert U256 to string, then parse to BigDecimal
        // This handles large values that exceed u64 range (e.g., token amounts with 18 decimals)
        let value_str = value.to_string();
        // Use FromStr trait for BigDecimal
        BigDecimal::from_str(&value_str)
            .unwrap_or_else(|_| {
                // Fallback to 0 if parsing fails (shouldn't happen for valid U256)
                eprintln!("Warning: Failed to convert U256 {} to BigDecimal, using 0", value_str);
                BigDecimal::from(0u64)
            })
    }

    async fn transfer(
        &self,
        log: &Log,
        connection: &Pool<Postgres>,
        chain_id: i64,
    ) -> Result<(), AppError> {
        // Validate topics - ERC20 Transfer event has 3 topics: [signature, from, to]
        let topics = log.topics();
        if topics.len() < 3 {
            return Err(AppError::InvalidAddress(
                "Transfer event must have at least 3 topics".into(),
            ));
        }

        // Extract addresses from topics (topics are 32 bytes, addresses are 20 bytes, padded with zeros)
        // topics[0] = event signature (keccak256("Transfer(address,address,uint256)"))
        // topics[1] = from address (padded to 32 bytes)
        // topics[2] = to address (padded to 32 bytes)
        let from = Address::from_slice(&topics[1].as_slice()[12..]);
        let to = Address::from_slice(&topics[2].as_slice()[12..]);

        // ERC20 Transfer value is in data as uint256 (32 bytes, big-endian)
        let value_u256 = alloy::primitives::U256::from_be_slice(log.data().data.as_ref());
        let value = Self::u256_to_bigdecimal(value_u256);

        // Extract transaction hash (required field)
        let tx_hash = log
            .transaction_hash
            .ok_or_else(|| AppError::InvalidAddress("Missing transaction hash".into()))?
            .into();

        // Extract block number (required field)
        let block_number: BigDecimal = <u64 as Into<BigDecimal>>::into(
            log.block_number
                .ok_or_else(|| AppError::InvalidAddress("Missing block number".into()))?,
        );

        // Extract block timestamp (optional, use current time if not available)
        // Log.block_timestamp is Option<u64> (Unix timestamp in seconds)
        let block_timestamp = log
            .block_timestamp
            .and_then(|ts| {
                // Convert Unix timestamp (u64) to NaiveDateTime
                // Use DateTime::from_timestamp (new API) instead of deprecated from_timestamp_opt
                chrono::DateTime::from_timestamp(ts as i64, 0)
                    .map(|dt| dt.naive_utc())
            })
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());

        // Convert addresses to byte arrays
        let from_address: [u8; 20] = from.into();
        let to_address: [u8; 20] = to.into();

        // Classification: 0 = regular transfer, can be extended later for different types
        let classification: i16 = 0;

        // Create the transfer record
        let transfer = UsdtTransfers {
            id: 0, // Will be set by database on insert
            tx_hash,
            block_number,
            block_timestamp,
            from_address,
            to_address,
            value,
            classification,
            chain_id,
            created_at: chrono::Utc::now().naive_utc(), // Will be set by database, but set here for completeness
        };

        // Insert into database
        // Pool<Postgres> implements Executor, so we can use it directly
        UsdtTransfers::create(transfer, connection)
            .await
            .map_err(|e| AppError::Sqlx { source: e })?;

        println!(
            "USDT Transfer stored: from {from:?} to {to:?} value {}",
            value_u256
        );

        Ok(())
    }

    async fn approval(
        &self,
        log: &Log,
        _connection: &Pool<Postgres>,
        _chain_id: i64,
    ) -> Result<(), AppError> {
        // topics[1] = owner, topics[2] = spender, data = value
        let topics = log.topics();
        if topics.len() < 3 {
            return Err(AppError::InvalidAddress("Missing topics for Approval".into()));
        }
        let owner = Address::from_slice(&topics[1].as_slice()[12..]);
        let spender = Address::from_slice(&topics[2].as_slice()[12..]);
        let value = alloy::primitives::U256::from_be_slice(log.data().data.as_ref());

        println!("USDT Approval: owner {owner:?} spender {spender:?} value {value}");
        // TODO: Implement approval storage if needed
        Ok(())
    }
}
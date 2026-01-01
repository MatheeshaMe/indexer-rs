use alloy::{
    primitives::{Address, Bytes, FixedBytes},
    rpc::types::Log,
};
use sqlx::{
    types::{chrono, BigDecimal},
    Executor, Postgres,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EvmLogsError {
    #[error("Failed to create a valid log data")]
    InvalidLogData,

    #[error("Invalid block number: `{0}`")]
    InvalidBlockNumber(String),
}

#[derive(sqlx::FromRow, Debug)]
pub struct EvmLogs {
    pub id: i32,
    pub block_number: BigDecimal,
    pub block_hash: [u8; 32],
    pub address: [u8; 20],
    pub transaction_hash: [u8; 32],
    pub data: Vec<u8>,
    pub event_signature: [u8; 32],
    pub topics: Vec<[u8; 32]>,
    pub transaction_index: i64,
    pub log_index: i64,
    pub removed: bool,
    pub created_at: chrono::NaiveDateTime,
    pub is_processed: bool,
}
 
impl TryInto<Log> for EvmLogs {
    type Error = EvmLogsError;

    fn try_into(self) -> Result<Log, Self::Error> {
        let transaction_hash = FixedBytes::<32>::from(self.transaction_hash);
        let contract_address = Address::from(self.address);
        let topics: Vec<FixedBytes<32>> = self.topics.iter().map(FixedBytes::<32>::from).collect();
        let data = Bytes::from(self.data);

        let inner = alloy::primitives::Log::new(contract_address, topics, data)
            .ok_or(EvmLogsError::InvalidLogData)?;

        let block_number = self
            .block_number
            .to_string()
            .parse::<u64>()
            .map_err(|_| EvmLogsError::InvalidBlockNumber(self.block_number.to_string()))?;

        Ok(Log {
            inner,
            block_number: Some(block_number),
            block_hash: None,
            block_timestamp: None,
            transaction_hash: Some(transaction_hash),
            transaction_index: None,
            log_index: None,
            removed: false,
        })
    }
}

impl EvmLogs {
    pub async fn create<'c, E>(log: Log, connection: E) -> Result<EvmLogs, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let block_hash = log
            .block_hash
            .ok_or_else(|| sqlx::Error::Decode("Missing block hash".into()))?
            .to_vec();

        let block_number: BigDecimal = <u64 as Into<BigDecimal>>::into(
            log.block_number
                .ok_or_else(|| sqlx::Error::Decode("Missing block number".into()))?,
        );

        let transaction_index: i64 = log
            .transaction_index
            .ok_or_else(|| sqlx::Error::Decode("Missing transaction index".into()))?
            .try_into()
            .map_err(|_| sqlx::Error::Decode("Transaction index exceeds i64 range".into()))?;

        let log_index: i64 = log
            .log_index
            .ok_or_else(|| sqlx::Error::Decode("Missing log index".into()))?
            .try_into()
            .map_err(|_| sqlx::Error::Decode("Log index exceeds i64 range".into()))?;

        let transaction_hash_vec = log
            .transaction_hash
            .ok_or_else(|| sqlx::Error::Decode("Missing transaction hash".into()))?
            .to_vec();

        let address = log.address().to_vec();

        let event_signature: &[u8] = log.topics()[0].as_slice();

        let topics: Vec<&[u8]> = log
            .topics()
            .iter()
            .map(|topic| -> &[u8] { topic.as_slice() })
            .collect();

        let log_data: Vec<u8> = log.inner.data.data.to_vec();

        // Insert log into the database with idempotency
        // Use ON CONFLICT DO UPDATE to return existing row if conflict
        // This ensures idempotency: same log can be inserted multiple times safely
        let query = r#"
            INSERT INTO evm_logs (block_hash, block_number, address, transaction_hash, transaction_index, event_signature, topics, data, log_index, removed, is_processed)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (transaction_hash, log_index) 
            DO UPDATE SET 
                block_hash = EXCLUDED.block_hash,
                block_number = EXCLUDED.block_number,
                removed = EXCLUDED.removed
            RETURNING *
        "#;
        println!("logs for {:?} {:?} {:?} {:?}",address,topics,log_data,log_index);
        sqlx::query_as::<_, EvmLogs>(query)
            .bind(block_hash)
            .bind(block_number)
            .bind(address)
            .bind(&transaction_hash_vec)
            .bind(transaction_index)
            .bind(event_signature)
            .bind(topics)
            .bind(log_data)
            .bind(log_index)
            .bind(log.removed)
            .bind(false) // is_processed = false for new logs
            .fetch_one(connection)
            .await
    }

    /// Find all unprocessed logs (removed = false AND is_processed = false)
    pub async fn find_unprocessed<'c, E>(page_size: i32, connection: E) -> Result<Vec<EvmLogs>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query_as::<_, EvmLogs>(
            "SELECT * FROM evm_logs WHERE removed = false AND is_processed = false ORDER BY block_number ASC, log_index ASC LIMIT $1"
        )
            .bind(page_size)
            .fetch_all(connection)
            .await
    }

    /// Legacy method for backward compatibility (finds all logs, not recommended)
    pub async fn find_all<'c, E>(page_size: i32, connection: E) -> Result<Vec<EvmLogs>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query_as::<_, EvmLogs>("SELECT * FROM evm_logs LIMIT $1")
            .bind(page_size)
            .fetch_all(connection)
            .await
    }

    /// Mark a log as processed
    pub async fn mark_as_processed<'c, E>(id: i32, connection: E) -> Result<(), sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query("UPDATE evm_logs SET is_processed = true WHERE id = $1")
            .bind(id)
            .execute(connection)
            .await?;

        Ok(())
    }

    /// Mark logs as removed for a block range (reorgs)
    pub async fn mark_removed_by_block_range<'c, E>(
        from_block: u64,
        to_block: u64,
        address: &[u8; 20],
        connection: E,
    ) -> Result<u64, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let from_block_bd = <u64 as Into<BigDecimal>>::into(from_block);
        let to_block_bd = <u64 as Into<BigDecimal>>::into(to_block);

        let result = sqlx::query(
            "UPDATE evm_logs SET removed = true WHERE address = $1 AND block_number >= $2 AND block_number <= $3 AND removed = false"
        )
            .bind(&address[..])
            .bind(from_block_bd)
            .bind(to_block_bd)
            .execute(connection)
            .await?;

        Ok(result.rows_affected() as u64)
    }

    /// Legacy delete method (deprecated, use mark_as_processed instead)
    #[deprecated(note = "Use mark_as_processed instead to preserve log history")]
    pub async fn delete<'c, E>(id: i32, connection: E) -> Result<(), sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query("DELETE FROM evm_logs WHERE id = $1")
            .bind(id)
            .execute(connection)
            .await?;

        Ok(())
    }

    /// Find processed logs that were marked as removed (for cleanup)
    pub async fn find_processed_but_removed<'c, E>(page_size: i32, connection: E) -> Result<Vec<EvmLogs>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query_as::<_, EvmLogs>(
            "SELECT * FROM evm_logs WHERE removed = true AND is_processed = true ORDER BY block_number ASC, log_index ASC LIMIT $1"
        )
            .bind(page_size)
            .fetch_all(connection)
            .await
    }

    /// Count unprocessed logs (removed = false AND is_processed = false)
    pub async fn count_unprocessed<'c, E>(connection: E) -> Result<Option<i64>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_logs WHERE removed = false AND is_processed = false")
            .fetch_one(connection)
            .await?;

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(count))
    }

    pub async fn count<'c, E>(connection: E) -> Result<Option<i64>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_logs")
            .fetch_one(connection)
            .await?;

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(count))
    }
}

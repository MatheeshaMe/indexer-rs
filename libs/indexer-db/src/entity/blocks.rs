use sqlx::{types::BigDecimal, Executor, Postgres, types::chrono};

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct Block {
    pub block_number: BigDecimal,
    pub block_hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub finalized_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
}

impl Block {
    /// Create or update a block record
    /// Uses ON CONFLICT to update if block already exists (idempotent)
    pub async fn create_or_update<'c, E>(
        block_number: u64,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
        connection: E,
    ) -> Result<Block, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let block_number_bd = <u64 as Into<BigDecimal>>::into(block_number);

        let query = r#"
            INSERT INTO blocks (block_number, block_hash, parent_hash)
            VALUES ($1, $2, $3)
            ON CONFLICT (block_number) 
            DO UPDATE SET 
                block_hash = EXCLUDED.block_hash,
                parent_hash = EXCLUDED.parent_hash
            RETURNING *
        "#;

        sqlx::query_as::<_, Block>(query)
            .bind(block_number_bd)
            .bind(&block_hash[..])
            .bind(&parent_hash[..])
            .fetch_one(connection)
            .await
    }

    /// Find block by block number
    pub async fn find_by_number<'c, E>(
        block_number: u64,
        connection: E,
    ) -> Result<Option<Block>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let block_number_bd = <u64 as Into<BigDecimal>>::into(block_number);

        sqlx::query_as::<_, Block>(
            "SELECT * FROM blocks WHERE block_number = $1"
        )
        .bind(block_number_bd)
        .fetch_optional(connection)
        .await
    }

    /// Find block by block hash
    pub async fn find_by_hash<'c, E>(
        block_hash: &[u8; 32],
        connection: E,
    ) -> Result<Option<Block>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query_as::<_, Block>(
            "SELECT * FROM blocks WHERE block_hash = $1"
        )
        .bind(&block_hash[..])
        .fetch_optional(connection)
        .await
    }

    /// Mark block as finalized
    pub async fn mark_as_finalized<'c, E>(
        block_number: u64,
        connection: E,
    ) -> Result<(), sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let block_number_bd = <u64 as Into<BigDecimal>>::into(block_number);

        sqlx::query(
            "UPDATE blocks SET finalized_at = NOW() WHERE block_number = $1"
        )
        .bind(block_number_bd)
        .execute(connection)
        .await?;

        Ok(())
    }
}


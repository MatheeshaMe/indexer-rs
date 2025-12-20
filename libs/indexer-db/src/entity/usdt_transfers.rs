use sqlx::{Executor, Postgres, types::{BigDecimal, chrono}};

#[derive(sqlx::FromRow,Debug,Clone)]
pub struct UsdtTransfers{
    pub id: i64,
    pub tx_hash: [u8; 32],
    pub block_number: BigDecimal,
    pub block_timestamp: chrono::NaiveDateTime,
    pub from_address: [u8; 20],
    pub to_address: [u8; 20],
    pub value: sqlx::types::BigDecimal,
    pub classification: i16,
    pub chain_id: i64,
    pub created_at: chrono::NaiveDateTime
}
impl UsdtTransfers {
    pub async fn create<'c, E>(transfer: UsdtTransfers, connection: E) -> Result<UsdtTransfers, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = r#"
        INSERT INTO usdt_transfers (tx_hash, block_number, block_timestamp, from_address, to_address, value, classification, chain_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (tx_hash, from_address, to_address, value, block_number) DO NOTHING
        RETURNING *
    "#;
        println!("query {}",query);
        sqlx::query_as::<_, UsdtTransfers>(query)
            .bind(transfer.tx_hash)
            .bind(transfer.block_number)
            .bind(transfer.block_timestamp)
            .bind(transfer.from_address)
            .bind(transfer.to_address)
            .bind(transfer.value)
            .bind(transfer.classification)
            .bind(transfer.chain_id)
            .fetch_one(connection)
            .await
    }


    pub async fn fetch_by_id<'c, E>(id: u64, connection: E) -> Result<UsdtTransfers, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = "SELECT * FROM usdt_transfers WHERE id = $1";

        sqlx::query_as::<_, UsdtTransfers>(query)
            .bind(id as i64)
            .fetch_one(connection)
            .await
    }
    // pub async fn upate_usdt_transfer<'c,E>(&self,tx_hash: Vec<u8>,connection: E) -> Result<UsdtTransfers,sqlx::Error> where E: Executor<'c,Database=Postgres>{
    //     let query = "UPDATE usdt_transfers SET"
    // }
}
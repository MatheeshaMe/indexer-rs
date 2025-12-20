CREATE TABLE IF NOT EXISTS usdt_transfers
(
    id BIGSERIAL PRIMARY KEY,
    tx_hash BYTEA NOT NULL,
    block_number NUMERIC NOT NULL,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    from_address BYTEA NOT NULL,
    to_address BYTEA NOT NULL,
    value NUMERIC NOT NULL,
    classification SMALLINT NOT NULL DEFAULT 0,
    chain_id BIGINT NOT NULL REFERENCES evm_chains(id),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_tx_hash ON usdt_transfers(tx_hash);
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_block_number ON usdt_transfers(block_number);
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_from_address ON usdt_transfers(from_address);
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_to_address ON usdt_transfers(to_address);
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_chain_id ON usdt_transfers(chain_id);
CREATE INDEX IF NOT EXISTS idx_usdt_transfers_block_timestamp ON usdt_transfers(block_timestamp);

-- Optional: Create a unique constraint to prevent duplicate transfers
CREATE UNIQUE INDEX IF NOT EXISTS idx_usdt_transfers_unique 
ON usdt_transfers(tx_hash, from_address, to_address, value, block_number);
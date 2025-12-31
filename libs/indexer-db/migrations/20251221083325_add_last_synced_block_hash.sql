-- Migration: Add last_synced_block_hash to evm_sync_logs
ALTER TABLE evm_sync_logs 
ADD COLUMN IF NOT EXISTS last_synced_block_hash BYTEA;

CREATE INDEX IF NOT EXISTS idx_evm_sync_logs_block_hash 
ON evm_sync_logs(address, last_synced_block_number);
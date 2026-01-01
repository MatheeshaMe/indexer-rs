-- Migration: Rename is_final to is_processed for semantic clarity
-- is_processed indicates whether a log has been processed, not whether it's finalized on-chain
ALTER TABLE evm_logs 
RENAME COLUMN is_final TO is_processed;


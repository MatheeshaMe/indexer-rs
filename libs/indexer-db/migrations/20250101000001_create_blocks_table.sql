-- Migration: Create blocks table for tracking block hashes and parent hashes
-- This enables efficient reorg detection and analytics
CREATE TABLE IF NOT EXISTS blocks (
    block_number NUMERIC PRIMARY KEY,
    block_hash BYTEA NOT NULL UNIQUE,
    parent_hash BYTEA NOT NULL,
    finalized_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_blocks_parent_hash ON blocks(parent_hash);
CREATE INDEX IF NOT EXISTS idx_blocks_finalized_at ON blocks(finalized_at);
CREATE INDEX IF NOT EXISTS idx_blocks_block_hash ON blocks(block_hash);


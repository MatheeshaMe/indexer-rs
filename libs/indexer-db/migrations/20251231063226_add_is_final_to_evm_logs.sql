-- Migration: Add is_final column to evm_logs
ALTER TABLE evm_logs 
ADD COLUMN IF NOT EXISTS is_final BOOL DEFAULT FALSE;

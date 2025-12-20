-- Add migration script here
INSERT INTO evm_chains (id, name, block_time, last_synced_block_number)
VALUES (1, 'Ethereum Mainnet', 12, 24039923)
ON CONFLICT (id) DO NOTHING;
use alloy::{json_abi::JsonAbi, primitives::Address, rpc::types::Log};
use crate::error::AppError;
use super::contract_handler::ContractHandler;

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

    async fn handle_event(&self, event: &str, log: &Log) -> Result<(), AppError> {
        match event {
            "Transfer" => self.transfer(log).await,
            "Approval" => self.approval(log).await,
            other => Err(AppError::MissingEventHandler(Self::NAME.into(), other.into())),
        }
    }
}

impl USDTErc20 {
    async fn transfer(&self, log: &Log) -> Result<(), AppError> {
        // topics[1] = from, topics[2] = to, data = value
        let topics = log.topics();
        if topics.len() < 3 {
            return Err(AppError::InvalidAddress("Missing topics for Transfer".into()));
        }

        let from = Address::from_slice(&topics[1].as_slice()[12..]);
        let to = Address::from_slice(&topics[2].as_slice()[12..]);

        // ERC20 Transfer value is in data as uint256
        let value = alloy::primitives::U256::from_be_slice(log.data().data.as_ref());

        println!("USDT Transfer: from {from:?} to {to:?} value {value}");
        // TODO: insert into your domain table (e.g., transfers) if desired
        Ok(())
    }

    async fn approval(&self, log: &Log) -> Result<(), AppError> {
        // topics[1] = owner, topics[2] = spender, data = value
        let topics = log.topics();
        if topics.len() < 3 {
            return Err(AppError::InvalidAddress("Missing topics for Approval".into()));
        }
        let owner = Address::from_slice(&topics[1].as_slice()[12..]);
        let spender = Address::from_slice(&topics[2].as_slice()[12..]);
        let value = alloy::primitives::U256::from_be_slice(log.data().data.as_ref());

        println!("USDT Approval: owner {owner:?} spender {spender:?} value {value}");
        Ok(())
    }
}
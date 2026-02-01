use alloy::primitives::{Address, U256};
use crate::amms::{amm::{AMM, AutomatedMarketMaker, UniswapPool}, error::AMMError};




#[derive(Debug, Clone)]
pub struct SwapHop {
    pub amm: AMM,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct UniswapHop {
    pub pool: UniswapPool,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct ArbPath {
    pub hops: Vec<SwapHop>,
}


#[derive(Debug, Clone)]
pub struct UniswapArbPath {
    pub hops: Vec<UniswapHop>,
}

impl ArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.amm.simulate_swap(
                hop.base,
                hop.quote,
                amount,
            )?;
        }

        Ok(amount)
    }
}

impl UniswapArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.pool.simulate_swap(
                hop.base,
                hop.quote,
                amount,
            )?;
        }

        Ok(amount)
    }
}

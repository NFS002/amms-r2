use crate::amms::Token;

use super::{
    balancer::BalancerPool, erc_4626::ERC4626Vault, error::AMMError, uniswap_v2::UniswapV2Pool,
    uniswap_v3::UniswapV3Pool,
};
use alloy::{
    eips::BlockId,
    network::Network,
    primitives::{Address, B256, U256},
    providers::Provider,
    rpc::types::Log,
};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
};

#[allow(async_fn_in_trait)]
pub trait AutomatedMarketMaker {
    /// Address of the AMM
    fn address(&self) -> Address;

    /// Event signatures that indicate when the AMM should be synced
    fn sync_events(&self) -> Vec<B256>;

    /// Syncs the AMM state
    fn sync(&mut self, log: &Log) -> Result<(), AMMError>;

    /// Returns a list of token addresses used in the AMM
    fn tokens(&self) -> Vec<Address>;

    /// Calculates the price of `base_token` in terms of `quote_token`
    fn calculate_price(&self, base_token: Address, quote_token: Address) -> Result<f64, AMMError>;

    /// Simulate a swap
    /// Returns the amount_out in `quote token` for a given `amount_in` of `base_token`
    fn simulate_swap(
        &self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError>;

    /// Simulate a swap, mutating the AMM state
    /// Returns the amount_out in `quote token` for a given `amount_in` of `base_token`
    fn simulate_swap_mut(
        &mut self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError>;

    // Initializes an empty pool and syncs state up to `block_number`
    async fn init<N, P>(self, block_number: BlockId, provider: P) -> Result<Self, AMMError>
    where
        Self: Sized,
        N: Network,
        P: Provider<N> + Clone;
}

macro_rules! amm {
    ($($pool_type:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub enum AMM {
            $($pool_type($pool_type),)+
        }

        impl AutomatedMarketMaker for AMM {
            fn address(&self) -> Address{
                match self {
                    $(AMM::$pool_type(pool) => pool.address(),)+
                }
            }

            fn sync_events(&self) -> Vec<B256> {
                match self {
                    $(AMM::$pool_type(pool) => pool.sync_events(),)+
                }
            }

            fn sync(&mut self, log: &Log) -> Result<(), AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.sync(log),)+
                }
            }

            fn simulate_swap(&self, base_token: Address, quote_token: Address,amount_in: U256) -> Result<U256, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.simulate_swap(base_token, quote_token, amount_in),)+
                }
            }

            fn simulate_swap_mut(&mut self, base_token: Address, quote_token: Address, amount_in: U256) -> Result<U256, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.simulate_swap_mut(base_token, quote_token, amount_in),)+
                }
            }

            fn tokens(&self) -> Vec<Address> {
                match self {
                    $(AMM::$pool_type(pool) => pool.tokens(),)+
                }
            }

            fn calculate_price(&self, base_token: Address, quote_token: Address) -> Result<f64, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.calculate_price(base_token, quote_token),)+
                }
            }

            async fn init<N, P>(self, block_number: BlockId, provider: P) -> Result<Self, AMMError>
            where
                Self: Sized,
                N: Network,
                P: Provider<N> + Clone,
            {
                match self {
                    $(AMM::$pool_type(pool) => pool.init(block_number, provider).await.map(AMM::$pool_type),)+
                }
            }
        }


        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum Variant {
            $($pool_type,)+
        }

        impl AMM {
            pub fn variant(&self) -> Variant {
                match self {
                    $(AMM::$pool_type(_) => Variant::$pool_type,)+
                }
            }
        }

        impl fmt::Display for AMM {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "This is an AMM")
            }
        }

        impl Hash for AMM {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.address().hash(state);
            }
        }

        impl PartialEq for AMM {
            fn eq(&self, other: &Self) -> bool {
                self.address() == other.address()
            }
        }

        impl Eq for AMM {}

        $(
            impl From<$pool_type> for AMM {
                fn from(amm: $pool_type) -> Self {
                    AMM::$pool_type(amm)
                }
            }
        )+
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UniswapPool {
    V2(UniswapV2Pool),
    V3(UniswapV3Pool),
}

impl UniswapPool {
    pub fn address(&self) -> Address {
        match self {
            UniswapPool::V2(pool) => pool.address(),
            UniswapPool::V3(pool) => pool.address(),
        }
    }

    pub fn token_a(&self) -> Token {
        match self {
            UniswapPool::V2(pool) => pool.token_a.clone(),
            UniswapPool::V3(pool) => pool.token_a.clone() // NOT IMPLEMENTED
        }
    }

    pub fn token_b(&self) -> Token {
        match self {
            UniswapPool::V2(pool) => pool.token_b.clone(),
            UniswapPool::V3(pool) => pool.token_b.clone() // NOT IMPLEMENTED
        }
    }

    pub fn r0(&self) -> u128 {
        match self {
            UniswapPool::V2(pool) => pool.reserve_0,
            UniswapPool::V3(_) => 0 // NOT IMPLEMENTED
        }
    }

    pub fn r1(&self) -> u128 {
        match self {
            UniswapPool::V2(pool) => pool.reserve_1,
            UniswapPool::V3(_) => 0 // NOT IMPLEMENTED
        }
    }

    pub fn fee(&self) -> usize {
        match self {
            UniswapPool::V2(pool) => pool.fee,
            UniswapPool::V3(_) => 0 // NOT IMPLEMENTED
        }
    }

    pub fn simulate_swap(
        &self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        match self {
            UniswapPool::V2(pool) => pool.simulate_swap(base_token, quote_token, amount_in),
            UniswapPool::V3(pool) => pool.simulate_swap(base_token, quote_token, amount_in),
        }
    }
}

amm!(UniswapV2Pool, UniswapV3Pool, ERC4626Vault, BalancerPool);

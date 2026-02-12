use std::collections::HashMap;
use thiserror::Error;

use super::{AMMFilter, FilterStage};
use crate::amms::{
    amm::{AMM, AutomatedMarketMaker},
    error::{AMMError, FilterError, IOError},
    retry_queue::{RetryQueueOutcome, run_retry_queue},
};
use alloy::{
    network::Ethereum,
    primitives::{Address, U256},
    providers::{DynProvider, Provider},
    sol,
    sol_types::SolValue,
};
use async_trait::async_trait;
use eyre::ContextCompat;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;
use WethValueInPools::{PoolInfo, PoolInfoReturn};

sol! {
    #[sol(rpc)]
    WethValueInPoolsBatchRequest,
    "src/amms/abi/WethValueInPoolsBatchRequest.json"
}

const DEFAULT_CHUNK_SIZE: usize = 200;
const DEFAULT_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_RETRY_DELAY_SECS: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueFilter {
    pub uniswap_v2_factory: Address,
    pub uniswap_v3_factory: Address,
    pub weth: Address,
    pub min_weth_threshold: U256,

    #[serde(skip)]
    pub provider: Option<DynProvider>,

    pub chunk_size: usize,
}

impl ValueFilter {
    pub fn new<P>(
        uniswap_v2_factory: Address,
        uniswap_v3_factory: Address,
        weth: Address,
        min_weth_threshold: U256,
        provider: P,
    ) -> Self
    where
        P: Provider<Ethereum> + Clone + 'static,
    { 
        Self {
            uniswap_v2_factory,
            uniswap_v3_factory,
            weth,
            min_weth_threshold,
            provider: Some(provider.erased()),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub async fn get_weth_value_in_pools(
        &self,
        pools: Vec<PoolInfo>,
    ) -> Result<HashMap<Address, PoolInfoReturn>, AMMError> {
        if pools.is_empty() {
            return Ok(HashMap::new());
        }

        let pool_len = pools.len();
        
        let provider = self.provider.clone().ok_or(AMMError::FilterError(FilterError::ValueFilterError(ValueFilterError::NoProvider)))?;

        let uniswap_v2_factory = self.uniswap_v2_factory;
        let uniswap_v3_factory = self.uniswap_v3_factory;
        let weth = self.weth;
        let retry_delay = Duration::from_secs(DEFAULT_RETRY_DELAY_SECS);


        let (batches, _failed_batches) = run_retry_queue(
            vec![pools],
            |pools| {
                let provider = provider.clone();
                async move {
                    let deployer = WethValueInPoolsBatchRequest::deploy_builder(
                        provider,
                        uniswap_v2_factory,
                        uniswap_v3_factory,
                        weth,
                        pools.clone(),
                    );

                    match deployer.call_raw().await {
                        Ok(res) => {
                            let return_data = <Vec<PoolInfoReturn> as SolValue>::abi_decode(&res)?;
                            Ok::<
                                RetryQueueOutcome<Vec<PoolInfoReturn>, Vec<PoolInfo>>,
                                AMMError,
                            >(RetryQueueOutcome::Success(return_data))
                        }
                        Err(_err) => Ok::<
                            RetryQueueOutcome<Vec<PoolInfoReturn>, Vec<PoolInfo>>,
                            AMMError,
                        >(RetryQueueOutcome::Retry(pools)),
                    }
                }
            },
            DEFAULT_RETRY_ATTEMPTS,
            retry_delay,
            "state_space::filters::value::get_weth_value_in_pools",
        )
        .await?;


        let mut pool_info_returns = HashMap::new();
        for batch in batches {
            for pool_info in batch {
                pool_info_returns.insert(pool_info.poolAddress, pool_info);
            }
        }

        Ok(pool_info_returns)
    }
}

#[async_trait]
impl AMMFilter for ValueFilter {
    async fn filter(&self, amms: Vec<AMM>) -> Result<Vec<AMM>, AMMError> {
        let pool_infos = amms
            .iter()
            .cloned()
            .map(|amm| {
                let pool_address = amm.address();
                let pool_type = match amm {
                    AMM::BalancerPool(_) => 0,
                    AMM::UniswapV2Pool(_) => 1,
                    AMM::UniswapV3Pool(_) => 2,
                    // TODO: At the moment, filters are not compatible with vaults
                    AMM::ERC4626Vault(_) => todo!(),
                };

                PoolInfo {
                    poolType: pool_type,
                    poolAddress: pool_address,
                }
            })
            .collect::<Vec<_>>();

        let mut pool_info_returns = HashMap::new();
        let futs = pool_infos
            .chunks(self.chunk_size)
            .map(|chunk| async { self.get_weth_value_in_pools(chunk.to_vec()).await })
            .collect::<Vec<_>>();

        let results = futures::future::join_all(futs).await;
        for result in results {
            pool_info_returns.extend(result?);
        }

        let filtered_amms = amms
            .into_iter()
            .filter(|amm| {
                let pool_address = amm.address();
                pool_info_returns
                    .get(&pool_address)
                    .is_some_and(|pool_info_return| {
                        pool_info_return.wethValue > self.min_weth_threshold
                    })
            })
            .collect::<Vec<_>>();
        Ok(filtered_amms)
    }

    fn stage(&self) -> FilterStage {
        FilterStage::Sync
    }
}

#[derive(Error, Debug)]
pub enum ValueFilterError {
    #[error("No provider")]
    NoProvider
}

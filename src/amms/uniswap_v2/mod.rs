use crate::{amms::formatters::debug_formatters::base62_encode, state_space::filters::FilterStage};

use super::{
    amm::{AutomatedMarketMaker, AMM},
    consts::{
        MPFR_T_PRECISION, U128_0X10000000000000000, U256_0X100, U256_0X10000, U256_0X100000000,
        U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
        U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, U256_1, U256_100000, U256_128,
        U256_16, U256_191, U256_192, U256_2, U256_255, U256_32, U256_4, U256_64, U256_8,
    },
    error::AMMError,
    factory::{AutomatedMarketMakerFactory, DiscoverySync},
    math::q64_to_float,
    retry_queue::{run_retry_queue, RetryQueueOutcome},
    Token,
};

use alloy::{
    eips::BlockId,
    network::Network,
    primitives::{Address, Bytes, Keccak256, B256, U256},
    providers::Provider,
    rpc::types::Log,
    sol,
    sol_types::{SolCall, SolEvent, SolValue},
};
use itertools::Itertools;
use rug::Float;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::format,
    future::Future,
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
};
use thiserror::Error;
use tokio::time::Duration;
use tracing::{info, warn};
use IGetUniswapV2PoolDataBatchRequest::IGetUniswapV2PoolDataBatchRequestInstance;
use IUniswapV2Factory::IUniswapV2FactoryInstance;

static UNISWAP_V2_POOL_COUNTER: AtomicUsize = AtomicUsize::new(0);

sol!(
// UniswapV2Factory
#[allow(missing_docs)]
#[derive(Debug)]
#[sol(rpc)]
contract IUniswapV2Factory {
    event PairCreated(address indexed token0, address indexed token1, address pair, uint256);
    function allPairs(uint256) external view returns (address pair);
    function allPairsLength() external view returns (uint256);

}

#[derive(Debug, PartialEq, Eq)]
#[sol(rpc)]
contract IUniswapV2Pair {
    event Sync(uint112 reserve0, uint112 reserve1);
    function token0() external view returns (address);
    function token1() external view returns (address);
    function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata data);
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
});

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    IGetUniswapV2PairsBatchRequest,
    "src/amms/abi/GetUniswapV2PairsBatchRequest.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    IGetUniswapV2PoolDataBatchRequest,
    "src/amms/abi/GetUniswapV2PoolDataBatchRequest.json"
);

#[derive(Error, Debug)]
pub enum UniswapV2Error {
    #[error("Swap arithmetic overflow")]
    ArithmeticOverflow,
    #[error("Invalid swap fee: {0} (maximum 100000)")]
    InvalidFee(usize),
    #[error("Invalid token pair: {base} -> {quote}")]
    InvalidTokenPair { base: Address, quote: Address },
    #[error("Division by zero")]
    DivisionByZero,
    #[error("Rounding Error")]
    RoundingError,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UniswapV2Pool {
    /// Unique instance number identifier, used for internal debugging.
    /// Two instances for the same pool adddress would have different values.
    #[serde(default)]
    instance_id: usize,

    /// A base64 encoding of the address, used for internal debugging
    /// However, unlike Self.instance, the field is unique per token address, and not per instance
    /// Not the canonical ERC20 symbol
    #[serde(default)]
    address_id: String,

    pub address: Address,
    pub token_a: Token,
    pub token_b: Token,
    pub reserve_0: u128,
    pub reserve_1: u128,
    pub fee: usize,
    pub count: Option<isize>,
}

impl AutomatedMarketMaker for UniswapV2Pool {
    fn address(&self) -> Address {
        self.address
    }

    fn sync_events(&self) -> Vec<B256> {
        vec![IUniswapV2Pair::Sync::SIGNATURE_HASH]
    }

    fn sync(&mut self, log: &Log) -> Result<(), AMMError> {
        let sync_event = IUniswapV2Pair::Sync::decode_log(&log.inner)?;

        let (reserve_0, reserve_1) = (
            sync_event.reserve0.to::<u128>(),
            sync_event.reserve1.to::<u128>(),
        );

        info!(
            target = "amm::uniswap_v2::sync",
            address = ?self.address,
            reserve_0, reserve_1, "Sync"
        );

        self.reserve_0 = reserve_0;
        self.reserve_1 = reserve_1;
        Ok(())
    }

    fn simulate_swap(
        &self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        let forward = self.swap_direction(base_token, quote_token)?;
        let (reserve_in, reserve_out) = if forward {
            (self.reserve_0, self.reserve_1)
        } else {
            (self.reserve_1, self.reserve_0)
        };
        self.get_amount_out(amount_in, U256::from(reserve_in), U256::from(reserve_out))
    }

    fn simulate_swap_mut(
        &mut self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        let forward = self.swap_direction(base_token, quote_token)?;
        let amount_out = self.simulate_swap(base_token, quote_token, amount_in)?;
        let input = u128::try_from(amount_in).map_err(|_| UniswapV2Error::ArithmeticOverflow)?;
        let output = u128::try_from(amount_out).map_err(|_| UniswapV2Error::ArithmeticOverflow)?;
        let (reserve_in, reserve_out) = if forward {
            (self.reserve_0, self.reserve_1)
        } else {
            (self.reserve_1, self.reserve_0)
        };
        // Calculate both values before publishing either, so errors leave the pool unchanged.
        let reserve_in = reserve_in
            .checked_add(input)
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        let reserve_out = reserve_out
            .checked_sub(output)
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        (self.reserve_0, self.reserve_1) = if forward {
            (reserve_in, reserve_out)
        } else {
            (reserve_out, reserve_in)
        };
        Ok(amount_out)
    }

    fn tokens(&self) -> Vec<Address> {
        vec![self.token_a.address, self.token_b.address]
    }

    fn calculate_price(&self, base_token: Address, _quote_token: Address) -> Result<f64, AMMError> {
        let price = self.calculate_price_64_x_64(base_token)?;
        q64_to_float(price)
    }

    async fn init<N, P>(mut self, block_number: BlockId, provider: P) -> Result<Self, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let deployer = IGetUniswapV2PoolDataBatchRequestInstance::deploy_builder(
            provider.clone(),
            vec![self.address()],
        );

        let res = deployer.call_raw().block(block_number).await?;

        let pool_data =
            <Vec<(Address, Address, u128, u128, u32, u32)> as SolValue>::abi_decode(&res)?[0];

        if pool_data.0.is_zero() {
            todo!("Return error");
        }

        self.token_a = Token::new_with_decimals(pool_data.0, pool_data.4 as u8);
        self.token_b = Token::new_with_decimals(pool_data.1, pool_data.5 as u8);
        self.reserve_0 = pool_data.2;
        self.reserve_1 = pool_data.3;

        // TODO: populate fee?

        Ok(self)
    }
}

pub fn u128_to_float(num: u128) -> Result<Float, AMMError> {
    let value_string = num.to_string();
    let parsed_value = Float::parse_radix(value_string, 10)?;
    Ok(Float::with_val(MPFR_T_PRECISION, parsed_value))
}

impl UniswapV2Pool {
    fn new_instance_id() -> usize {
        UNISWAP_V2_POOL_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_address_id(address: Address) -> String {
        let mut hasher = Keccak256::new();
        hasher.update(address);
        let hash = hasher.finalize();

        // Take first 8 bytes → u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        let num = u64::from_be_bytes(bytes);

        base62_encode(num)
    }

    pub fn set_ids(&mut self) {
        (self.instance_id == 0).then(|| self.instance_id = Self::new_instance_id());
        (self.address_id.is_empty()).then(|| self.address_id = Self::new_address_id(self.address));
    }

    pub fn id(&self) -> String {
        format!("uni2-{}", self.address_id)
    }

    // Create a new, unsynced UniswapV2 pool
    // TODO: update the init function to derive the fee
    pub fn new(address: Address, fee: usize) -> Self {
        Self {
            address,
            fee,
            instance_id: Self::new_instance_id(),
            ..Default::default()
        }
    }

    fn swap_direction(&self, base: Address, quote: Address) -> Result<bool, AMMError> {
        if base != quote && base == self.token_a.address && quote == self.token_b.address {
            Ok(true)
        } else if base != quote && base == self.token_b.address && quote == self.token_a.address {
            Ok(false)
        } else {
            Err(UniswapV2Error::InvalidTokenPair { base, quote }.into())
        }
    }

    /// Calculates output with fees in units of 1/100000, rounding down.
    /// Returns an error if any intermediate calculation overflows U256.
    pub fn get_amount_out(
        &self,
        amount_in: U256,
        reserve_in: U256,
        reserve_out: U256,
    ) -> Result<U256, AMMError> {
        let fee_multiplier = U256_100000
            .checked_sub(U256::from(self.fee))
            .ok_or(UniswapV2Error::InvalidFee(self.fee))?;
        if amount_in.is_zero() || reserve_in.is_zero() || reserve_out.is_zero() {
            return Ok(U256::ZERO);
        }
        let effective_input = amount_in
            .checked_mul(fee_multiplier)
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        let numerator = effective_input
            .checked_mul(reserve_out)
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        let denominator = reserve_in
            .checked_mul(U256_100000)
            .and_then(|reserve| reserve.checked_add(effective_input))
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        let amount_out = numerator
            .checked_div(denominator)
            .ok_or(UniswapV2Error::ArithmeticOverflow)?;
        Ok(amount_out)
    }

    /// Calculates the price of the base token in terms of the quote token.
    ///
    /// Returned as a Q64 fixed point number.
    pub fn calculate_price_64_x_64(&self, base_token: Address) -> Result<u128, AMMError> {
        let decimal_shift = self.token_a.decimals as i8 - self.token_b.decimals as i8;

        let (r_0, r_1) = if decimal_shift < 0 {
            (
                U256::from(self.reserve_0)
                    * U256::from(10u128.pow(decimal_shift.unsigned_abs() as u32)),
                U256::from(self.reserve_1),
            )
        } else {
            (
                U256::from(self.reserve_0),
                U256::from(self.reserve_1) * U256::from(10u128.pow(decimal_shift as u32)),
            )
        };

        if base_token == self.token_a.address {
            if r_0.is_zero() {
                Ok(U128_0X10000000000000000)
            } else {
                div_uu(r_1, r_0)
            }
        } else if r_1.is_zero() {
            Ok(U128_0X10000000000000000)
        } else {
            div_uu(r_0, r_1)
        }
    }

    pub fn swap_calldata(
        &self,
        amount_0_out: U256,
        amount_1_out: U256,
        to: Address,
        calldata: Vec<u8>,
    ) -> Result<Bytes, AMMError> {
        Ok(IUniswapV2Pair::swapCall {
            amount0Out: amount_0_out,
            amount1Out: amount_1_out,
            to,
            data: calldata.into(),
        }
        .abi_encode()
        .into())
    }
}

pub fn div_uu(x: U256, y: U256) -> Result<u128, AMMError> {
    if !y.is_zero() {
        let mut answer;

        if x <= U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {
            answer = (x << U256_64) / y;
        } else {
            let mut msb = U256_192;
            let mut xc = x >> U256_192;

            if xc >= U256_0X100000000 {
                xc >>= U256_32;
                msb += U256_32;
            }

            if xc >= U256_0X10000 {
                xc >>= U256_16;
                msb += U256_16;
            }

            if xc >= U256_0X100 {
                xc >>= U256_8;
                msb += U256_8;
            }

            if xc >= U256_16 {
                xc >>= U256_4;
                msb += U256_4;
            }

            if xc >= U256_4 {
                xc >>= U256_2;
                msb += U256_2;
            }

            if xc >= U256_2 {
                msb += U256_1;
            }

            answer = (x << (U256_255 - msb)) / (((y - U256_1) >> (msb - U256_191)) + U256_1);
        }

        if answer > U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {
            return Ok(0);
        }

        let hi = answer * (y >> U256_128);
        let mut lo = answer * (y & U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF);

        let mut xh = x >> U256_192;
        let mut xl = x << U256_64;

        if xl < lo {
            xh -= U256_1;
        }

        xl = xl.overflowing_sub(lo).0;
        lo = hi << U256_128;

        if xl < lo {
            xh -= U256_1;
        }

        xl = xl.overflowing_sub(lo).0;

        if xh != hi >> U256_128 {
            return Err(UniswapV2Error::RoundingError.into());
        }

        answer += xl / y;

        if answer > U256_0XFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {
            return Ok(0_u128);
        }

        Ok(answer.to::<u128>())
    } else {
        Err(UniswapV2Error::DivisionByZero.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct UniswapV2Factory {
    pub address: Address,
    pub fee: usize,
    pub creation_block: u64,
    pub pair_discovery_retry_attempts: usize,
    pub stage: FilterStage,
}

impl UniswapV2Factory {
    const DEFAULT_PAIR_DISCOVERY_RETRY_ATTEMPTS: usize = 10;

    pub fn new(address: Address, fee: usize, creation_block: u64) -> Self {
        Self {
            address,
            creation_block,
            fee,
            pair_discovery_retry_attempts: Self::DEFAULT_PAIR_DISCOVERY_RETRY_ATTEMPTS,
            stage: FilterStage::Discovery,
        }
    }

    pub fn with_pair_discovery_retry_attempts(mut self, attempts: usize) -> Self {
        self.pair_discovery_retry_attempts = attempts;
        self
    }

    pub async fn get_all_pairs<N, P>(
        factory_address: Address,
        block_number: BlockId,
        provider: P,
        max_retry_attempts: usize,
    ) -> Result<Vec<Address>, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let factory = IUniswapV2FactoryInstance::new(factory_address, provider.clone());
        let pairs_length = factory
            .allPairsLength()
            .call()
            .block(block_number)
            .await?
            .to::<usize>();
        let step = 766;
        info!(
            target = "amms::uniswap_v2::get_all_pairs",
            pairs_length, "Getting all pairs"
        );
        let retry_delay = Duration::from_secs(10);
        let starts = (0..pairs_length).step_by(step).collect::<Vec<_>>();

        let (batches, _failed_starts) = run_retry_queue(
            starts,
            |start| {
                let provider = provider.clone();
                async move {
                    let deployer = IGetUniswapV2PairsBatchRequest::deploy_builder(
                        provider,
                        U256::from(start),
                        U256::from(step),
                        factory_address,
                    );

                    match deployer.call_raw().block(block_number).await {
                        Ok(res) => {
                            let return_data = <Vec<Address> as SolValue>::abi_decode(&res)?;
                            Ok::<RetryQueueOutcome<Vec<Address>, usize>, AMMError>(
                                RetryQueueOutcome::Success(return_data),
                            )
                        }
                        Err(_err) => Ok::<RetryQueueOutcome<Vec<Address>, usize>, AMMError>(
                            RetryQueueOutcome::Retry(start),
                        ),
                    }
                }
            },
            max_retry_attempts,
            retry_delay,
            "amms::uniswap_v2::get_all_pairs",
        )
        .await?;

        let mut pairs = Vec::new();
        for tokens in batches {
            for token in tokens {
                if !token.is_zero() {
                    pairs.push(token);
                }
            }
        }

        Ok(pairs)
    }

    pub async fn sync_all_pools<N, P>(
        amms: Vec<AMM>,
        block_number: BlockId,
        provider: P,
        max_retry_attempts: usize,
    ) -> Result<Vec<AMM>, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let step = 120;
        let groups = amms
            .iter()
            .chunks(step)
            .into_iter()
            .map(|chunk| chunk.map(|amm| amm.address()).collect())
            .collect::<Vec<Vec<Address>>>();

        let retry_delay = Duration::from_secs(20);
        let (batch_results, _failed_groups) = run_retry_queue(
            groups,
            |group| {
                let provider = provider.clone();
                async move {
                    let deployer = IGetUniswapV2PoolDataBatchRequestInstance::deploy_builder(
                        provider,
                        group.clone(),
                    );

                    match deployer.call_raw().block(block_number).await {
                        Ok(res) => {
                            let return_data = <Vec<(Address, Address, u128, u128, u32, u32)> as SolValue>::abi_decode(&res)?;
                            Ok::<
                                RetryQueueOutcome<
                                    (Vec<Address>, Vec<(Address, Address, u128, u128, u32, u32)>),
                                    Vec<Address>,
                                >,
                                AMMError,
                            >(RetryQueueOutcome::Success((group, return_data)))
                        }
                        Err(_err) => Ok::<
                            RetryQueueOutcome<
                                (Vec<Address>, Vec<(Address, Address, u128, u128, u32, u32)>),
                                Vec<Address>,
                            >,
                            AMMError,
                        >(RetryQueueOutcome::Retry(group)),
                    }
                }
            },
            max_retry_attempts,
            retry_delay,
            "amms::uniswap_v2::sync_all_pools",
        )
        .await?;

        let mut amms = amms
            .into_iter()
            .map(|amm| (amm.address(), amm))
            .collect::<HashMap<_, _>>();

        for (group, return_data) in batch_results {
            for (pool_data, pool_address) in return_data.iter().zip(group.iter()) {
                // If the pool token A is not zero, signaling that the pool data was polulated

                if pool_data.0.is_zero() {
                    continue;
                }

                let amm = amms.get_mut(pool_address).unwrap();

                let AMM::UniswapV2Pool(pool) = amm else {
                    // TODO:: We should never receive a non UniswapV2Pool AMM here, we can handle this more gracefully in the future
                    panic!("Unexpected pool type")
                };

                pool.token_a = Token::new_with_decimals(pool_data.0, pool_data.4 as u8);
                pool.token_b = Token::new_with_decimals(pool_data.1, pool_data.5 as u8);
                pool.reserve_0 = pool_data.2;
                pool.reserve_1 = pool_data.3;
            }
        }

        let amms = amms
            .into_iter()
            .filter_map(|(_, amm)| {
                if amm.tokens().iter().any(|t| t.is_zero()) {
                    None
                } else {
                    Some(amm)
                }
            })
            .collect();

        Ok(amms)
    }
}

impl AutomatedMarketMakerFactory for UniswapV2Factory {
    type PoolVariant = UniswapV2Pool;

    fn address(&self) -> Address {
        self.address
    }

    fn stage(&self) -> FilterStage {
        self.stage
    }

    fn pool_creation_event(&self) -> B256 {
        IUniswapV2Factory::PairCreated::SIGNATURE_HASH
    }

    fn create_pool(&self, log: Log) -> Result<AMM, AMMError> {
        let event = IUniswapV2Factory::PairCreated::decode_log(&log.inner)?;
        let address = event.pair;
        Ok(AMM::UniswapV2Pool(UniswapV2Pool {
            address,
            token_a: event.token0.into(),
            token_b: event.token1.into(),
            reserve_0: 0,
            reserve_1: 0,
            fee: self.fee,
            count: None,
            instance_id: UniswapV2Pool::new_instance_id(),
            address_id: UniswapV2Pool::new_address_id(address),
        }))
    }

    fn creation_block(&self) -> u64 {
        self.creation_block
    }
}

impl DiscoverySync for UniswapV2Factory {
    fn discover<N, P>(
        &self,
        to_block: BlockId,
        provider: P,
    ) -> impl Future<Output = Result<Vec<AMM>, AMMError>>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        info!(
            target = "amms::uniswap_v2::discover",
            address = ?self.address,
            "Discovering all pools"
        );

        let provider = provider.clone();
        async move {
            let pairs = UniswapV2Factory::get_all_pairs(
                self.address,
                to_block,
                provider.clone(),
                self.pair_discovery_retry_attempts,
            )
            .await?;

            Ok(pairs
                .into_iter()
                .map(|pair| {
                    AMM::UniswapV2Pool(UniswapV2Pool {
                        address: pair,
                        token_a: Address::default().into(),
                        token_b: Address::default().into(),
                        reserve_0: 0,
                        reserve_1: 0,
                        fee: self.fee,
                        count: None,
                        instance_id: UniswapV2Pool::new_instance_id(),
                        address_id: UniswapV2Pool::new_address_id(pair),
                    })
                })
                .collect())
        }
    }

    fn sync<N, P>(
        &self,
        amms: Vec<AMM>,
        to_block: BlockId,
        provider: P,
    ) -> impl Future<Output = Result<Vec<AMM>, AMMError>>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        info!(
            target = "amms::uniswap_v2::sync",
            address = ?self.address,
            "Syncing all pools"
        );
        UniswapV2Factory::sync_all_pools(
            amms,
            to_block,
            provider,
            self.pair_discovery_retry_attempts,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::amms::{amm::AutomatedMarketMaker, uniswap_v2::UniswapV2Pool, Token};
    use alloy::primitives::{address, Address, U256};

    #[test]
    fn test_get_amount_out() {
        // Independently calculated integer reference values, including substantial price impact.
        for (fee, input, reserve_in, reserve_out, expected) in [
            (125, 1_000_000u64, 10_000_000u64, 10_000_000u64, 908_057u64),
            (150, 1_000_000, 10_000_000, 10_000_000, 907_851),
            (300, 1_000_000, 10_000_000, 10_000_000, 906_610),
            (1000, 1_000_000, 10_000_000, 10_000_000, 900_818),
            (300, 1_000_000, 1_000_000, 1_000_000, 499_248),
            (300, 10_000, 1_000_000, 2_000_000, 19_743),
            (300, 10_000, 2_000_000, 1_000_000, 4_960),
            (300, 1, 1_000_000, 1_000_000, 0),
        ] {
            let pool = UniswapV2Pool::new(Address::ZERO, fee);
            assert_eq!(
                pool.get_amount_out(
                    U256::from(input),
                    U256::from(reserve_in),
                    U256::from(reserve_out)
                )
                .unwrap(),
                U256::from(expected)
            );
        }
    }

    #[test]
    fn swap_overflows_and_invalid_fees_return_errors() {
        let mut pool = UniswapV2Pool::new(Address::ZERO, 300);
        for (input, reserve_in, reserve_out) in [
            (U256::MAX, U256::ONE, U256::ONE),
            (U256::from(2), U256::ONE, U256::MAX),
            (U256::ONE, U256::MAX, U256::ONE),
            (U256::ONE, U256::MAX / U256::from(100_000), U256::ONE),
        ] {
            assert!(matches!(
                pool.get_amount_out(input, reserve_in, reserve_out),
                Err(crate::amms::error::AMMError::UniswapV2Error(
                    super::UniswapV2Error::ArithmeticOverflow
                ))
            ));
        }
        pool.fee = 100_001;
        assert!(matches!(
            pool.get_amount_out(U256::ONE, U256::ONE, U256::ONE),
            Err(crate::amms::error::AMMError::UniswapV2Error(
                super::UniswapV2Error::InvalidFee(100_001)
            ))
        ));
    }

    #[test]
    fn swap_validates_both_tokens_and_preserves_reserves_on_error() {
        let a = Address::repeat_byte(1);
        let b = Address::repeat_byte(2);
        let unknown = Address::repeat_byte(3);
        let mut pool = UniswapV2Pool::new(Address::ZERO, 300);
        pool.token_a.address = a;
        pool.token_b.address = b;
        pool.reserve_0 = 1_000_000;
        pool.reserve_1 = 2_000_000;
        for (base, quote) in [
            (a, a),
            (b, b),
            (unknown, a),
            (a, unknown),
            (unknown, unknown),
        ] {
            assert!(matches!(
                pool.simulate_swap(base, quote, U256::ONE),
                Err(crate::amms::error::AMMError::UniswapV2Error(
                    super::UniswapV2Error::InvalidTokenPair { .. }
                ))
            ));
            assert!(pool.simulate_swap_mut(base, quote, U256::ONE).is_err());
            assert_eq!((pool.reserve_0, pool.reserve_1), (1_000_000, 2_000_000));
        }
        for (base, quote) in [(a, b), (b, a)] {
            let mut updated = pool.clone();
            let output = pool.simulate_swap(base, quote, U256::from(10_000)).unwrap();
            assert_eq!(
                updated
                    .simulate_swap_mut(base, quote, U256::from(10_000))
                    .unwrap(),
                output
            );
            let expected = if base == a {
                (1_010_000, 2_000_000 - output.to::<u128>())
            } else {
                (1_000_000 - output.to::<u128>(), 2_010_000)
            };
            assert_eq!((updated.reserve_0, updated.reserve_1), expected);
        }
        pool.reserve_0 = u128::MAX;
        pool.reserve_1 = 1;
        assert!(pool.simulate_swap_mut(a, b, U256::ONE).is_err());
        assert_eq!((pool.reserve_0, pool.reserve_1), (u128::MAX, 1));
        pool.reserve_0 = 1;
        assert!(pool
            .simulate_swap_mut(a, b, U256::from(u128::MAX) + U256::ONE)
            .is_err());
        assert_eq!((pool.reserve_0, pool.reserve_1), (1, 1));
    }

    #[test]
    fn test_calculate_price_edge_case() {
        let token_a = address!("0d500b1d8e8ef31e21c99d1db9a6444d3adf1270");
        let token_b = address!("8f18dc399594b451eda8c5da02d0563c0b2d0f16");
        let address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let pool = UniswapV2Pool {
            address,
            token_a: Token::new_with_decimals(
                address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                6,
            ),
            token_b: Token::new_with_decimals(
                address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
                18,
            ),
            reserve_0: 23595096345912178729927,
            reserve_1: 154664232014390554564,
            fee: 300,
            count: None,
            instance_id: UniswapV2Pool::new_instance_id(),
            address_id: UniswapV2Pool::new_address_id(address),
        };

        assert!(pool.calculate_price(token_a, Address::default()).unwrap() != 0.0);
        assert!(pool.calculate_price(token_b, Address::default()).unwrap() != 0.0);
    }

    #[tokio::test]
    async fn test_calculate_price() {
        let address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let pool = UniswapV2Pool {
            address,
            token_a: Token::new_with_decimals(
                address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                6,
            ),
            token_b: Token::new_with_decimals(
                address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
                18,
            ),
            reserve_0: 47092140895915,
            reserve_1: 28396598565590008529300,
            fee: 300,
            count: None,
            instance_id: UniswapV2Pool::new_instance_id(),
            address_id: UniswapV2Pool::new_address_id(address),
        };

        let price_a_64_x = pool
            .calculate_price(pool.token_a.address, Address::default())
            .unwrap();
        let price_b_64_x = pool
            .calculate_price(pool.token_b.address, Address::default())
            .unwrap();

        // No precision loss: 30591574867092394336528 / 2**64
        assert_eq!(1658.3725965327264, price_b_64_x);
        // Precision loss: 11123401407064628 / 2**64
        assert_eq!(0.0006030007985483893, price_a_64_x);
    }

    #[tokio::test]
    async fn test_calculate_price_64_x_64() {
        let address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let pool = UniswapV2Pool {
            address,
            token_a: Token::new_with_decimals(
                address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                6,
            ),
            token_b: Token::new_with_decimals(
                address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
                18,
            ),
            reserve_0: 47092140895915,
            reserve_1: 28396598565590008529300,
            fee: 300,
            count: None,
            instance_id: UniswapV2Pool::new_instance_id(),
            address_id: UniswapV2Pool::new_address_id(address),
        };

        let price_a_64_x = pool.calculate_price_64_x_64(pool.token_a.address).unwrap();
        let price_b_64_x = pool.calculate_price_64_x_64(pool.token_b.address).unwrap();

        assert_eq!(30591574867092394336528, price_b_64_x);
        assert_eq!(11123401407064628, price_a_64_x);
    }
}

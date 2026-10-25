pub mod constants;
pub mod discovery;
pub mod error;
pub mod filters;
#[cfg(test)]
mod tests;

use crate::amms;
use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::error::ReorgError;
use crate::amms::factory::Factory;
use crate::amms::formatters::debug_formatters::{dbg_block_ref, fmt_prefix};
use crate::amms::math::percentage_change_bp;
use crate::amms::path::find_arb_paths_v2;
use crate::amms::path::UniswapArbPath;
use crate::amms::path::UniswapArbPaths;
use crate::amms::path::{
    CachedSimulation, SimulationBatch, SimulationContext, UniswapSimulationResult,
};
use crate::amms::retry_queue;
use crate::amms::uniswap_v2::IUniswapV2Pair;
use crate::amms::uniswap_v2::UniswapV2Factory;
use crate::amms::uniswap_v2::UniswapV2Pool;
use crate::amms::uniswap_v3::IUniswapV3PoolEvents;
use crate::state_space::constants::address::WETH_ADDRESS;
use crate::state_space::constants::WETH_AMOUNT_IN;
use crate::state_space::filters::FilterStage;

use alloy::consensus::BlockHeader;
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::network::primitives::HeaderResponse;
use alloy::primitives::BlockHash;
use alloy::primitives::BlockNumber;
use alloy::primitives::Uint;
use alloy::primitives::{I256, U256};
use alloy::rpc::types::FilterBlockOption;
use alloy::rpc::types::Header;
use alloy::rpc::types::{Block, Filter, FilterSet, Log};
use alloy::sol_types::SolEvent;
use alloy::{
    network::Network,
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use alloy_network::BlockResponse;
use async_stream::stream;
use chrono::DateTime;
use chrono::{Local, Utc};

use error::StateSpaceError;
use filters::AMMFilter;
use filters::PoolFilter;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::collections::VecDeque;
use std::debug_assert;
use std::fmt;
use std::fmt::Debug;
use std::fs::read_to_string;
use std::fs::File;
use std::io::Error;
use std::ops::Not;
use std::pin::Pin;
use std::u128;
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::warn;
use tracing::{debug, info};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PoolReserves {
    pub r0: u128,
    pub r1: u128,
}

impl PoolReserves {
    pub fn default() -> Self {
        PoolReserves {
            r0: u128::MIN,
            r1: u128::MIN,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PoolDiff {
    pub topic: FixedBytes<32>,
    pub topic_name: String,
    pub address: Address,
    pub pre: PoolReserves,
    pub post: PoolReserves,
}

impl PoolDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr = UniswapV2Pool::new_address_id(self.address);
        writeln!(f, "diff-{}:", addr)?;
        fmt_prefix(f, &self.topic_name, "\t")?;
        let args = format_args!(
            "({}, {}) -> ({}, {})",
            self.pre.r0, self.pre.r1, self.post.r0, self.post.r1
        );
        fmt_prefix(f, &args, "\t")
    }
}

impl fmt::Display for PoolDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for PoolDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

type AMMBlockDiff = Vec<PoolDiff>;

#[derive(Clone, Serialize, Deserialize)]
pub struct BlockRef {
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub number: BlockNumber,
    pub timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub block_diff: Option<AMMBlockDiff>,
}

impl From<Header> for BlockRef {
    fn from(header: Header) -> Self {
        Self {
            hash: header.hash,
            parent_hash: header.inner.parent_hash,
            number: header.number,
            timestamp: header.timestamp(),
            base_fee_per_gas: header.base_fee_per_gas(),
            gas_used: header.gas_used(),
            gas_limit: header.gas_limit(),
            block_diff: None,
        }
    }
}

impl fmt::Display for BlockRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        dbg_block_ref(self, f)
    }
}

impl fmt::Debug for BlockRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        dbg_block_ref(self, f)
    }
}

impl BlockRef {
    /* Clone without block diff */
    pub fn shallow_clone(&self) -> Self {
        Self {
            hash: self.hash,
            parent_hash: self.parent_hash,
            number: self.number,
            timestamp: self.timestamp,
            base_fee_per_gas: self.base_fee_per_gas,
            gas_used: self.gas_used,
            gas_limit: self.gas_limit,
            block_diff: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BlockBuffer {
    pub blocks: VecDeque<BlockRef>,
    pub capacity: u64,
}

impl Default for BlockBuffer {
    fn default() -> Self {
        Self {
            blocks: VecDeque::with_capacity(64),
            capacity: 64,
        }
    }
}

impl BlockBuffer {
    pub fn push(&mut self, block: BlockRef) {
        self.blocks.push_back(block);

        if self.blocks.len() > (self.capacity as usize) {
            self.blocks.pop_front();
        }
    }

    pub fn head(&self) -> Option<&BlockRef> {
        self.blocks.iter().last()
    }

    pub fn clone_head(&self) -> Option<BlockRef> {
        let b = self.blocks.iter().last();
        b.cloned()
    }

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.blocks.len();
        writeln!(f, "BlockBuffer ({}/{})", len, self.capacity)?;

        let blocks = self.blocks.iter().rev();

        match len {
            0..=4 => {
                for block in blocks {
                    fmt_prefix(f, block, "\t")?;
                }
            }
            _ => {
                let top2 = blocks.clone().take(2).clone();
                let bottom2 = blocks.skip(len - 2);
                for block in top2 {
                    fmt_prefix(f, block, "\t")?;
                }

                writeln!(f, "\t...")?;

                for block in bottom2 {
                    fmt_prefix(f, block, "\t")?;
                }
            }
        }

        write!(f, "")
    }
}

impl fmt::Debug for BlockBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Display for BlockBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    pub block_filter: Filter,
    pub provider: P,
    pub pubsub_provider: P,
    phantom: PhantomData<N>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    filters: Vec<PoolFilter>,
    factories: Vec<Factory>,
    arb_paths_v2: UniswapArbPaths,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSpaceJSONFile {
    amms: Vec<AMM>,
    meta: CacheMeta,
}

/* Reorg from the closest buffered ancestor to new_head.
 *  - Updates the state's head buffer with the new branch
 *  - Updates self.state and removes pool diffs from pruned branch
 *  - Updates self.state and applied pool diffs of new branch
 *  - Returns a result of <new_head, error> */
impl<N, P> StateSpaceManager<N, P> {
    pub async fn head(&self) -> Option<BlockRef> {
        self.state.read().await.clone_head()
    }
    pub async fn reorg(&self, new_head: BlockRef) -> Result<BlockRef, StateSpaceError>
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let provider = self.provider.clone();

        info!(
            ?new_head.hash,
            ?new_head.number,
            target = "StateSpaceManager::reorg",
            "Starting reorg"
        );

        // ---------------------------
        // Phase 0: Snapshot canonical state (short read lock)
        // ---------------------------
        let (max_depth, hashes) = {
            let state = self.state.read().await;
            let guard = &state.head_buffer;

            let max_depth = guard.capacity as usize;

            let hashes: Vec<_> = guard.blocks.iter().map(|b| b.hash).collect();

            (max_depth, hashes)
        };

        let find_ancestor = |hash| hashes.iter().position(|&h| h == hash);

        // ---------------------------
        // Phase 1: Walk backwards via RPC (no lock)
        // ---------------------------
        let mut cursor = new_head.shallow_clone();
        let mut depth = 0usize;
        let mut new_branch: Vec<BlockRef> = Vec::new();

        let (ancestor_idx, ancestor_hash) = loop {
            if let Some(idx) = find_ancestor(cursor.hash) {
                info!(
                    hash = cursor.hash.to_string(),
                    number = cursor.number,
                    ?depth,
                    ?idx,
                    target = "StateSpaceManager::reorg",
                    "Found common ancestor"
                );
                break (idx, cursor.hash);
            }

            if depth >= max_depth {
                tracing::error!(
                    ?new_head.hash,
                    ?max_depth,
                    target = "StateSpaceManager::reorg",
                    "Reorg too deep"
                );
                return Err(ReorgError::ReeorgTooDeep {
                    max_depth: max_depth as u64,
                }
                .into());
            }

            new_branch.push(cursor.shallow_clone());

            let parent_hash = cursor.parent_hash;

            let parent_block = match provider
                .get_block_by_hash(parent_hash)
                .await
                .map_err(|e| ReorgError::TransportError(e))
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(
                        ?parent_hash,
                        target = "StateSpaceManager::reorg",
                        "Transport error fetching parent block"
                    );
                    return Err(e.into());
                }
            };

            let parent_block = match parent_block {
                Some(b) => b,
                None => {
                    tracing::error!(
                        ?parent_hash,
                        target = "StateSpaceManager::reorg",
                        "Missing parent block during reorg"
                    );
                    return Err(ReorgError::MissingBlock { hash: parent_hash }.into());
                }
            };

            cursor = parent_block.header.into();

            depth += 1;
        };

        info!(
            new_branch_len = new_branch.len(),
            ?ancestor_hash,
            target = "StateSpaceManager::reorg",
            "Computed new branch"
        );

        // Sanity check - makes sure the branches line up
        {
            let state = self.state.read().await;
            let head_buffer = &state.head_buffer;
            let ancestor_block =
                head_buffer
                    .blocks
                    .get(ancestor_idx)
                    .ok_or(ReorgError::ReorgFailed {
                        message: "ancestor block not found",
                    })?;
            let first_block_of_new_branch = new_branch.last().ok_or(ReorgError::ReorgFailed {
                message: "new branch is empty",
            })?;

            if (ancestor_block.hash != first_block_of_new_branch.parent_hash) {
                return Err(ReorgError::ReorgFailed { message: "ancestor block hash is not equal to the parent hash of the first block of the new branch" }.into());
            } else if (ancestor_block.number != first_block_of_new_branch.number - 1) {
                return Err(ReorgError::ReorgFailed { message: "ancestor block number is not equal to the block number - 1 of first block of the new branch" }.into());
            }
        };

        // Fetch before changing live state. A failed RPC leaves the old branch intact.
        let mut replacement_logs = Vec::with_capacity(new_branch.len());
        for block in new_branch.iter().rev() {
            let filter = self.block_filter.clone().at_block_hash(block.hash);
            replacement_logs.push(self.provider.get_logs(&filter).await?);
        }

        // Stage rollback/replay privately, then publish pools and head together.
        // There are no awaits after acquiring the write guard.
        let mut state_guard = self.state.write().await;
        if !state_guard
            .head_buffer
            .blocks
            .iter()
            .map(|b| b.hash)
            .eq(hashes.iter().copied())
        {
            return Err(ReorgError::ReorgFailed {
                message: "Head buffer changed during reorg",
            }
            .into());
        }
        let mut staged_state = state_guard.pools.clone();
        let mut staged_buffer = state_guard.head_buffer.clone();
        let pruned = staged_buffer.blocks.split_off(ancestor_idx + 1);
        info!(target = "state_space::sync", from = ?state_guard.clone_head().map(|head| head.number), to = ?staged_buffer.clone_head().map(|head| head.number), "Unwinding state changes");
        for block in pruned.iter().rev() {
            let diff = block.block_diff.as_ref().ok_or(ReorgError::ReorgFailed {
                message: "Pruned branch missing block diff",
            })?;
            for change in diff.iter().rev() {
                let Some(AMM::UniswapV2Pool(pool)) = staged_state.get_mut(&change.address) else {
                    return Err(ReorgError::ReorgFailed {
                        message: "Pruned diff references missing or unsupported pool",
                    }
                    .into());
                };
                pool.reserve_0 = change.pre.r0;
                pool.reserve_1 = change.pre.r1;
                pool.count = Some(
                    pool.count
                        .filter(|count| *count > 0)
                        .ok_or(ReorgError::ReorgFailed {
                            message: "Invalid pool count during rollback",
                        })?
                        - 1,
                );
                debug!(target = "state_space::sync", amm = ?pool, "Reverting AMM state (staged)");
            }
        }
        for (block, logs) in new_branch.iter().rev().zip(replacement_logs) {
            let diff = StateSpace::apply_logs(&mut staged_state, logs)?;
            debug!(target = "state_space::sync", block_hash = ?block.hash, state_change = ?diff, "Caching state change (staged)");
            staged_buffer.push(BlockRef {
                block_diff: Some(diff),
                ..block.shallow_clone()
            });
        }
        let head = staged_buffer.clone_head().ok_or(ReorgError::ReorgFailed {
            message: "Head buffer is empty after reorg",
        })?;
        state_guard.pools = staged_state;
        state_guard.head_buffer = staged_buffer;
        state_guard.invalidate_simulations();

        info!(?head, target = "StateSpaceManager::reorg", "Reorg complete");
        Ok(head)
    }

    pub async fn revert_block_diff(&self, diff: AMMBlockDiff) -> Option<StateSpaceError> {
        let mut state_guard = self.state.write().await;
        let state = &mut state_guard.pools;
        for pool_diff in diff.iter().rev() {
            match state.get_mut(&pool_diff.address) {
                Some(AMM::UniswapV2Pool(pool)) => {
                    debug_assert_eq!(
                        pool_diff.topic,
                        IUniswapV2Pair::Sync::SIGNATURE_HASH,
                        "Pool diff topic ({}) is not IUniswapV2Pair::Sync::SIGNATURE_HASH",
                        pool_diff.topic
                    );
                    pool.reserve_0 = pool_diff.pre.r0;
                    pool.reserve_1 = pool_diff.pre.r1;
                    pool.count = Some(pool.count.unwrap() - 1);
                    debug!(target = "state_space::sync", amm = ?pool, "Reverting AMM state");
                }
                _ => unreachable!(),
            }
        }
        state_guard.invalidate_simulations();
        Option::None
    }

    pub async fn extract_apply_block_diff(
        &self,
        block_hash: BlockHash,
    ) -> Result<AMMBlockDiff, StateSpaceError>
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let block_filter = self.block_filter.clone().at_block_hash(block_hash);
        let provider = self.provider.clone();
        let logs = provider
            .get_logs(&block_filter)
            .await
            .map_err(|e| StateSpaceError::TransportError(e))?;
        let mut state_guard = self.state.write().await;
        let diff = state_guard.apply_pool_logs(logs)?;
        state_guard.invalidate_simulations();
        Ok(diff)
    }

    pub fn subscribe(
        self: Arc<Self>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<SimulationBatch, StateSpaceError>> + Send>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
        BlockRef: From<<N as Network>::HeaderResponse>,
    {
        let provider = self.pubsub_provider.clone();

        Ok(Box::pin(stream! {
            let block_stream = provider.subscribe_blocks().await?.into_stream();
            tokio::pin!(block_stream);

            while let Some(next_block) = block_stream.next().await {
                info!(
                    target = "state_space::subscribe",
                    number = next_block.number(),
                    hash = next_block.hash().to_string(),
                    "Processing next block"
                );
                if let Some(batch) = self.process_next_block(next_block.into()).await? {
                    yield Ok(batch);
                }
            }
        }))
    }

    // Keep block processing independently testable without a websocket connection.
    async fn process_next_block(
        &self,
        block_ref: BlockRef,
    ) -> Result<Option<SimulationBatch>, StateSpaceError>
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let next_hash = block_ref.hash;
        let (duplicate, curr_hash) = {
            let state = self.state.read().await;
            let head_hash = state
                .clone_head()
                .map(|head| head.hash)
                .expect("Expected head hash");
            (
                head_hash == next_hash
                    || state.head_buffer.blocks.iter().any(|b| b.hash == next_hash),
                head_hash,
            )
        };
        if duplicate {
            info!(
                target = "state_space::subscribe",
                "Duplicate notification: block already processed"
            );
            return Ok(None);
        }
        if curr_hash != block_ref.parent_hash {
            info!(target = "state_space::subscribe", ?next_hash, next_parent_hash = ?block_ref.parent_hash, "Reorg detected");
            let head = self.reorg(block_ref).await?;
            let state = self.state.write().await;
            if state.clone_head().map(|current| current.hash) != Some(head.hash) {
                return Err(Error::new(
                    std::io::ErrorKind::Other,
                    "State changed before reorg simulation",
                )
                .into());
            }
            // Rollbacks and backfilled blocks can affect paths absent from the final block diff.
            //state.simulate_all_paths(U256::from(WETH_AMOUNT_IN))?
        } else {
            let filter = self.block_filter.clone().at_block_hash(next_hash);
            let logs = self.provider.get_logs(&filter).await?;
            let mut state = self.state.write().await;
            state.apply_block(block_ref, logs)?;
            //state.simulate_affected_paths(U256::from(WETH_AMOUNT_IN))?
        };

        let batch = {
            let mut state = self.state.write().await;
            let amount_in = U256::from(WETH_AMOUNT_IN);
            if state.head_buffer.blocks.len() == 2 {
                state.simulate_all_paths(amount_in)?
            } else {
                state.simulate_affected_paths(amount_in)?
            }
        };
        {
            let state = self.state.read().await;
            info!(
                target = "state_space::subscribe",
                "{:#?}", state.head_buffer
            );
            info!(target = "state_space::subscribe", "{}", state);
        }
        // The write guard has dropped before the batch reaches the stream consumer.
        Ok(Some(batch))
    }

    pub async fn resync(&self) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone + 'static,
    {
        self.resync_from_block(BlockId::latest()).await
    }

    pub async fn resync_from_block(&self, block: BlockId) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone + 'static,
    {
        let sync_start = Instant::now();
        let (old_map, expected_head) = {
            let state = self.state.read().await;
            (
                state.pools.clone(),
                state.clone_head().map(|head| head.hash),
            )
        };
        let target =
            self.provider.get_block(block).await?.ok_or_else(|| {
                Error::new(std::io::ErrorKind::NotFound, "Resync block not found")
            })?;
        let header = target.header();
        let head = BlockRef {
            hash: header.hash(),
            parent_hash: header.parent_hash(),
            number: header.number(),
            timestamp: header.timestamp(),
            base_fee_per_gas: header.base_fee_per_gas(),
            gas_used: header.gas_used(),
            gas_limit: header.gas_limit(),
            block_diff: None,
        };
        // Pin every pool read to the same block, even if the chain advances.
        let block_id = BlockId::hash_canonical(head.hash);
        let n_prev_amms = old_map.len();
        let mut amm_variants = HashMap::new();
        for (_, amm) in old_map {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }
        let mut new_pools = HashMap::new();
        for (variant, remaining_amms) in amm_variants {
            match variant {
                amms::amm::Variant::UniswapV2Pool => {
                    let pools = UniswapV2Factory::sync_all_pools(
                        remaining_amms,
                        block_id,
                        self.provider.clone(),
                        5,
                    )
                    .await?;
                    for pool in pools {
                        new_pools.insert(pool.address(), pool);
                    }
                }
                _ => info!(
                    "Skipping syncing {} AMMs of variant {:?}",
                    remaining_amms.len(),
                    variant
                ),
            }
        }
        let n_curr_amms = new_pools.len();
        {
            let mut state = self.state.write().await;
            if state.clone_head().map(|head| head.hash) != expected_head {
                return Err(Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "State advanced during resync; retry against the new head",
                )
                .into());
            }
            state.pools = new_pools;
            state.head_buffer.blocks.clear();
            state.head_buffer.push(head.clone());
            state.synced_at = Some(head.clone());
            state.invalidate_simulations();
        }
        info!(target = "state_space:resync", elapsed_secs = sync_start.elapsed().as_secs_f32(),
            n_prev_amms, n_curr_amms, block_number = head.number, block_hash = ?head.hash,
            "State space resync complete");
        Ok(())
    }

    pub async fn simulate_all_paths(&self) -> Result<SimulationBatch, Error> {
        self.state
            .write()
            .await
            .simulate_all_paths(U256::from(WETH_AMOUNT_IN))
    }

    pub async fn simulate_affected_paths(&self) -> Result<SimulationBatch, Error> {
        self.state
            .write()
            .await
            .simulate_affected_paths(U256::from(WETH_AMOUNT_IN))
    }
}

// TODO: Drop impl, create a checkpoint
#[derive(Debug, Default)]
pub struct StateSpaceBuilder<N, P> {
    pub http_provider: P,
    pub pubsub_provider: P,
    pub latest_block: u64,
    pub factories: Vec<Factory>,
    pub amms: Vec<AMM>,
    pub arb_paths: UniswapArbPaths,
    pub filters: Vec<PoolFilter>,
    phantom: PhantomData<N>,
    output_file: Option<String>,
}

impl<N, P> StateSpaceBuilder<N, P>
where
    N: Network,
    P: Provider<N> + Clone + 'static,
{
    pub fn new(provider: P) -> StateSpaceBuilder<N, P> {
        Self {
            http_provider: provider.clone(),
            pubsub_provider: provider.clone(),
            latest_block: 0,
            factories: vec![],
            amms: vec![],
            filters: vec![],
            arb_paths: UniswapArbPaths::default(),
            output_file: Option::None,
            // discovery: false,
            phantom: PhantomData,
        }
    }

    pub fn block(self, latest_block: u64) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder {
            latest_block,
            ..self
        }
    }

    pub fn with_factories(self, factories: Vec<Factory>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { factories, ..self }
    }

    pub fn with_amms(self, amms: Vec<AMM>) -> StateSpaceBuilder<N, P> {
        let amms_with_ids = amms
            .into_iter()
            .map(|amm| {
                if let AMM::UniswapV2Pool(mut pool) = amm {
                    pool.set_ids();
                    pool.reserve_0 = 0;
                    pool.reserve_1 = 0;
                    AMM::UniswapV2Pool(pool)
                } else {
                    unreachable!("This is not supposed to happen!");
                }
            })
            .collect_vec();

        StateSpaceBuilder {
            amms: amms_with_ids,
            ..self
        }
    }

    pub fn with_filters(self, filters: Vec<PoolFilter>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { filters, ..self }
    }

    pub fn from_cache(self, input_file: String) -> StateSpaceBuilder<N, P> {
        debug!(
            target: "state_space::from_cache",
            path = %input_file,
            "Loading amms from cache file"
        );

        let contents_str = read_to_string(input_file.clone()).unwrap();

        let mut value = serde_json::from_str::<StateSpaceJSONFile>(contents_str.as_str()).unwrap();

        value.amms.iter_mut().for_each(|amm| {
            if let AMM::UniswapV2Pool(pool) = amm {
                pool.set_ids();
                pool.reserve_0 = 0;
                pool.reserve_1 = 0;
            } else {
                unreachable!("This not supposed to happen!");
            }
        });

        StateSpaceBuilder {
            filters: value.meta.filters,
            factories: value.meta.factories,
            arb_paths: value.meta.arb_paths_v2,
            amms: value.amms,
            ..self
        }
    }

    pub fn to_cache(self, output_file: Option<String>) -> StateSpaceBuilder<N, P> {
        let output_file = output_file.unwrap_or_else(|| {
            let date_time = Utc::now().to_rfc3339();
            format!("cache/pools/{date_time}.json")
        });
        StateSpaceBuilder {
            output_file: Some(output_file),
            ..self
        }
    }

    pub fn with_pubsub_provider(self, pubsub_provider: P) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder {
            pubsub_provider,
            ..self
        }
    }

    pub async fn sync(self) -> Result<StateSpaceManager<N, P>, AMMError> {
        let sync_start = Instant::now();
        let factories_count = self.factories.len();
        let filters_count = self.filters.len();
        let latest_block = self
            .http_provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?
            .expect("Failed to fetch latest block");
        let block_ref = BlockRef {
            hash: latest_block.header().hash(),
            parent_hash: latest_block.header().parent_hash(),
            number: latest_block.header().number(),
            timestamp: latest_block.header().timestamp(),
            base_fee_per_gas: latest_block.header().base_fee_per_gas(),
            gas_used: latest_block.header().gas_used(),
            gas_limit: latest_block.header().gas_limit(),
            block_diff: None,
        };
        let block_id = BlockId::from(latest_block.header().number());
        let factories = self.factories.clone();
        let discovery_factories = factories
            .clone()
            .into_iter()
            .filter(|f| f.stage() == FilterStage::Discovery)
            .collect_vec();
        let mut futures = FuturesUnordered::new();
        let mut filter_set = HashSet::new();
        for factory in &self.factories {
            for event in factory.pool_events() {
                filter_set.insert(event);
            }
        }

        for amm in self.amms.iter() {
            for event in amm.sync_events() {
                filter_set.insert(event);
            }
        }
        //

        let block_filter = Filter::new().event_signature(FilterSet::from(
            filter_set.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        let mut amm_variants = HashMap::new();
        for amm in self.amms.into_iter() {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        for factory in discovery_factories {
            let provider = self.http_provider.clone();
            let filters = self.filters.clone();

            let extension = amm_variants.remove(&factory.variant());
            futures.push(tokio::spawn(async move {
                let mut discovered_amms = factory.discover(block_id, provider.clone()).await?;

                if let Some(amms) = extension {
                    discovered_amms.extend(amms);
                }

                // Apply discovery filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Discovery {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Discovery filter"
                        );
                    }
                }

                discovered_amms = factory.sync(discovered_amms, block_id, provider).await?;

                // Apply sync filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Sync {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Sync filter"
                        );
                    }
                }

                Ok::<Vec<AMM>, AMMError>(discovered_amms)
            }));
        }

        let mut state_space = StateSpace::default();
        state_space.chain_id = Some(self.http_provider.get_chain_id().await?);
        while let Some(res) = futures.next().await {
            let synced_amms = res??;

            for amm in synced_amms {
                state_space.pools.insert(amm.address(), amm);
            }
        }

        // Sync remaining AMM variants
        for (variant, remaining_amms) in amm_variants.drain() {
            match variant {
                amms::amm::Variant::UniswapV2Pool => {
                    info!("Syncing {} UniswapV2 AMMs", remaining_amms.len());
                    let res = UniswapV2Factory::sync_all_pools(
                        remaining_amms,
                        block_id,
                        self.http_provider.clone(),
                        5,
                    )
                    .await?;
                    for amm in res {
                        state_space.pools.insert(amm.address(), amm);
                    }
                }
                // TODO other variantss
                _ => info!(
                    "Skipping syncing {} AMMs of variant {:?}",
                    remaining_amms.len(),
                    variant
                ),
            };
        }

        let new_amms_count = state_space.pools.len();

        let uniswapv2pools = state_space
            .pools
            .values()
            .filter_map(|value| {
                if let AMM::UniswapV2Pool(pool) = value {
                    Some(pool.clone())
                } else {
                    None
                }
            })
            .collect_vec();

        let arb_paths = find_arb_paths_v2(uniswapv2pools, WETH_ADDRESS);

        if let Some(path) = self.output_file.as_deref() {
            debug!(
                target = "state_space::sync",
                path = %path,
                amms = %new_amms_count,
                "Syncing to output file"
            );

            let file = File::create_new(path)
                .inspect_err(|e| {
                    println!("Error creating file at path {}: {:?}", path, e);
                })
                .map_err(|e| AMMError::FileError(e))?;

            let all_amms = state_space.pools.values().cloned().collect::<Vec<AMM>>();

            let file_contents = StateSpaceJSONFile {
                amms: all_amms,
                meta: CacheMeta {
                    arb_paths_v2: arb_paths.clone(),
                    filters: self.filters.clone(),
                    factories: self.factories.clone(),
                },
            };

            serde_json::to_writer(file, &file_contents).map_err(|e| AMMError::JSONError(e))?;
        }

        state_space.arb_paths = arb_paths;
        state_space.head_buffer.push(block_ref.clone());
        state_space.synced_at = Some(block_ref);
        let ssm = StateSpaceManager {
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.http_provider.clone(),
            pubsub_provider: self.pubsub_provider.clone(),
            phantom: PhantomData,
        };

        info!(
            target: "state_space::sync",
            elapsed_secs = sync_start.elapsed().as_secs_f32(),
            factories = factories_count,
            filters = filters_count,
            amms = new_amms_count,
            "State space sync complete"
        );

        Ok(ssm)
    }
}

#[derive(Debug, Default)]
pub struct StateSpace {
    pub chain_id: Option<u64>,
    pub pools: HashMap<Address, AMM>,
    pub arb_paths: UniswapArbPaths,
    pub head_buffer: BlockBuffer,
    pub synced_at: Option<BlockRef>,
}

impl StateSpace {
    /// The head represented by this state's pool reserves.
    pub fn clone_head(&self) -> Option<BlockRef> {
        self.head_buffer.clone_head()
    }

    fn invalidate_simulations(&mut self) {
        for entry in &mut self.arb_paths.paths {
            entry.last_simulation = None;
        }
    }

    /// Commit a complete block under the caller's single state write guard.
    pub(crate) fn apply_block(
        &mut self,
        mut block: BlockRef,
        logs: Vec<Log>,
    ) -> Result<BlockRef, StateSpaceError> {
        if let Some(head) = self.clone_head() {
            if block.parent_hash != head.hash || head.number.checked_add(1) != Some(block.number) {
                return Err(ReorgError::ReorgFailed {
                    message: "Block does not extend the current head",
                }
                .into());
            }
        }
        Self::validate_block_logs(&block, &logs)?;
        block.block_diff = Some(self.apply_pool_logs(logs)?);
        debug!(target = "state_space::sync", block_hash = ?block.hash, state_change = ?block.block_diff, "Caching state change");
        self.head_buffer.push(block.clone());
        self.invalidate_simulations();
        Ok(block)
    }

    fn validate_block_logs(block: &BlockRef, logs: &[Log]) -> Result<(), StateSpaceError> {
        if logs.iter().any(|log| {
            log.block_hash.is_some_and(|hash| hash != block.hash)
                || log
                    .block_number
                    .is_some_and(|number| number != block.number)
        }) {
            return Err(ReorgError::ReorgFailed {
                message: "Logs do not belong to the requested block",
            }
            .into());
        }
        Ok(())
    }

    /// Stage only touched pools so a decoding error leaves live reserves intact.
    fn apply_pool_logs(&mut self, logs: Vec<Log>) -> Result<AMMBlockDiff, StateSpaceError> {
        let mut staged_pools: HashMap<_, _> = logs
            .iter()
            .filter_map(|log| {
                self.pools
                    .get(&log.address())
                    .map(|pool| (log.address(), pool.clone()))
            })
            .collect();
        let diff = Self::apply_logs(&mut staged_pools, logs)?;
        self.pools.extend(staged_pools);
        Ok(diff)
    }

    /// Simulate against one guarded state and return results with their shared head context.
    pub fn simulate_all_paths(&mut self, amount_in: U256) -> Result<SimulationBatch, Error> {
        info!(
            target = "amms::state_space::StateSpace::simulate_all_paths",
            amount_in = amount_in.to_string()
        );
        self.simulate_path_indices(amount_in, (0..self.arb_paths.paths.len()).collect())
    }

    fn simulate_path_indices(
        &mut self,
        amount_in: U256,
        path_indices: Vec<usize>,
    ) -> Result<SimulationBatch, Error> {
        debug!(
            target = "amms::state_space::StateSpace::simulate_path_indices",
            amount_in = amount_in.to_string(),
            n_paths = path_indices.len(),
            path_indices = path_indices.iter().join(",")
        );
        let mut simulation_results = Vec::new();
        let mut successful_indices = Vec::new();
        let head = self
            .clone_head()
            .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "No synced head available"))?;
        let chain_id = self
            .chain_id
            .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "No chain ID available"))?;
        let pools = &self.pools;
        for path_index in path_indices {
            let path_entry = &mut self.arb_paths.paths[path_index];
            let simulated_result = path_entry.path.resolve_pools(pools).and_then(|path| {
                let amount_out = path.simulate(amount_in)?;
                let pct_change = percentage_change_bp(amount_in, amount_out)?;
                Ok((amount_out, path.snapshot(), pct_change))
            });
            match simulated_result {
                Err(e) => {
                    path_entry.last_simulation = None;
                    warn!(
                        path_index = path_index,
                        error = e.to_string(),
                        target = "state_space::StateSpaceManager::simulate_all_paths",
                        "Error simulating path",
                    );
                }
                Ok((amount_out, path_snapshot, pct_change)) => {
                    let full_result = UniswapSimulationResult {
                        path_snapshot,
                        amount_in,
                        amount_out,
                        pct_gain_bp: pct_change,
                    };
                    successful_indices.push(path_index);
                    simulation_results.push(full_result);
                    // debug!(
                    //     target = "amms::state_space::StateSpace::simulate_path_indices",
                    //     ?path_index,
                    //     ?pct_change,
                    //     ?amount_out,
                    //     "Sucessfully simulated path"
                    // );
                }
            }
        }
        let context = Arc::new(SimulationContext {
            chain_id,
            block_number: head.number,
            block_hash: head.hash,
            parent_hash: head.parent_hash,
            block_timestamp: head.timestamp,
            base_fee_per_gas: head.base_fee_per_gas,
            gas_used: head.gas_used,
            gas_limit: head.gas_limit,
            simulated_at: Local::now().to_utc(),
        });
        // Attach the same completion timestamp and head context to every cached result.
        for (index, result) in successful_indices.into_iter().zip(&simulation_results) {
            self.arb_paths.paths[index].last_simulation = Some(CachedSimulation {
                context: Arc::clone(&context),
                result: result.clone(),
            });
        }
        info!(
            target = "amms::state_space::StateSpace::simulate_path_indices",
            n_simulation_results = simulation_results.len(),
            "Finished simulating paths"
        );
        Ok(SimulationBatch {
            context,
            results: simulation_results,
        })
    }

    /// Simulate against one guarded state and return results with their shared head context.
    pub fn simulate_affected_paths(&mut self, amount_in: U256) -> Result<SimulationBatch, Error> {
        info!(
            target = "amms::state_space::StateSpace::simulate_affected_paths",
            amount_in = amount_in.to_string()
        );
        let head = self
            .head_buffer
            .head()
            .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "No head available"))?;
        let affected_pools = head
            .block_diff
            .as_ref()
            .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "No block diff available"))?;
        let path_indices = affected_pools
            .iter()
            .filter_map(|pool| self.arb_paths.paths_by_pool.get(&pool.address))
            .flatten()
            .copied()
            .unique()
            .collect();
        self.simulate_path_indices(amount_in, path_indices)
    }

    fn apply_logs(
        state: &mut HashMap<Address, AMM>,
        logs: Vec<Log>,
    ) -> Result<AMMBlockDiff, StateSpaceError> {
        let mut block_diff = AMMBlockDiff::new();
        for log in logs {
            let address = log.address();
            /* If we dont have this AMM, we can discard the log events */
            if let Some(amm) = state.get_mut(&address) {
                match log.topic0() {
                    Some(topic0 @ &IUniswapV2Pair::Sync::SIGNATURE_HASH) => match amm {
                        AMM::UniswapV2Pool(pool) => {
                            let decoded_log = IUniswapV2Pair::Sync::decode_log(&log.inner)
                                .map_err(|e| StateSpaceError::AlloyError(e))?;
                            let r0_post = decoded_log.reserve0.to::<u128>();
                            let r1_post = decoded_log.reserve1.to::<u128>();
                            let pool_diff = PoolDiff {
                                topic_name: IUniswapV2Pair::Sync::SIGNATURE.to_string(),
                                topic: *topic0,
                                address,
                                pre: PoolReserves {
                                    r0: pool.reserve_0,
                                    r1: pool.reserve_1,
                                },
                                post: PoolReserves {
                                    r0: r0_post,
                                    r1: r1_post,
                                },
                            };
                            pool.reserve_0 = r0_post;
                            pool.reserve_1 = r1_post;
                            pool.count = Some(pool.count.unwrap_or(0) + 1);
                            info!(target = "state_space::sync", amm = ?pool, "Synced AMM (staged)");
                            block_diff.push(pool_diff);
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
        }
        Ok(block_diff)
    }

    pub fn get(&self, address: &Address) -> Option<&AMM> {
        self.pools.get(address)
    }

    pub fn get_mut(&mut self, address: &Address) -> Option<&mut AMM> {
        self.pools.get_mut(address)
    }

    pub fn get_counts(&self) -> Vec<String> {
        let pools = self.pools.values().clone().filter_map(|amm| match amm {
            AMM::UniswapV2Pool(pool) => Some(pool.clone()),
            _ => None,
        });
        let top_pools = pools
            .sorted_by_key(|pool| pool.count.unwrap_or_default())
            .rev()
            .take(10)
            .map(|pool| {
                format!(
                    "{}::{} ({})",
                    pool.id(),
                    pool.address,
                    pool.count.unwrap_or_default()
                )
            })
            .collect_vec();
        top_pools
    }

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let l1 = if f.alternate() { 20 } else { 10 };
        let l2 = self.pools.len();

        writeln!(f, "StateSpace ({}/{})", l1, l2)?;

        for amm in self.pools.values().take(l1) {
            match amm {
                AMM::UniswapV2Pool(pool) => {
                    writeln!(f, "{}:", pool.id())?;
                    writeln!(
                        f,
                        "\t0: {} -> {} ({})",
                        pool.token_a.id(),
                        pool.reserve_0,
                        pool.token_a.decimals
                    )?;
                    writeln!(
                        f,
                        "\t1: {} -> {} ({})",
                        pool.token_b.id(),
                        pool.reserve_1,
                        pool.token_b.decimals
                    )?;
                }
                _ => {
                    writeln!(f, "Unknown amm")?;
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for StateSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

#[macro_export]
macro_rules! sync {
    // Sync factories with provider
    ($factories:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .sync()
            .await?
    }};

    // Sync factories with filters
    ($factories:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_filters($filters)
            .sync()
            .await?
    }};

    ($factories:expr, $amms:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_amms($amms)
            .with_filters($filters)
            .sync()
            .await?
    }};
}

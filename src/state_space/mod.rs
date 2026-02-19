pub mod cache;
pub mod discovery;
pub mod error;
pub mod filters;

use crate::amms;
use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::error::IOError;
use crate::amms::error::ReorgError;
use crate::amms::factory::Factory;
use crate::amms::io::amms_file_exists;
use crate::amms::uniswap_v2::UniswapV2Factory;
use crate::amms::uniswap_v2::UniswapV2Pool;

use alloy::consensus::BlockHeader;
use alloy::eips::BlockId;
use alloy::network::primitives::HeaderResponse;
use alloy::primitives::BlockHash;
use alloy::primitives::BlockNumber;
use alloy::rpc::types::{Block, Filter, FilterSet, Log};
use alloy::{
    network::Network,
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use async_stream::stream;
use cache::StateChange;
use cache::StateChangeCache;
use chrono::Local;

use core::num;
use error::StateSpaceError;
use eyre::OptionExt;
use filters::AMMFilter;
use filters::PoolFilter;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::read_to_string;
use std::fs::{exists as file_exists, File};
use std::mem;
use std::ops::Not;
use std::os::macos::raw::stat;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;

pub const CACHE_SIZE: usize = 30;

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct BlockRef {
    hash: BlockHash,
    number: BlockNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadBuffer {
    block_heads: VecDeque<BlockRef>,
    capacity: u64,
}

impl HeadBuffer {
    pub fn push(&mut self, block_head: BlockRef) {
        self.block_heads.push_back(block_head);

        if self.block_heads.len() > (self.capacity as usize) {
            self.block_heads.pop_front();
        }
    }

    pub fn get_ref_at(&self, index: usize) -> Option<BlockRef> {
        self.block_heads.get(index).copied()
    }
}

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    pub latest_block: Arc<RwLock<BlockHash>>,
    // discovery_manager: Option<DiscoveryManager>,
    pub block_filter: Filter,
    pub provider: P,
    pub pubsub_provider: P,
    pub head_buffer: HeadBuffer,
    phantom: PhantomData<N>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    filters: Vec<PoolFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSpaceJSONFile {
    amms: Vec<AMM>,
    meta: CacheMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDiff {
    foo: usize,
}

/* Reorg from the closest ancestor({block}, {self.head_buffer.block_heads[0]}) to {block}
 *  - Updates self.head_buffer.block_heads with head_refs of new branch
 *  - Updates self.state and removes pool diffs from pruned branch
 *  - Updates self.state and applied pool diffs of new branch */
impl<N, P> StateSpaceManager<N, P> {
    pub async fn reorg(
        &mut self,
        mut block: <N as Network>::HeaderResponse,
    ) -> Result<VecDeque<BlockRef>, StateSpaceError>
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let mut new_branch: Vec<(BlockRef, PoolDiff)> = Vec::new();
        let mut depth = 0;
        let max_depth = self.head_buffer.capacity;

        while depth < max_depth {
            depth += 1;
            // 1️⃣ Check if this hash exists in our canonical buffer
            if let Some((idx, _)) = self
                .head_buffer
                .block_heads
                .iter()
                .rev()
                .find_position(|b| b.hash == block.hash())
            {
                // Found common ancestor
                let pruned_branch = self.head_buffer.block_heads.split_off(idx);
                pruned_branch.into_iter().for_each(|a| {
                    //let pool_diff = self.pool_diffs.get_mut(a.hash);
                    //self.revertPoolDiff(a)
                });
                new_branch.into_iter().for_each(|(a, b)| {
                    self.head_buffer.push(a);
                    //self.pool_diffs.set(a.hash, b);
                    //self.applyPoolDiff(b);
                });
                return Ok(pruned_branch);
            }

            // 2️⃣ Not found — push this block into new branch
            // TODO: collect pool reserve diffs per block and apply to new branch
            new_branch.push_front(block_ref.clone());

            // 3️⃣ Fetch parent block via RPC
            //let parent_hash = block_ref.parent_hash();

            let next_block = self
                .provider
                .get_block_by_hash(block_ref.hash)
                .await
                .map_err(|e| ReorgError::TransportError(e))?
                .ok_or(ReorgError::MissingBlock {
                    hash: block_ref.hash,
                })?;

            block_ref = BlockRef {
                hash: next_block.header.parent_hash(),
                number: next_block.header.number(),
            }
        }

        Err(ReorgError::ReeorgTooDeep { max_depth }.into())
    }

    pub fn subscribe(
        self: Arc<Self>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<Address>, StateSpaceError>> + Send>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let provider = self.pubsub_provider.clone();
        let state = self.state.clone();
        let mut block_filter = self.block_filter.clone();

        Ok(Box::pin(stream! {
            let block_stream = provider.subscribe_blocks().await?.into_stream();
            tokio::pin!(block_stream);
            // mut not needed on block_stream if the line above is uncommented

            while let Some(block) = block_stream.next().await {
                let parent_hash = block.parent_hash();
                let curr_head = self.head_buffer.get_ref_at(0).ok_or(StateSpaceError::MissingBlockAtIdx(0))?;
                if parent_hash != curr_head.hash {
                    // reorg, rollback to canonical state
                    let new_head = self.reorg(block);
                    //rollback(new_head)
                };

                //self.head_buffer.push((block_hash, number));




                let block_number = block.number();
                let block_hash = block.hash();
                let block_id = BlockId::from(block_number);
                if block_id.is_finalized() {
                    self.resync_from_block(block_id).await?;
                }

                block_filter = block_filter.select(block_number);


                let logs = provider.get_logs(&block_filter).await?;

                let affected_amms = state.write().await.sync(&logs)?;
                let mut latest_block = self.latest_block.write().await;
                latest_block = block_hash;

                yield Ok(affected_amms);
            }
        }))
    }

    pub async fn subscribe_new_blocks(
        &self,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<Address>, StateSpaceError>> + Send>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let provider = self.pubsub_provider.clone();
        let latest_block = self.latest_block.clone();
        let state = self.state.clone();
        let mut block_filter = self.block_filter.clone();

        let block_stream = provider.subscribe_blocks().await?.into_stream();

        Ok(Box::pin(stream! {
            tokio::pin!(block_stream);

            while let Some(block) = block_stream.next().await {
                let now = Local::now().to_string();
                let block_number = block.number();
                block_filter = block_filter.select(block_number);


                let logs = provider.get_logs(&block_filter).await?;

                let affected_amms = state.write().await.sync(&logs)?;
                latest_block.store(block_number, Ordering::Relaxed);

                yield Ok(affected_amms);
            }
        }))
    }

    pub async fn resync(&mut self) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone + 'static,
    {
        let sync_start = Instant::now();
        let chain_tip = BlockId::from(self.provider.get_block_number().await?);

        let old_map = {
            let state = self.state.read().await;
            state.state.clone()
        };

        let n_prev_amms = old_map.values().len();

        let mut amm_variants = HashMap::new();
        for (_, amm) in old_map {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        let mut new_state = HashMap::new();

        for (variant, remaining_amms) in amm_variants.drain() {
            match variant {
                amms::amm::Variant::UniswapV2Pool => {
                    info!("Syncing {} UniswapV2 AMMs", remaining_amms.len());
                    let res = UniswapV2Factory::sync_all_pools(
                        remaining_amms,
                        chain_tip,
                        self.provider.clone(),
                        5,
                    )
                    .await?;
                    for amm in res {
                        new_state.insert(amm.address(), amm);
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

        let n_curr_amms = new_state.values().len();

        {
            let mut state = self.state.write().await;
            state.state = new_state;
        }

        info!(
            target = "state_space:resync",
            elapsed_secs = sync_start.elapsed().as_secs_f32(),
            elapsed = sync_start.elapsed().as_secs_f32(),
            ?n_prev_amms,
            ?n_curr_amms,
            chain_tip = chain_tip.as_u64(),
            "State space resync complete"
        );

        Ok(())
    }

    pub async fn resync_from_block(&self, block: BlockId) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone + 'static,
    {
        let chain_tip = BlockId::from(self.provider.get_block_number().await?);

        let sync_start = Instant::now();

        let old_map = {
            let state = self.state.read().await;
            state.state.clone()
        };

        let n_prev_amms = old_map.values().len();

        let mut amm_variants = HashMap::new();
        for (_, amm) in old_map {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        let mut new_state = HashMap::new();

        for (variant, remaining_amms) in amm_variants.drain() {
            match variant {
                amms::amm::Variant::UniswapV2Pool => {
                    info!("Syncing {} UniswapV2 AMMs", remaining_amms.len());
                    let res = UniswapV2Factory::sync_all_pools(
                        remaining_amms,
                        block,
                        self.provider.clone(),
                        5,
                    )
                    .await?;
                    for amm in res {
                        new_state.insert(amm.address(), amm);
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

        let n_curr_amms = new_state.values().len();

        {
            let mut state = self.state.write().await;
            state.state = new_state;
        }

        info!(
            target = "state_space:resync_from_block",
            elapsed_secs = sync_start.elapsed().as_secs_f32(),
            elapsed = sync_start.elapsed().as_secs_f32(),
            ?n_prev_amms,
            ?n_curr_amms,
            chain_tip = chain_tip.as_u64(),
            block = block.as_u64(),
            "State space resync from block complete"
        );

        Ok(())
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
        StateSpaceBuilder { amms, ..self }
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
                pool.reserve_0 = 0;
                pool.reserve_1 = 0;
            } else {
                unreachable!("This not supposed to happen!");
            }
        });

        StateSpaceBuilder {
            amms: value.amms.clone(),
            ..self
        }
    }

    pub fn to_cache(self, output_file: String) -> StateSpaceBuilder<N, P> {
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
        let chain_tip = BlockId::from(self.http_provider.get_block_number().await?);
        let factories = self.factories.clone();
        let mut futures = FuturesUnordered::new();
        //
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

        for factory in factories {
            let provider = self.http_provider.clone();
            let filters = self.filters.clone();

            let extension = amm_variants.remove(&factory.variant());
            futures.push(tokio::spawn(async move {
                let mut discovered_amms = factory.discover(chain_tip, provider.clone()).await?;

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

                discovered_amms = factory.sync(discovered_amms, chain_tip, provider).await?;

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
        while let Some(res) = futures.next().await {
            let synced_amms = res??;

            for amm in synced_amms {
                state_space.state.insert(amm.address(), amm);
            }
        }

        // Sync remaining AMM variants
        for (variant, remaining_amms) in amm_variants.drain() {
            match variant {
                amms::amm::Variant::UniswapV2Pool => {
                    info!("Syncing {} UniswapV2 AMMs", remaining_amms.len());
                    let res = UniswapV2Factory::sync_all_pools(
                        remaining_amms,
                        chain_tip,
                        self.http_provider.clone(),
                        5,
                    )
                    .await?;
                    for amm in res {
                        state_space.state.insert(amm.address(), amm);
                    }
                }
                // TODO other variantss
                _ => info!(
                    "Skipping syncing {} AMMs of variant {:?}",
                    remaining_amms.len(),
                    variant
                ),
            };

            // for mut amm in remaining_amms {
            //     let address = amm.address();
            //     amm = amm.init(chain_tip, self.provider.clone()).await?;
            //     state_space.state.insert(address, amm);
            // }
        }

        let new_amms_count = state_space.state.values().cloned().count();

        if let Some(path) = self.output_file.as_deref() {
            debug!(
                target: "state_space::sync",
                path = %path,
                amms = %new_amms_count,
                "Attempting to sync to output file"
            );

            let file = File::create_new(path)
                .inspect_err(|e| {
                    println!("Error creating file at path {}: {:?}", path, e);
                })
                .map_err(|e| AMMError::FileError(e))?;

            let amms = state_space.state.values().cloned().collect::<Vec<AMM>>();

            let file_contents = StateSpaceJSONFile {
                amms,
                meta: CacheMeta {
                    filters: self.filters.clone(),
                },
            };

            serde_json::to_writer(file, &file_contents).map_err(|e| AMMError::JSONError(e))?;
        }

        let ssm = StateSpaceManager {
            latest_block: Arc::new(AtomicU64::new(self.latest_block)),
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.http_provider.clone(),
            pubsub_provider: self.pubsub_provider.clone(),
            phantom: PhantomData,
            head_buffer: HeadBuffer {
                block_heads: VecDeque::with_capacity(64),
                capacity: 64,
            },
        };

        info!(
            target: "state_space::sync",
            elapsed_secs = sync_start.elapsed().as_secs_f32(),
            factories = factories_count,
            amms = new_amms_count,
            "State space sync complete"
        );

        Ok(ssm)
    }
}

#[derive(Debug, Default)]
pub struct StateSpace {
    pub state: HashMap<Address, AMM>,
    pub latest_block: Arc<AtomicU64>,
    cache: StateChangeCache<CACHE_SIZE>,
}

impl StateSpace {
    pub fn get(&self, address: &Address) -> Option<&AMM> {
        self.state.get(address)
    }

    pub fn get_mut(&mut self, address: &Address) -> Option<&mut AMM> {
        self.state.get_mut(address)
    }

    pub fn sync(&mut self, logs: &[Log]) -> Result<Vec<Address>, StateSpaceError> {
        let latest = self.latest_block.load(Ordering::Relaxed);
        let Some(mut block_number) = logs
            .first()
            .map(|log| log.block_number.ok_or(StateSpaceError::MissingBlockNumber))
            .transpose()?
        else {
            return Ok(vec![]);
        };

        // Check if there is a reorg and unwind to state before block_number
        if latest >= block_number {
            info!(
                target: "state_space::sync",
                from = %latest,
                to = %block_number - 1,
                "Unwinding state changes"
            );

            let cached_state = self.cache.unwind_state_changes(block_number);
            for amm in cached_state {
                debug!(target: "state_space::sync", ?amm, "Reverting AMM state");
                self.state.insert(amm.address(), amm);
            }
        }

        let mut cached_amms = HashSet::new();
        let mut affected_amms = HashSet::new();
        for log in logs {
            // If the block number is updated, cache the current block state changes
            let log_block_number = log
                .block_number
                .ok_or(StateSpaceError::MissingBlockNumber)?;
            if log_block_number != block_number {
                let amms = cached_amms.drain().collect::<Vec<AMM>>();
                affected_amms.extend(amms.iter().map(|amm| amm.address()));
                let state_change = StateChange::new(amms, block_number);

                debug!(
                    target: "state_space::sync",
                    state_change = ?state_change,
                    "Caching state change"
                );

                self.cache.push(state_change);
                block_number = log_block_number;
            }

            // If the AMM is in the state space add the current state to cache and sync from log
            let address = log.address();
            if let Some(amm) = self.state.get_mut(&address) {
                cached_amms.insert(amm.clone());
                amm.sync(log)?;

                info!(
                    target: "state_space::sync",
                    ?amm,
                    "Synced AMM"
                );
            }
        }

        if !cached_amms.is_empty() {
            let amms = cached_amms.drain().collect::<Vec<AMM>>();
            affected_amms.extend(amms.iter().map(|amm| amm.address()));
            let state_change = StateChange::new(amms, block_number);

            debug!(
                target: "state_space::sync",
                state_change = ?state_change,
                "Caching state change"
            );

            self.cache.push(state_change);
        }

        Ok(affected_amms.into_iter().collect())
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

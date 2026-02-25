pub mod cache;
pub mod discovery;
pub mod error;
pub mod filters;

use crate::amms;
use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::error::ReorgError;
use crate::amms::factory::Factory;
use crate::amms::uniswap_v2::IUniswapV2Pair;
use crate::amms::uniswap_v2::UniswapV2Factory;
use crate::amms::uniswap_v2::UniswapV2Pool;
use crate::amms::uniswap_v3::IUniswapV3PoolEvents;

use alloy::consensus::BlockHeader;
use alloy::eips::BlockId;
use alloy::network::primitives::HeaderResponse;
use alloy::primitives::BlockHash;
use alloy::primitives::BlockNumber;
use alloy::primitives::Uint;
use alloy::rpc::types::FilterBlockOption;
use alloy::rpc::types::Header;
use alloy::rpc::types::{Block, Filter, FilterSet, Log};
use alloy::sol_types::SolEvent;
use alloy::{
    network::Network,
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use async_stream::stream;
use cache::StateChange;
use cache::StateChangeCache;
use chrono::Local;

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
use std::fs::read_to_string;
use std::fs::File;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::u128;
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;

pub const CACHE_SIZE: usize = 30;

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Debug, Copy)]
pub struct PoolDiff {
    pub topic: FixedBytes<32>,
    pub address: Address,
    pub pre: PoolReserves,
    pub post: PoolReserves,
}

type AMMBlockDiff = Vec<PoolDiff>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BlockRef {
    hash: BlockHash,
    parent_hash: BlockHash,
    number: BlockNumber,
    block_diff: Option<AMMBlockDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockBuffer {
    blocks: VecDeque<BlockRef>,
    capacity: u64,
}

impl BlockBuffer {
    pub fn push(&mut self, block: BlockRef) {
        self.blocks.push_back(block);

        if self.blocks.len() > (self.capacity as usize) {
            self.blocks.pop_front();
        }
    }

    pub fn hash_at(&self, index: usize) -> Option<BlockHash> {
        let b = self.blocks.get(index)?;
        Some(b.hash.clone())
    }
}

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    pub latest_block: Arc<RwLock<BlockHash>>,
    pub block_filter: Filter,
    pub provider: P,
    pub pubsub_provider: P,
    pub head_buffer: Arc<RwLock<BlockBuffer>>,
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

/* Reorg from: closest_ancestor(new_head, self.head_buffer.block_heads[0]) to new_head
 *  - Updates self.head_buffer.block_heads with head_refs of new branch
 *  - Updates self.state and removes pool diffs from pruned branch
 *  - Updates self.state and applied pool diffs of new branch */
impl<N, P> StateSpaceManager<N, P> {
    pub async fn reorg(&self, new_head: BlockRef) -> Option<StateSpaceError>
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
        let (max_depth, canonical_hashes) = {
            let guard = self.head_buffer.read().await;

            let max_depth = guard.capacity as usize;

            let hashes: Vec<_> = guard.blocks.iter().map(|b| b.hash).collect();

            (max_depth, hashes)
        };

        let find_ancestor = |hash| canonical_hashes.iter().position(|&h| h == hash);

        // ---------------------------
        // Phase 1: Walk backwards via RPC (no lock)
        // ---------------------------
        let mut cursor = new_head;
        let mut depth = 0usize;
        let mut new_branch_backwards: Vec<BlockRef> = Vec::new();

        let (ancestor_idx_snapshot, ancestor_hash) = loop {
            if let Some(idx) = find_ancestor(cursor.hash) {
                info!(
                    ?cursor.hash,
                    ?depth,
                    ?idx,
                    target = "StateSpaceManager::reorg",
                    "Found common ancestor"
                );
                break (idx, cursor.hash);
            }

            if depth >= max_depth {
                warn!(
                    ?new_head.hash,
                    ?max_depth,
                    target = "StateSpaceManager::reorg",
                    "Reorg too deep"
                );
                return Some(
                    ReorgError::ReeorgTooDeep {
                        max_depth: max_depth as u64,
                    }
                    .into(),
                );
            }

            new_branch_backwards.push(BlockRef {
                hash: cursor.hash,
                parent_hash: cursor.parent_hash,
                number: cursor.number,
                block_diff: None,
            });

            let parent_hash = cursor.parent_hash;

            let parent_block = match provider
                .get_block_by_hash(parent_hash)
                .await
                .map_err(|e| ReorgError::TransportError(e))
            {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        ?parent_hash,
                        target = "StateSpaceManager::reorg",
                        "Transport error fetching parent block"
                    );
                    return Some(e.into());
                }
            };

            let parent_block = match parent_block {
                Some(b) => b,
                None => {
                    error!(
                        ?parent_hash,
                        target = "StateSpaceManager::reorg",
                        "Missing parent block during reorg"
                    );
                    return Some(ReorgError::MissingBlock { hash: parent_hash }.into());
                }
            };

            let Header { hash, inner, .. } = parent_block.header;

            cursor = BlockRef {
                hash,
                parent_hash: inner.parent_hash,
                number: inner.number,
                block_diff: None,
            };

            depth += 1;
        };

        // Convert backward branch to forward order
        let mut new_branch = new_branch_backwards;
        new_branch.reverse();

        info!(
            new_branch_len = new_branch.len(),
            ?ancestor_hash,
            target = "StateSpaceManager::reorg",
            "Computed new branch"
        );

        // ---------------------------
        // Phase 1b: Compute diffs (no lock)
        // ---------------------------
        for b in &mut new_branch {
            let diff: AMMBlockDiff = match self.extract_apply_block_diff(b.hash).await {
                Ok(d) => d,
                Err(e) => {
                    error!(
                        ?b.hash,
                        target = "StateSpaceManager::reorg",
                        "Failed to extract/apply block diff"
                    );
                    return Some(e);
                }
            };
            b.block_diff = Some(diff);
        }

        // ---------------------------
        // Phase 2: Commit (short write lock)
        // ---------------------------
        {
            let mut guard = self.head_buffer.write().await;

            let ancestor_idx_now = guard
                .blocks
                .iter()
                .position(|b| b.hash == ancestor_hash)
                .unwrap_or(ancestor_idx_snapshot);

            let split_point = ancestor_idx_now + 1;

            let mut pruned = guard.blocks.split_off(split_point);
            let pruned_len = pruned.len();

            info!(
                pruned_len,
                new_branch_len = new_branch.len(),
                target = "StateSpaceManager::reorg",
                "Applying reorg changes"
            );

            // Revert canonical blocks from tip backwards
            while let Some(b) = pruned.pop_back() {
                let diff = b.block_diff.expect("Canonical block must have diff");
                self.revert_block_diff(diff);
            }

            // Append new branch
            for b in new_branch {
                guard.blocks.push_back(b);
            }
        }

        info!(
            ?new_head.hash,
            target = "StateSpaceManager::reorg",
            "Reorg complete"
        );
        None
    }

    pub async fn revert_block_diff(&self, diff: AMMBlockDiff) -> Option<StateSpaceError> {
        let mut state_guard = self.state.write().await;
        let state = &mut state_guard.state;
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
                }
                _ => unreachable!(),
            }
        }
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
        let mut block_diff = AMMBlockDiff::new();
        let mut state_guard = self.state.write().await;
        let state = &mut state_guard.state;

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

    pub fn subscribe(
        self: Arc<Self>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<BlockRef, StateSpaceError>> + Send>>,
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

            while let Some(block) = block_stream.next().await {
                let curr_hash = self.head_buffer.hash_at(0).ok_or(StateSpaceError::MissingBlockAtIdx(0))?;
                let next_hash = block.hash();
                let next_parent_hash = block.parent_hash();
                let mut next_head = BlockRef {
                    hash: next_hash,
                    parent_hash: next_parent_hash,
                    number: block.number(),
                    block_diff: None
                };
                if next_parent_hash != curr_hash {
                    //reorg, rollback to canonical state


                    let new_head = self.reorg(next_head);
                };

                //self.head_buffer.push((block_hash, number));




                // let block_number = block.number();
                // let block_hash = block.hash();
                // let block_id = BlockId::from(block_number);
                // if block_id.is_finalized() {
                //     self.resync_from_block(block_id).await?;
                // }

                //block_filter = block_filter.at_block_hash(next_hash);
                let block_diff = self.extract_apply_block_diff(next_hash).await?;
                next_head.block_diff = Some(block_diff);
                self.head_buffer.push(next_head);
                //let logs = provider.get_logs(&block_filter).await?;

                // let affected_amms = state.write().await.sync(&logs)?;
                // let mut latest_block = self.latest_block.write().await;
                // latest_block = block_hash;

                yield Ok(next_head);
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
            head_buffer: BlockBuffer {
                blocks: VecDeque::with_capacity(64),
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

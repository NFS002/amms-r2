use crate::amms::path::{UniswapArbPathEntry, UniswapHop};
use crate::state_space::*;
use alloy::{
    network::Ethereum,
    primitives::B256,
    providers::{ProviderBuilder, RootProvider},
    transports::mock::Asserter,
};

type Manager = StateSpaceManager<Ethereum, RootProvider<Ethereum>>;
const POOL: Address = Address::repeat_byte(1);
const OTHER: Address = Address::repeat_byte(2);
const BASE: Address = Address::repeat_byte(3);
const QUOTE: Address = Address::repeat_byte(4);

fn block(number: u64, id: u8, parent: u8, diffs: AMMBlockDiff) -> BlockRef {
    BlockRef {
        number,
        timestamp: number * 12,
        base_fee_per_gas: Some(number * 100),
        gas_used: 15_000_000,
        gas_limit: 30_000_000,
        hash: B256::repeat_byte(id),
        parent_hash: B256::repeat_byte(parent),
        block_diff: Some(diffs),
    }
}

fn diff(address: Address, pre: (u128, u128), post: (u128, u128)) -> PoolDiff {
    PoolDiff {
        address,
        topic: IUniswapV2Pair::Sync::SIGNATURE_HASH,
        topic_name: IUniswapV2Pair::Sync::SIGNATURE.to_string(),
        pre: PoolReserves {
            r0: pre.0,
            r1: pre.1,
        },
        post: PoolReserves {
            r0: post.0,
            r1: post.1,
        },
    }
}

fn manager(blocks: Vec<BlockRef>, capacity: u64, reserves: (u128, u128)) -> (Manager, Asserter) {
    let rpc = Asserter::new();
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_mocked_client(rpc.clone());
    let mut state = StateSpace::default();
    state.chain_id = Some(1);
    let mut pools: Vec<UniswapV2Pool> = Vec::new();
    for (address, (r0, r1)) in [(POOL, reserves), (OTHER, (900, 800))] {
        let mut pool = UniswapV2Pool::new(address, 300);
        pool.reserve_0 = r0;
        pool.reserve_1 = r1;
        pool.count = Some(
            blocks
                .iter()
                .filter_map(|b| b.block_diff.as_ref())
                .flatten()
                .filter(|diff| diff.address == address)
                .count() as isize,
        );
        pools.push(pool.clone());
        state.pools.insert(address, AMM::UniswapV2Pool(pool));
    }
    state.arb_paths = find_arb_paths_v2(pools, WETH_ADDRESS);
    state.head_buffer = BlockBuffer {
        blocks: blocks.into(),
        capacity,
    };
    (
        StateSpaceManager {
            state: Arc::new(RwLock::new(state)),
            block_filter: Filter::new().event_signature(IUniswapV2Pair::Sync::SIGNATURE_HASH),
            provider: provider.clone(),
            pubsub_provider: provider,
            phantom: PhantomData,
        },
        rpc,
    )
}

fn parent_response(rpc: &Asserter, b: &BlockRef) {
    let mut response: Block = Block::default();
    response.header.hash = b.hash;
    response.header.inner.number = b.number;
    response.header.inner.parent_hash = b.parent_hash;
    response.header.inner.timestamp = b.timestamp;
    response.header.inner.base_fee_per_gas = b.base_fee_per_gas;
    response.header.inner.gas_used = b.gas_used;
    response.header.inner.gas_limit = b.gas_limit;
    rpc.push_success(&response);
}

fn sync_logs(b: &BlockRef, reserves: &[(u128, u128)]) -> Vec<Log> {
    // Encode real Sync ABI data so the production log decoder is exercised.
    let logs: Vec<serde_json::Value> = reserves
        .iter()
        .enumerate()
        .map(|(index, (r0, r1))| {
            serde_json::json!({
                "address": POOL,
                "topics": [IUniswapV2Pair::Sync::SIGNATURE_HASH],
                "data": format!("0x{r0:064x}{r1:064x}"),
                "blockHash": b.hash, "blockNumber": format!("0x{:x}", b.number),
                "transactionHash": B256::repeat_byte(99), "transactionIndex": "0x0",
                "logIndex": format!("0x{index:x}"), "removed": false
            })
        })
        .collect();
    serde_json::from_value(serde_json::to_value(logs).unwrap()).unwrap()
}

fn logs_response(rpc: &Asserter, b: &BlockRef, reserves: &[(u128, u128)]) {
    rpc.push_success(&sync_logs(b, reserves));
}

fn add_test_path(state: &mut StateSpace) {
    let Some(AMM::UniswapV2Pool(pool)) = state.pools.get_mut(&POOL) else {
        panic!("expected pool")
    };
    pool.token_a.address = BASE;
    pool.token_b.address = QUOTE;
    state.arb_paths.paths.push(UniswapArbPathEntry {
        path: UniswapArbPath::from_hops(vec![UniswapHop {
                pool_address: POOL,
                base: BASE,
                quote: QUOTE,
            }]),
        last_simulation: None,
    });
    state.arb_paths.paths_by_pool.insert(POOL, vec![0]);
}

fn state_snapshot(state: &StateSpace) -> serde_json::Value {
    serde_json::json!({
        "pools": state.pools,
        "head_buffer": state.head_buffer,
        "synced_at": state.synced_at,
        "arb_paths": state.arb_paths,
    })
}

async fn assert_reserves(m: &Manager, expected: (u128, u128)) {
    let state = m.state.read().await;
    for (address, reserves) in [(POOL, expected), (OTHER, (900, 800))] {
        let AMM::UniswapV2Pool(pool) = &state.pools[&address] else {
            panic!("wrong pool variant")
        };
        assert_eq!((pool.reserve_0, pool.reserve_1), reserves);
    }
}

async fn assert_chain(m: &Manager, expected: &[u8]) {
    let state = m.state.read().await;
    let buffer = &state.head_buffer;
    assert_eq!(
        buffer.blocks.iter().map(|b| b.hash).collect::<Vec<_>>(),
        expected
            .iter()
            .map(|id| B256::repeat_byte(*id))
            .collect::<Vec<_>>()
    );
    assert!(buffer.blocks.len() <= buffer.capacity as usize);
    for (a, b) in buffer.blocks.iter().zip(buffer.blocks.iter().skip(1)) {
        assert_eq!(b.parent_hash, a.hash);
        assert_eq!(b.number, a.number + 1);
    }
}

#[tokio::test]
async fn empty_replacement_reverts_multiple_blocks_and_events_in_reverse_order() {
    // Reorg should roll back state to the {ancestor} block,
    // and then apply the (empty) {replacement} block
    let ancestor = block(10, 10, 9, vec![]);
    let old1 = block(
        11,
        11,
        10,
        vec![
            diff(POOL, (100, 200), (110, 190)),
            diff(POOL, (110, 190), (120, 180)),
        ],
    );
    let old2 = block(12, 12, 11, vec![diff(POOL, (120, 180), (130, 170))]);
    let replacement = block(11, 21, 10, vec![]);
    let (m, rpc) = manager(vec![ancestor.clone(), old1, old2], 4, (130, 170));
    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &replacement, &[]);
    let head = m.reorg(replacement).await.unwrap();
    assert_eq!(head.hash, B256::repeat_byte(21));
    assert!(head.block_diff.unwrap().is_empty());
    assert_reserves(&m, (100, 200)).await;
    assert_chain(&m, &[10, 21]).await;
    assert!(rpc.read_q().is_empty());
}

#[tokio::test]
async fn replacement_replays_oldest_first_and_returns_newest_head_with_diff() {
    let ancestor = block(10, 10, 9, vec![]);
    let old = block(11, 11, 10, vec![diff(POOL, (100, 200), (150, 150))]);
    let new1 = block(11, 21, 10, vec![]);
    let new2 = block(12, 22, 21, vec![]);
    let (m, rpc) = manager(vec![ancestor.clone(), old], 4, (150, 150));
    parent_response(&rpc, &new1);
    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &new1, &[(105, 195), (110, 190)]);
    logs_response(&rpc, &new2, &[(120, 180)]);
    let head = m.reorg(new2).await.unwrap();
    assert_eq!((head.number, head.hash), (12, B256::repeat_byte(22)));
    let diffs = head.block_diff.unwrap();
    assert_eq!(diffs.len(), 1);
    assert_eq!((diffs[0].pre.r0, diffs[0].pre.r1), (110, 190));
    assert_eq!((diffs[0].post.r0, diffs[0].post.r1), (120, 180));
    let state = m.state.read().await;
    let buffer = &state.head_buffer;
    let first = buffer.blocks[1].block_diff.as_ref().unwrap();
    assert_eq!((first[0].pre.r0, first[0].pre.r1), (100, 200));
    assert_eq!((first[1].pre.r0, first[1].pre.r1), (105, 195));
    drop(state);
    assert_reserves(&m, (120, 180)).await;
    assert_chain(&m, &[10, 21, 22]).await;
    assert!(rpc.read_q().is_empty());
}

#[tokio::test]
async fn missed_blocks_are_backfilled_without_rollback_or_capacity_growth() {
    let ancestor = block(10, 10, 9, vec![diff(POOL, (90, 210), (100, 200))]);
    let new1 = block(11, 21, 10, vec![]);
    let new2 = block(12, 22, 21, vec![]);
    let (m, rpc) = manager(
        vec![
            block(8, 8, 7, vec![]),
            block(9, 9, 8, vec![]),
            ancestor.clone(),
        ],
        3,
        (100, 200),
    );
    parent_response(&rpc, &new1);
    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &new1, &[(110, 190)]);
    logs_response(&rpc, &new2, &[]);
    let head = m.reorg(new2).await.unwrap();
    assert_eq!(head.number, 12);
    assert!(head.block_diff.unwrap().is_empty());
    assert_chain(&m, &[10, 21, 22]).await;
    let state = m.state.read().await;
    let buffer = &state.head_buffer;
    assert_eq!(buffer.blocks[1].block_diff.as_ref().unwrap()[0].pre.r0, 100);
    drop(state);
    assert_reserves(&m, (110, 190)).await;
    assert!(rpc.read_q().is_empty());
}

#[tokio::test]
async fn empty_to_empty_replacement_preserves_reserves() {
    let ancestor = block(10, 10, 9, vec![]);
    let new = block(11, 21, 10, vec![]);
    let (m, rpc) = manager(
        vec![ancestor.clone(), block(11, 11, 10, vec![])],
        3,
        (100, 200),
    );
    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &new, &[]);
    m.reorg(new).await.unwrap();
    assert_reserves(&m, (100, 200)).await;
    assert_chain(&m, &[10, 21]).await;
}

#[tokio::test]
async fn missing_parent_leaves_state_and_buffer_untouched() {
    let (m, rpc) = manager(vec![block(10, 10, 9, vec![])], 3, (100, 200));
    rpc.push_success(&Option::<Block>::None);
    let err = m.reorg(block(12, 22, 21, vec![])).await.unwrap_err();
    assert!(
        matches!(err, StateSpaceError::ReOrgError(ReorgError::MissingBlock { hash }) if hash == B256::repeat_byte(21))
    );
    assert_reserves(&m, (100, 200)).await;
    assert_chain(&m, &[10]).await;
}

#[tokio::test]
async fn parent_rpc_failure_leaves_state_and_buffer_untouched() {
    let (m, rpc) = manager(vec![block(10, 10, 9, vec![])], 3, (100, 200));
    rpc.push_failure_msg("mock parent fetch failure");
    let err = m.reorg(block(12, 22, 21, vec![])).await.unwrap_err();
    assert!(matches!(
        err,
        StateSpaceError::ReOrgError(ReorgError::TransportError(_))
    ));
    assert_reserves(&m, (100, 200)).await;
    assert_chain(&m, &[10]).await;
}

#[tokio::test]
async fn too_deep_reorg_leaves_state_and_buffer_untouched() {
    let (m, rpc) = manager(vec![block(10, 10, 9, vec![])], 2, (100, 200));
    parent_response(&rpc, &block(12, 22, 21, vec![]));
    parent_response(&rpc, &block(11, 21, 20, vec![]));
    let err = m.reorg(block(13, 23, 22, vec![])).await.unwrap_err();
    assert!(matches!(
        err,
        StateSpaceError::ReOrgError(ReorgError::ReeorgTooDeep { max_depth: 2 })
    ));
    assert_reserves(&m, (100, 200)).await;
    assert_chain(&m, &[10]).await;
    assert!(rpc.read_q().is_empty());
}

// Both RPC and decoding failures must preserve the complete pre-reorg state,
// including diffs needed to retry the same reorg later.
#[tokio::test]
async fn failed_replacement_preserves_state_and_can_be_retried() {
    for malformed_logs in [false, true] {
        let ancestor = block(10, 10, 9, vec![]);
        let old = block(11, 11, 10, vec![diff(POOL, (100, 200), (150, 150))]);
        let new1 = block(11, 21, 10, vec![]);
        let new2 = block(12, 22, 21, vec![]);
        let (m, rpc) = manager(vec![ancestor.clone(), old], 3, (150, 150));
        add_test_path(&mut *m.state.write().await);
        assert_eq!(m.simulate_all_paths().await.unwrap().results.len(), 1);
        let before = state_snapshot(&*m.state.read().await);
        parent_response(&rpc, &new1);
        parent_response(&rpc, &ancestor);
        logs_response(&rpc, &new1, &[(110, 190)]);
        if malformed_logs {
            // A valid event is followed by a malformed Sync in the second block.
            logs_response(&rpc, &new2, &[(120, 180)]);
            let mut response = rpc.write_q();
            let last = response.pop_back().unwrap();
            drop(response);
            let alloy::transports::mock::MockResponse::Success(raw) = last else {
                panic!("expected logs")
            };
            let mut logs: Vec<serde_json::Value> = serde_json::from_str(raw.get()).unwrap();
            let mut invalid = logs[0].clone();
            invalid["data"] = serde_json::json!("0x");
            logs.push(invalid);
            rpc.push_success(&logs);
        } else {
            rpc.push_failure_msg("second replacement block unavailable");
        }
        let error = m.reorg(new2.clone()).await.unwrap_err();
        if malformed_logs {
            assert!(matches!(error, StateSpaceError::AlloyError(_)));
        } else {
            assert!(matches!(error, StateSpaceError::TransportError(_)));
        }
        assert_eq!(state_snapshot(&*m.state.read().await), before);
        assert!(rpc.read_q().is_empty());

        parent_response(&rpc, &new1);
        parent_response(&rpc, &ancestor);
        logs_response(&rpc, &new1, &[(110, 190)]);
        logs_response(&rpc, &new2, &[(120, 180)]);
        let head = m.reorg(new2).await.unwrap();
        assert_eq!(head.hash, B256::repeat_byte(22));
        assert_reserves(&m, (120, 180)).await;
        assert_chain(&m, &[10, 21, 22]).await;
        let state = m.state.read().await;
        let AMM::UniswapV2Pool(pool) = &state.pools[&POOL] else {
            panic!("expected pool")
        };
        assert_eq!(pool.count, Some(2));
        assert!(state.arb_paths.paths[0].last_simulation.is_none());
        assert!(rpc.read_q().is_empty());
    }
}

#[tokio::test]
async fn repeated_simulation_uses_reorg_reserves_and_the_committed_head() {
    let ancestor = block(10, 10, 9, vec![]);
    let old = block(11, 11, 10, vec![diff(POOL, (100, 200), (150, 150))]);
    let replacement = block(11, 21, 10, vec![]);
    let (m, rpc) = manager(vec![ancestor.clone(), old], 3, (150, 150));
    // Shared ownership must still allow both the initial and later simulations.
    let m = Arc::new(m);
    add_test_path(&mut *m.state.write().await);
    let before_batch = m.simulate_all_paths().await.unwrap();
    let before = &before_batch.results[0];
    assert_eq!(
        (
            before_batch.context.block_number,
            before_batch.context.block_hash
        ),
        (11, B256::repeat_byte(11))
    );

    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &replacement, &[(110, 190)]);
    m.reorg(replacement).await.unwrap();
    {
        let state = m.state.read().await;
        assert!(state.arb_paths.paths[0].last_simulation.is_none());
        assert_eq!(state.clone_head().unwrap().hash, B256::repeat_byte(21));
        let AMM::UniswapV2Pool(pool) = &state.pools[&POOL] else {
            panic!("expected pool")
        };
        assert_eq!((pool.reserve_0, pool.reserve_1), (110, 190));
    }

    let after_batch = m.simulate_all_paths().await.unwrap();
    let after = &after_batch.results[0];
    assert_eq!(
        (
            after_batch.context.block_number,
            after_batch.context.block_hash
        ),
        (11, B256::repeat_byte(21))
    );
    assert_ne!(after.amount_out, before.amount_out);
    assert_eq!(after_batch.context.base_fee_per_gas, Some(1100));
    assert_eq!(after_batch.context.block_timestamp, 132);
    assert_eq!(
        before.path_snapshot.simulate(before.amount_in).unwrap(),
        before.amount_out
    );
    assert_eq!(
        after.path_snapshot.simulate(after.amount_in).unwrap(),
        after.amount_out
    );
    let state = m.state.read().await;
    let entry = &state.arb_paths.paths[0];
    let view = entry.path.resolve(&state).unwrap();
    assert_eq!(view.simulate(after.amount_in).unwrap(), after.amount_out);
    assert_eq!(
        entry.last_simulation.as_ref().unwrap().context.block_hash,
        after_batch.context.block_hash
    );
    assert!(rpc.read_q().is_empty());
}

#[tokio::test]
async fn block_commit_updates_pools_and_head_and_invalidates_simulations() {
    let (m, _) = manager(vec![], 3, (100, 200));
    let mut state = m.state.write().await;
    state.synced_at = Some(block(10, 10, 9, vec![]));
    add_test_path(&mut state);
    let amount_in = U256::from(10);
    let before_batch = state.simulate_all_paths(amount_in).unwrap();
    let before = &before_batch.results[0];
    assert_eq!(before_batch.context.block_hash, B256::repeat_byte(10));

    let next = block(11, 11, 10, vec![]);
    let logs = sync_logs(&next, &[(110, 190), (120, 180)]);
    let committed = state.apply_block(next, logs).unwrap();
    assert_eq!(state.clone_head().unwrap().hash, committed.hash);
    assert_eq!(committed.block_diff.as_ref().unwrap().len(), 2);
    assert!(state.arb_paths.paths[0].last_simulation.is_none());
    let AMM::UniswapV2Pool(pool) = &state.pools[&POOL] else {
        panic!("expected pool")
    };
    assert_eq!((pool.reserve_0, pool.reserve_1), (120, 180));
    let after_batch = state.simulate_all_paths(amount_in).unwrap();
    let after = &after_batch.results[0];
    assert_eq!(
        (
            after_batch.context.block_number,
            after_batch.context.block_hash
        ),
        (11, committed.hash)
    );
    assert_ne!(before.amount_out, after.amount_out);
}

#[tokio::test]
async fn failed_block_commit_preserves_pools_head_and_cached_simulations() {
    for malformed_log in [false, true] {
        let (m, _) = manager(vec![block(10, 10, 9, vec![])], 3, (100, 200));
        let mut state = m.state.write().await;
        add_test_path(&mut state);
        assert_eq!(
            state
                .simulate_all_paths(U256::from(10))
                .unwrap()
                .results
                .len(),
            1
        );
        let before = state_snapshot(&state);
        let next = block(11, 11, if malformed_log { 10 } else { 99 }, vec![]);
        let mut logs = sync_logs(&next, &[(110, 190)]);
        if malformed_log {
            // The first event is valid: a later decode error must not publish it.
            let mut invalid = serde_json::to_value(&logs[0]).unwrap();
            invalid["data"] = serde_json::json!("0x");
            logs.push(serde_json::from_value(invalid).unwrap());
        }
        assert!(state.apply_block(next, logs).is_err());
        assert_eq!(state_snapshot(&state), before);
    }
}

#[tokio::test]
async fn failed_simulation_clears_the_previous_result() {
    let (m, _) = manager(vec![block(10, 10, 9, vec![])], 3, (100, 200));
    let mut state = m.state.write().await;
    add_test_path(&mut state);
    assert_eq!(
        state
            .simulate_all_paths(U256::from(10))
            .unwrap()
            .results
            .len(),
        1
    );
    state.pools.remove(&POOL);
    assert!(state
        .simulate_all_paths(U256::from(10))
        .unwrap()
        .results
        .is_empty());
    assert!(state.arb_paths.paths[0].last_simulation.is_none());
}

#[tokio::test]
async fn simulation_without_a_head_returns_an_error_without_mutating_state() {
    let (m, _) = manager(vec![], 3, (100, 200));
    let mut state = m.state.write().await;
    add_test_path(&mut state);
    let before = state_snapshot(&state);
    let error = state.simulate_all_paths(U256::from(10)).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(state_snapshot(&state), before);
}

#[tokio::test]
async fn affected_simulations_preserve_path_ids_and_capture_snapshots() {
    let head = block(11, 11, 10, vec![diff(POOL, (100, 200), (150, 150))]);
    let (m, _) = manager(vec![head], 3, (150, 150));
    let mut state = m.state.write().await;
    add_test_path(&mut state);
    let entry = state.arb_paths.paths[0].clone();
    state.arb_paths.paths.push(entry);
    state.arb_paths.paths_by_pool.insert(POOL, vec![1, 1]);
    let amount_in = U256::from(10);
    let batch = state.simulate_affected_paths(amount_in).unwrap();
    let results = &batch.results;
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(
        result.path_snapshot.id(),
        state.arb_paths.paths[1].path.id()
    );
    assert_eq!(result.amount_in, amount_in);
    assert_eq!(
        result.path_snapshot.simulate(amount_in).unwrap(),
        result.amount_out
    );
    assert!(state.arb_paths.paths[0].last_simulation.is_none());
    assert_eq!(
        state.arb_paths.paths[1]
            .last_simulation
            .as_ref()
            .unwrap()
            .result
            .path_snapshot
            .id(),
        state.arb_paths.paths[1].path.id()
    );
}

#[tokio::test]
async fn simulation_batch_and_cache_share_committed_block_context() {
    let head = block(12, 12, 11, vec![]);
    let (m, _) = manager(vec![head.clone()], 3, (100, 200));
    let mut state = m.state.write().await;
    add_test_path(&mut state);
    let started = Utc::now();
    let batch = state.simulate_all_paths(U256::from(10)).unwrap();
    let context = &batch.context;
    assert_eq!(context.chain_id, 1);
    assert_eq!(context.block_number, head.number);
    assert_eq!(context.block_hash, head.hash);
    assert_eq!(context.parent_hash, head.parent_hash);
    assert_eq!(context.block_timestamp, head.timestamp);
    assert_eq!(context.base_fee_per_gas, head.base_fee_per_gas);
    assert_eq!(context.gas_used, head.gas_used);
    assert_eq!(context.gas_limit, head.gas_limit);
    assert!(context.simulated_at >= started && context.simulated_at <= Utc::now());
    let cached = state.arb_paths.paths[0].last_simulation.as_ref().unwrap();
    assert!(Arc::ptr_eq(context, &cached.context));
    let result = &batch.results[0];
    assert_eq!(
        result.pct_gain_bp,
        percentage_change_bp(result.amount_in, result.amount_out).unwrap()
    );
    let json = serde_json::to_string(&batch).unwrap();
    let restored: SimulationBatch = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.context.block_hash, context.block_hash);
    assert_eq!(restored.results[0].pct_gain_bp, result.pct_gain_bp);
    assert!(!json.contains("spread_pct"));
}

#[test]
fn block_header_conversion_and_shallow_clone_preserve_execution_context() {
    let mut header: Header = Header::default();
    header.inner.timestamp = 1234;
    header.inner.base_fee_per_gas = Some(5678);
    header.inner.gas_used = 123;
    header.inner.gas_limit = 456;
    let head = BlockRef::from(header).shallow_clone();
    assert_eq!(head.timestamp, 1234);
    assert_eq!(head.base_fee_per_gas, Some(5678));
    assert_eq!((head.gas_used, head.gas_limit), (123, 456));
}

#[tokio::test]
async fn empty_simulation_batch_still_contains_head_context() {
    let (m, _) = manager(vec![block(12, 12, 11, vec![])], 3, (100, 200));
    let batch = m.simulate_all_paths().await.unwrap();
    assert!(batch.results.is_empty());
    assert_eq!(batch.context.block_number, 12);
}

#[tokio::test]
async fn subscription_processing_returns_owned_batches_and_skips_duplicates() {
    let (m, rpc) = manager(vec![block(10, 10, 9, vec![])], 3, (100, 200));
    add_test_path(&mut *m.state.write().await);
    let next = block(11, 11, 10, vec![]);
    logs_response(&rpc, &next, &[(110, 190)]);
    let batch = m.process_next_block(next.clone()).await.unwrap().unwrap();
    assert_eq!(batch.context.block_hash, next.hash);
    assert_eq!(batch.results.len(), 1);
    assert!(
        m.state.try_write().is_ok(),
        "batch must not retain a state guard"
    );
    assert!(m.process_next_block(next).await.unwrap().is_none());
    let empty = block(12, 12, 11, vec![]);
    logs_response(&rpc, &empty, &[]);
    let batch = m.process_next_block(empty).await.unwrap().unwrap();
    assert!(batch.results.is_empty());
    assert_eq!(batch.context.block_number, 12);
    assert!(rpc.read_q().is_empty());
}

#[tokio::test]
async fn subscription_reorg_simulates_paths_changed_only_by_rollback() {
    let ancestor = block(10, 10, 9, vec![]);
    let old = block(11, 11, 10, vec![diff(POOL, (100, 200), (150, 150))]);
    let replacement = block(11, 21, 10, vec![]);
    let (m, rpc) = manager(vec![ancestor.clone(), old], 3, (150, 150));
    add_test_path(&mut *m.state.write().await);
    parent_response(&rpc, &ancestor);
    logs_response(&rpc, &replacement, &[]);
    let batch = m
        .process_next_block(replacement.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.context.block_hash, replacement.hash);
    assert_eq!(batch.results.len(), 1);
    assert!(m.state.try_write().is_ok());
    assert!(rpc.read_q().is_empty());
}

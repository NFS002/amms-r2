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

fn block(number: u64, id: u8, parent: u8, diffs: AMMBlockDiff) -> BlockRef {
    BlockRef {
        number,
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
    for (address, (r0, r1)) in [(POOL, reserves), (OTHER, (900, 800))] {
        let mut pool = UniswapV2Pool::new(address, 300);
        pool.reserve_0 = r0;
        pool.reserve_1 = r1;
        state.state.insert(address, AMM::UniswapV2Pool(pool));
    }
    (
        StateSpaceManager {
            state: Arc::new(RwLock::new(state)),
            block_filter: Filter::new().event_signature(IUniswapV2Pair::Sync::SIGNATURE_HASH),
            provider: provider.clone(),
            pubsub_provider: provider,
            head_buffer: Arc::new(RwLock::new(BlockBuffer {
                blocks: blocks.into(),
                capacity,
            })),
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
    rpc.push_success(&response);
}

fn logs_response(rpc: &Asserter, b: &BlockRef, reserves: &[(u128, u128)]) {
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
    rpc.push_success(&logs);
}

async fn assert_reserves(m: &Manager, expected: (u128, u128)) {
    let state = m.state.read().await;
    for (address, reserves) in [(POOL, expected), (OTHER, (900, 800))] {
        let AMM::UniswapV2Pool(pool) = &state.state[&address] else {
            panic!("wrong pool variant")
        };
        assert_eq!((pool.reserve_0, pool.reserve_1), reserves);
    }
}

async fn assert_chain(m: &Manager, expected: &[u8]) {
    let buffer = m.head_buffer.read().await;
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
    let buffer = m.head_buffer.read().await;
    let first = buffer.blocks[1].block_diff.as_ref().unwrap();
    assert_eq!((first[0].pre.r0, first[0].pre.r1), (100, 200));
    assert_eq!((first[1].pre.r0, first[1].pre.r1), (105, 195));
    drop(buffer);
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
    let buffer = m.head_buffer.read().await;
    assert_eq!(buffer.blocks[1].block_diff.as_ref().unwrap()[0].pre.r0, 100);
    drop(buffer);
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

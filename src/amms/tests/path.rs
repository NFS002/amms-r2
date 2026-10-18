use super::*;
use crate::amms::balancer::BalancerPool;
use std::collections::HashSet;

type Route = Vec<(Address, Address, Address)>;

fn pool(id: u8, a: u8, b: u8) -> UniswapV2Pool {
    let mut pool = UniswapV2Pool::new(Address::repeat_byte(id), 300);
    pool.token_a.address = Address::repeat_byte(a);
    pool.token_b.address = Address::repeat_byte(b);
    pool
}

// Independent exhaustive oracle for small fixtures; no production indexes used.
fn exhaustive(pools: &[UniswapV2Pool], start: Address) -> HashSet<Route> {
    let mut routes = HashSet::new();
    for p in pools {
        for q in pools {
            for r in pools {
                if p.address == q.address || p.address == r.address || q.address == r.address {
                    continue;
                }
                let mut token = start;
                let mut route = Vec::new();
                for pool in [p, q, r] {
                    let output = if pool.token_a.address == token {
                        pool.token_b.address
                    } else if pool.token_b.address == token {
                        pool.token_a.address
                    } else {
                        break;
                    };
                    route.push((pool.address, token, output));
                    token = output;
                }
                if route.len() == 3 && token == start {
                    routes.insert(route);
                }
            }
        }
    }
    routes
}

fn check(pools: Vec<UniswapV2Pool>, start: Address, expected_count: usize) {
    let expected = exhaustive(&pools, start);
    let result = find_arb_paths_v2(pools, start);
    let mut routes = HashSet::new();
    let mut index: AddressMap<Vec<PathIndex>> = AddressMap::default();
    for (id, entry) in result.paths.iter().enumerate() {
        assert!(entry.last_simulation.is_none());
        let route: Route = entry
            .path
            .hops
            .iter()
            .map(|h| (h.pool_address, h.base, h.quote))
            .collect();
        assert_eq!(route.len(), 3);
        assert_eq!(route[0].1, start);
        assert_eq!(route[2].2, start);
        for i in 0..3 {
            assert_eq!(route[i].2, route[(i + 1) % 3].1);
        }
        assert_eq!(route.iter().map(|h| h.0).collect::<HashSet<_>>().len(), 3);
        for &(address, _, _) in &route {
            index.entry(address).or_default().push(id);
        }
        assert!(routes.insert(route), "duplicate route");
    }
    assert_eq!(routes, expected);
    assert_eq!(routes.len(), expected_count);
    assert_eq!(result.paths_by_pool, index);
    assert_eq!(result.path_indices_by_id.len(), result.paths.len());
    for entry in &result.paths {
        assert!(std::ptr::eq(
            result.get_by_id(&entry.path.id()).unwrap(),
            entry
        ));
    }
    for route in &routes {
        let reverse: Route = route.iter().rev().map(|&(p, a, b)| (p, b, a)).collect();
        assert!(routes.contains(&reverse));
    }
    let restored: UniswapArbPaths =
        serde_json::from_value(serde_json::to_value(&result).unwrap()).unwrap();
    assert_eq!(restored.paths_by_pool, result.paths_by_pool);
    assert_eq!(restored.path_indices_by_id, result.path_indices_by_id);
    assert_eq!(restored.paths.len(), result.paths.len());
}

#[test]
fn both_directions_for_every_token_ordering() {
    for mask in 0..8 {
        let mut pools = vec![pool(10, 1, 2), pool(11, 2, 3), pool(12, 3, 1)];
        for (i, pool) in pools.iter_mut().enumerate() {
            if mask & (1 << i) != 0 {
                std::mem::swap(&mut pool.token_a, &mut pool.token_b);
            }
        }
        check(pools, Address::repeat_byte(1), 2);
    }
}

#[test]
fn parallel_pools_duplicates_and_disconnected_edges() {
    let mut pools = vec![
        pool(10, 1, 2),
        pool(11, 2, 3),
        pool(12, 3, 1),
        pool(13, 1, 3),
        pool(14, 4, 5),
    ];
    pools.push(pools[0].clone());
    check(pools, Address::repeat_byte(1), 4);
}

#[test]
fn no_cycle_or_missing_start() {
    check(vec![], Address::repeat_byte(1), 0);
    check(
        vec![pool(10, 1, 2), pool(11, 2, 3)],
        Address::repeat_byte(1),
        0,
    );
    check(
        vec![pool(10, 1, 2), pool(11, 2, 3), pool(12, 3, 1)],
        Address::repeat_byte(9),
        0,
    );
}

#[test]
fn saved_snapshot_triangles() {
    // Pool topology from the 2026-09-20 snapshot. Reserves do not affect discovery.
    let topology: Vec<[Address; 3]> =
        serde_json::from_str(include_str!("fixtures/path_topology.json")).unwrap();
    let pools = topology
        .into_iter()
        .map(|[address, a, b]| {
            let mut pool = UniswapV2Pool::new(address, 300);
            pool.token_a.address = a;
            pool.token_b.address = b;
            pool
        })
        .collect();
    check(
        pools,
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            .parse()
            .unwrap(),
        32,
    );
}

// Optional full snapshot verification without committing a large reserve snapshot.
#[test]
#[ignore = "requires AMMS_PATH_SNAPSHOT pointing to a serialized state snapshot"]
fn full_snapshot() {
    let filename = std::env::var("AMMS_PATH_SNAPSHOT").unwrap();
    #[derive(Deserialize)]
    struct Snapshot {
        amms: Vec<SnapshotPool>,
    }
    #[derive(Deserialize)]
    enum SnapshotPool {
        UniswapV2Pool(UniswapV2Pool),
    }
    // Deserialize directly so u128 reserves never pass through a JSON f64 Value.
    let snapshot: Snapshot =
        serde_json::from_str(&std::fs::read_to_string(filename).unwrap()).unwrap();
    let pools = snapshot
        .amms
        .into_iter()
        .map(|pool| match pool {
            SnapshotPool::UniswapV2Pool(pool) => pool,
        })
        .collect();
    let result = find_arb_paths_v2(
        pools,
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            .parse()
            .unwrap(),
    );
    assert_eq!(result.paths.len(), 32);
    let mut expected: AddressMap<Vec<PathIndex>> = AddressMap::default();
    for (id, entry) in result.paths.iter().enumerate() {
        for hop in &entry.path.hops {
            expected.entry(hop.pool_address).or_default().push(id);
        }
    }
    assert_eq!(result.paths_by_pool, expected);
}

fn live_path_fixture() -> (UniswapArbPath, StateSpace) {
    let mut pool = pool(10, 1, 2);
    pool.token_a.decimals = 18;
    pool.token_b.decimals = 6;
    pool.reserve_0 = 1_000_000;
    pool.reserve_1 = 2_000_000;
    let path = UniswapArbPath {
        hops: vec![UniswapHop {
            pool_address: pool.address,
            base: pool.token_a.address,
            quote: pool.token_b.address,
        }],
    };
    let mut state = StateSpace::default();
    state.pools.insert(pool.address, AMM::UniswapV2Pool(pool));
    (path, state)
}

#[test]
fn resolved_path_borrows_live_pools_and_observes_updated_reserves() {
    let (path, mut state) = live_path_fixture();
    let address = path.hops[0].pool_address;
    {
        let view = path.resolve(&state).unwrap();
        let ResolvedUniswapPool::V2(borrowed) = view.hops[0].pool else {
            panic!("expected a v2 pool");
        };
        let AMM::UniswapV2Pool(stored) = state.pools.get(&address).unwrap() else {
            panic!("expected a v2 pool");
        };
        assert!(std::ptr::eq(borrowed, stored));
        assert_eq!(
            view.simulate(U256::from(10_000)).unwrap(),
            U256::from(19_743)
        );
    }

    let AMM::UniswapV2Pool(pool) = state.pools.get_mut(&address).unwrap() else {
        panic!("expected a v2 pool");
    };
    pool.reserve_1 = 4_000_000;

    let view = path.resolve(&state).unwrap();
    assert_eq!(
        view.simulate(U256::from(10_000)).unwrap(),
        U256::from(39_486)
    );
    assert!(view.to_string().contains("Token (b) reserves: 4000000"));
}

#[test]
fn path_resolves_replaced_state_map_without_rebuilding_topology() {
    let (path, mut state) = live_path_fixture();
    let path_id = path.id();
    let initial_amount = path
        .resolve(&state)
        .unwrap()
        .simulate(U256::from(10_000))
        .unwrap();

    let mut replacement = pool(10, 1, 2);
    replacement.reserve_0 = 2_000_000;
    replacement.reserve_1 = 1_000_000;
    state.pools = HashMap::from([(replacement.address, AMM::UniswapV2Pool(replacement))]);

    let view = path.resolve(&state).unwrap();
    assert_eq!(
        view.simulate(U256::from(10_000)).unwrap(),
        U256::from(4_960)
    );
    assert_ne!(view.simulate(U256::from(10_000)).unwrap(), initial_amount);
    assert_eq!(path.id(), path_id);
}

#[test]
fn resolve_reports_missing_and_unsupported_pools() {
    let (path, mut state) = live_path_fixture();
    let address = path.hops[0].pool_address;
    state.pools.remove(&address);
    assert!(matches!(
        path.resolve(&state),
        Err(AMMError::PoolNotFound(missing)) if missing == address
    ));

    state
        .pools
        .insert(address, AMM::BalancerPool(BalancerPool::default()));
    assert!(matches!(
        path.resolve(&state),
        Err(AMMError::UnsupportedUniswapPool(unsupported)) if unsupported == address
    ));
}

#[test]
fn resolved_display_and_debug_include_current_pool_details() {
    let (path, state) = live_path_fixture();
    let view = path.resolve(&state).unwrap();
    let text = view.to_string();

    for expected in [
        "Hop 1/1".to_owned(),
        format!("Pool: {}", path.hops[0].pool_address),
        "Fee: 300".to_owned(),
        format!("Token (a): {}", path.hops[0].base),
        "Token (a) decimals: 18".to_owned(),
        "Token (a) reserves: 1000000".to_owned(),
        format!("Token (b): {}", path.hops[0].quote),
        "Token (b) decimals: 6".to_owned(),
        "Token (b) reserves: 2000000".to_owned(),
    ] {
        assert!(text.contains(&expected), "missing {expected:?} in {text:?}");
    }
    assert_eq!(format!("{view:?}"), text);
    assert_eq!(format!("{view:#?}"), text);
    assert!(!text.contains("not implemented"));
}

#[test]
fn resolved_v3_display_reports_liquidity_instead_of_invented_reserves() {
    let (path, mut state) = live_path_fixture();
    let mut pool = UniswapV3Pool::default();
    pool.address = path.hops[0].pool_address;
    pool.token_a.address = path.hops[0].base;
    pool.token_b.address = path.hops[0].quote;
    pool.fee = 3_000;
    pool.liquidity = 123_456;
    pool.sqrt_price = U256::from(1) << 96;
    pool.tick = 7;
    state.pools.insert(pool.address, AMM::UniswapV3Pool(pool));

    let text = path.resolve(&state).unwrap().to_string();
    assert!(text.contains("Fee: 3000"));
    assert!(text.contains("Liquidity: 123456"));
    assert!(text.contains("Sqrt price X96: 79228162514264337593543950336"));
    assert!(text.contains("Tick: 7"));
    assert!(!text.contains("reserves:"));
}

#[test]
fn persistent_hops_serialize_addresses_without_pool_state() {
    let (path, _) = live_path_fixture();
    let encoded = serde_json::to_value(&path).unwrap();
    let hop = &encoded["hops"][0];
    assert_eq!(
        hop["pool_address"],
        serde_json::to_value(path.hops[0].pool_address).unwrap()
    );
    assert!(hop.get("pool").is_none());
    let restored: UniswapArbPath = serde_json::from_value(encoded).unwrap();
    assert_eq!(restored.id(), path.id());
    assert_eq!(restored.hops[0].base, path.hops[0].base);
    assert_eq!(restored.hops[0].quote, path.hops[0].quote);
}

#[test]
fn hop_deserialization_rejects_missing_pool_identity() {
    let encoded = serde_json::json!({
        "base": Address::repeat_byte(1),
        "quote": Address::repeat_byte(2),
    });
    let error = serde_json::from_value::<UniswapHop>(encoded).unwrap_err();
    assert!(error.to_string().contains("pool_address"));
}

#[test]
fn snapshot_preserves_simulation_and_display_after_live_state_changes() {
    let (path, mut state) = live_path_fixture();
    let amount_in = U256::from(10_000);
    let view = path.resolve(&state).unwrap();
    let expected = view.simulate(amount_in).unwrap();
    let expected_display = view.to_string();
    let snapshot = view.snapshot();
    assert_eq!(snapshot.id(), path.id());
    let AMM::UniswapV2Pool(pool) = state.pools.get_mut(&path.hops[0].pool_address).unwrap() else {
        panic!("expected v2 pool");
    };
    pool.reserve_1 *= 2;
    assert_ne!(
        path.resolve(&state).unwrap().simulate(amount_in).unwrap(),
        expected
    );
    drop(state);
    assert_eq!(snapshot.simulate(amount_in).unwrap(), expected);
    assert_eq!(snapshot.to_string(), expected_display);
    let encoded = serde_json::to_string(&snapshot).unwrap();
    let restored: UniswapArbPathSnapshot = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored.simulate(amount_in).unwrap(), expected);
    assert_eq!(restored.to_string(), expected_display);
}

#[test]
fn snapshot_owns_v3_pool_state() {
    let (path, mut state) = live_path_fixture();
    let mut pool = UniswapV3Pool::default();
    pool.address = path.hops[0].pool_address;
    pool.liquidity = 123_456;
    pool.sqrt_price = U256::from(1) << 96;
    pool.tick = 7;
    state.pools.insert(pool.address, AMM::UniswapV3Pool(pool));
    let snapshot = path.resolve(&state).unwrap().snapshot();
    state.pools.clear();
    let UniswapPool::V3(pool) = &snapshot.hops[0].pool else {
        panic!("expected v3 pool");
    };
    assert_eq!(pool.liquidity, 123_456);
    assert_eq!(pool.tick, 7);
    assert!(snapshot.to_string().contains("Liquidity: 123456"));
}

#[test]
fn route_identity_includes_pool_direction_and_hop_order() {
    let (path, _) = live_path_fixture();
    let original_id = path.id();
    assert_eq!(path.clone().id(), original_id);
    let mut changed = path.clone();
    changed.hops[0].base = path.hops[0].quote;
    changed.hops[0].quote = path.hops[0].base;
    assert_ne!(changed.id(), original_id);
    changed = path.clone();
    changed.hops[0].pool_address = Address::repeat_byte(99);
    assert_ne!(changed.id(), original_id);
    changed.hops.push(path.hops[0].clone());
    let ordered_id = changed.id();
    changed.hops.reverse();
    assert_ne!(changed.id(), ordered_id);
}

#[test]
fn rebuilding_indices_updates_lookup_after_reordering_and_removal() {
    let (path, _) = live_path_fixture();
    let mut other = path.clone();
    other.hops[0].pool_address = Address::repeat_byte(99);
    let first_id = path.id();
    let second_id = other.id();
    let mut paths = UniswapArbPaths::default();
    paths.paths = vec![path, other]
        .into_iter()
        .map(|path| UniswapArbPathEntry {
            path,
            last_simulation: None,
        })
        .collect();
    paths.rebuild_indices();
    assert_eq!(paths.path_indices_by_id[&first_id], 0);
    paths.paths.reverse();
    paths.rebuild_indices();
    assert_eq!(paths.path_indices_by_id[&first_id], 1);
    assert_eq!(paths.get_by_id(&second_id).unwrap().path.id(), second_id);
    paths.paths.pop();
    paths.rebuild_indices();
    assert!(paths.get_by_id(&first_id).is_none());
    assert_eq!(paths.paths_by_pool.len(), 1);
    assert_eq!(paths.get_by_id(&second_id).unwrap().path.id(), second_id);
}

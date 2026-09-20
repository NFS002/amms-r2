use super::*;
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
    let mut index: AddressMap<Vec<PathId>> = AddressMap::default();
    for (id, entry) in result.paths.iter().enumerate() {
        assert!(entry.last_simulation.is_none());
        let route: Route = entry
            .path
            .hops
            .iter()
            .map(|h| (h.pool.address(), h.base, h.quote))
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
    for route in &routes {
        let reverse: Route = route.iter().rev().map(|&(p, a, b)| (p, b, a)).collect();
        assert!(routes.contains(&reverse));
    }
    let restored: UniswapArbPaths =
        serde_json::from_value(serde_json::to_value(&result).unwrap()).unwrap();
    assert_eq!(restored.paths_by_pool, result.paths_by_pool);
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
    let mut expected: AddressMap<Vec<PathId>> = AddressMap::default();
    for (id, entry) in result.paths.iter().enumerate() {
        for hop in &entry.path.hops {
            expected.entry(hop.pool.address()).or_default().push(id);
        }
    }
    assert_eq!(result.paths_by_pool, expected);
}

use std::{collections::HashMap, fmt, sync::Arc, time::Instant};

use crate::amms::{
    amm::{AutomatedMarketMaker, UniswapPool, AMM},
    error::AMMError,
    uniswap_v2::UniswapV2Pool,
    uniswap_v3::UniswapV3Pool,
    Token,
};
use crate::state_space::StateSpace;
use alloy::primitives::{keccak256, map::AddressMap, Address, BlockHash, B256, I256, U256};
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};

// Hash topology only; reserves and other pool state do not affect identity.
fn route_id(hops: impl Iterator<Item = (Address, Address, Address)>) -> B256 {
    let mut route = Vec::new();
    for (pool, base, quote) in hops {
        route.extend_from_slice(pool.as_slice());
        route.extend_from_slice(base.as_slice());
        route.extend_from_slice(quote.as_slice());
    }
    keccak256(route)
}

// Serialized IDs are derived data: reconstruct them from topology on input.
#[derive(Deserialize)]
struct PathTopology {
    hops: Vec<UniswapHop>,
}

impl From<PathTopology> for UniswapArbPath {
    fn from(value: PathTopology) -> Self {
        Self::from_hops(value.hops)
    }
}

#[derive(Deserialize)]
struct SnapshotTopology {
    hops: Vec<UniswapHopSnapshot>,
}

impl From<SnapshotTopology> for UniswapArbPathSnapshot {
    fn from(value: SnapshotTopology) -> Self {
        Self::from_hops(value.hops)
    }
}

type PathIndex = usize;

#[derive(Debug, Clone)]
pub struct SwapHop {
    pub amm: AMM,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapHop {
    pub pool_address: Address,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct ArbPath {
    pub hops: Vec<SwapHop>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "PathTopology")]
pub struct UniswapArbPath {
    hops: Vec<UniswapHop>,
    id: B256,
}

impl UniswapArbPath {
    pub fn id(&self) -> B256 {
        self.id
    }

    pub fn hops(&self) -> &[UniswapHop] {
        &self.hops
    }

    pub fn from_hops(hops: Vec<UniswapHop>) -> Self {
        let id = route_id(
            hops.iter()
                .map(|hop| (hop.pool_address, hop.base, hop.quote)),
        );
        Self { hops, id }
    }

    /// Borrow the pools currently stored in this state for simulation or display.
    ///
    /// The returned view cannot outlive the state borrow (or its read guard).
    pub fn resolve<'state>(
        &self,
        state: &'state StateSpace,
    ) -> Result<ResolvedUniswapArbPath<'state>, AMMError> {
        self.resolve_pools(&state.pools)
    }

    pub(crate) fn resolve_pools<'state>(
        &self,
        pools: &'state HashMap<Address, AMM>,
    ) -> Result<ResolvedUniswapArbPath<'state>, AMMError> {
        let hops = self
            .hops
            .iter()
            .map(|hop| {
                let pool = match pools
                    .get(&hop.pool_address)
                    .ok_or(AMMError::PoolNotFound(hop.pool_address))?
                {
                    AMM::UniswapV2Pool(pool) => ResolvedUniswapPool::V2(pool),
                    AMM::UniswapV3Pool(pool) => ResolvedUniswapPool::V3(pool),
                    _ => return Err(AMMError::UnsupportedUniswapPool(hop.pool_address)),
                };
                Ok(ResolvedUniswapHop {
                    pool,
                    base: hop.base,
                    quote: hop.quote,
                })
            })
            .collect::<Result<_, AMMError>>()?;

        Ok(ResolvedUniswapArbPath { hops, id: self.id })
    }
}

impl fmt::Display for UniswapArbPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, hop) in self.hops.iter().enumerate() {
            writeln!(f, "Hop {}/{}", idx + 1, self.hops.len())?;
            writeln!(f, "\tPool: {}", hop.pool_address)?;
            writeln!(f, "\tDirection: {} -> {}", hop.base, hop.quote)?;
        }
        Ok(())
    }
}

impl fmt::Debug for UniswapArbPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// A temporary path view over the live pools borrowed from one state.
pub struct ResolvedUniswapArbPath<'state> {
    id: B256,
    hops: Vec<ResolvedUniswapHop<'state>>,
}

/// An owned copy of the pools and directions used by a simulation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "SnapshotTopology")]
pub struct UniswapArbPathSnapshot {
    id: B256,
    hops: Vec<UniswapHopSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapHopSnapshot {
    pub pool: UniswapPool,
    pub base: Address,
    pub quote: Address,
}

impl UniswapArbPathSnapshot {
    pub fn id(&self) -> B256 {
        self.id
    }

    pub fn hops(&self) -> &[UniswapHopSnapshot] {
        &self.hops
    }

    pub fn from_hops(hops: Vec<UniswapHopSnapshot>) -> Self {
        let id = route_id(
            hops.iter()
                .map(|hop| (hop.pool.address(), hop.base, hop.quote)),
        );
        Self { hops, id }
    }

    fn as_resolved(&self) -> ResolvedUniswapArbPath<'_> {
        ResolvedUniswapArbPath {
            id: self.id,
            hops: self
                .hops
                .iter()
                .map(|hop| ResolvedUniswapHop {
                    pool: match &hop.pool {
                        UniswapPool::V2(pool) => ResolvedUniswapPool::V2(pool),
                        UniswapPool::V3(pool) => ResolvedUniswapPool::V3(pool),
                    },
                    base: hop.base,
                    quote: hop.quote,
                })
                .collect(),
        }
    }

    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        self.as_resolved().simulate(amount_in)
    }
}

impl fmt::Display for UniswapArbPathSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.as_resolved(), f)
    }
}

impl fmt::Debug for UniswapArbPathSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

struct ResolvedUniswapHop<'state> {
    pool: ResolvedUniswapPool<'state>,
    base: Address,
    quote: Address,
}

enum ResolvedUniswapPool<'state> {
    V2(&'state UniswapV2Pool),
    V3(&'state UniswapV3Pool),
}

impl ResolvedUniswapArbPath<'_> {
    pub fn id(&self) -> B256 {
        self.id
    }

    /// Clone the underlying pool values so the snapshot outlives the state guard.
    pub fn snapshot(&self) -> UniswapArbPathSnapshot {
        UniswapArbPathSnapshot {
            id: self.id,
            hops: self
                .hops
                .iter()
                .map(|hop| UniswapHopSnapshot {
                    pool: match hop.pool {
                        ResolvedUniswapPool::V2(pool) => UniswapPool::V2(pool.clone()),
                        ResolvedUniswapPool::V3(pool) => UniswapPool::V3(pool.clone()),
                    },
                    base: hop.base,
                    quote: hop.quote,
                })
                .collect(),
        }
    }

    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;
        for hop in &self.hops {
            amount = match hop.pool {
                ResolvedUniswapPool::V2(pool) => pool.simulate_swap(hop.base, hop.quote, amount)?,
                ResolvedUniswapPool::V3(pool) => pool.simulate_swap(hop.base, hop.quote, amount)?,
            };
        }
        Ok(amount)
    }
}

fn fmt_token(
    f: &mut fmt::Formatter<'_>,
    label: &str,
    token: &Token,
    reserves: Option<u128>,
) -> fmt::Result {
    writeln!(f, "\t\tToken ({label}): {}", token.address)?;
    writeln!(f, "\t\t\tToken ({label}) decimals: {}", token.decimals)?;
    if let Some(reserves) = reserves {
        writeln!(f, "\t\t\tToken ({label}) reserves: {reserves}")?;
    }
    Ok(())
}

impl fmt::Display for ResolvedUniswapArbPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, hop) in self.hops.iter().enumerate() {
            writeln!(f, "Hop {}/{}", idx + 1, self.hops.len())?;
            writeln!(f, "\tDirection: {} -> {}", hop.base, hop.quote)?;
            match hop.pool {
                ResolvedUniswapPool::V2(pool) => {
                    writeln!(f, "\tPool: {}", pool.address)?;
                    writeln!(f, "\tFee: {}", pool.fee)?;
                    fmt_token(f, "a", &pool.token_a, Some(pool.reserve_0))?;
                    fmt_token(f, "b", &pool.token_b, Some(pool.reserve_1))?;
                }
                ResolvedUniswapPool::V3(pool) => {
                    writeln!(f, "\tPool: {}", pool.address)?;
                    writeln!(f, "\tFee: {}", pool.fee)?;
                    fmt_token(f, "a", &pool.token_a, None)?;
                    fmt_token(f, "b", &pool.token_b, None)?;
                    writeln!(f, "\tLiquidity: {}", pool.liquidity)?;
                    writeln!(f, "\tSqrt price X96: {}", pool.sqrt_price)?;
                    writeln!(f, "\tTick: {}", pool.tick)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ResolvedUniswapArbPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationContext {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub parent_hash: BlockHash,
    pub block_timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
    pub gas_used: u64,
    pub gas_limit: u64,
    /// When the batch finished simulating.
    pub simulated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationBatch {
    pub context: Arc<SimulationContext>,
    pub results: Vec<UniswapSimulationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapSimulationResult {
    pub path_snapshot: UniswapArbPathSnapshot,
    pub amount_in: U256,
    pub amount_out: U256,
    pub pct_gain_bp: I256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSimulation {
    pub context: Arc<SimulationContext>,
    pub result: UniswapSimulationResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapArbPathEntry {
    pub path: UniswapArbPath,
    pub last_simulation: Option<CachedSimulation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(from = "PathEntries")]
pub struct UniswapArbPaths {
    pub paths: Vec<UniswapArbPathEntry>,
    /// Positions in `paths`; rebuild this index if paths are reordered or removed.
    pub paths_by_pool: AddressMap<Vec<PathIndex>>,
    /// Route IDs mapped to positions in `paths`.
    pub path_indices_by_id: HashMap<B256, PathIndex>,
}

#[derive(Deserialize)]
struct PathEntries {
    paths: Vec<UniswapArbPathEntry>,
}

impl From<PathEntries> for UniswapArbPaths {
    fn from(value: PathEntries) -> Self {
        let mut paths = Self {
            paths: value.paths,
            ..Self::default()
        };
        paths.rebuild_indices();
        paths
    }
}

impl UniswapArbPaths {
    pub fn get_by_id(&self, path_id: &B256) -> Option<&UniswapArbPathEntry> {
        self.path_indices_by_id
            .get(path_id)
            .and_then(|&index| self.paths.get(index))
    }

    /// Call after changing path topology or the ordering of `paths`.
    /// Duplicate route IDs resolve to their first entry.
    pub fn rebuild_indices(&mut self) {
        self.paths_by_pool.clear();
        self.path_indices_by_id.clear();
        for (index, entry) in self.paths.iter().enumerate() {
            self.path_indices_by_id
                .entry(entry.path.id)
                .or_insert(index);
            for hop in &entry.path.hops {
                let indices = self.paths_by_pool.entry(hop.pool_address).or_default();
                if indices.last() != Some(&index) {
                    indices.push(index);
                }
            }
        }
    }
}

impl ArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.amm.simulate_swap(hop.base, hop.quote, amount)?;
        }

        Ok(amount)
    }
}

/// Enumerate directed, three-distinct-pool cycles starting at `token_in`.
/// Index construction is linear; traversal visits connected two-hop candidates
/// and looks up closing pools rather than scanning every pool a third time.
pub fn find_arb_paths_v2(pools: Vec<UniswapV2Pool>, token_in: Address) -> UniswapArbPaths {
    let start_time = Instant::now();
    let pair_key = |a: Address, b: Address| if a < b { (a, b) } else { (b, a) };
    // Store both orientations, so direction is handled identically at every hop.
    let mut pools_by_token: AddressMap<Vec<(Address, usize)>> = AddressMap::default();
    let mut pools_by_pair: HashMap<(Address, Address), Vec<usize>> = HashMap::new();
    let mut seen: AddressMap<()> = AddressMap::default();
    for (id, pool) in pools.iter().enumerate() {
        let (a, b) = (pool.token_a.address, pool.token_b.address);
        if a == b || seen.insert(pool.address, ()).is_some() {
            continue;
        }
        pools_by_token.entry(a).or_default().push((b, id));
        pools_by_token.entry(b).or_default().push((a, id));
        pools_by_pair.entry(pair_key(a, b)).or_default().push(id);
    }

    let mut result = UniswapArbPaths::default();
    let starts = pools_by_token
        .get(&token_in)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let pb = ProgressBar::new(starts.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );
    for &(a, first) in starts {
        for &(b, second) in &pools_by_token[&a] {
            if b == token_in || first == second {
                continue;
            }
            let Some(closing) = pools_by_pair.get(&pair_key(b, token_in)) else {
                continue;
            };
            for &third in closing {
                if third == first || third == second {
                    continue;
                }
                let path_index = result.paths.len();
                let hops = [(first, token_in, a), (second, a, b), (third, b, token_in)]
                    .into_iter()
                    .map(|(id, base, quote)| {
                        result
                            .paths_by_pool
                            .entry(pools[id].address)
                            .or_default()
                            .push(path_index);
                        UniswapHop {
                            pool_address: pools[id].address,
                            base,
                            quote,
                        }
                    })
                    .collect();
                let path = UniswapArbPath::from_hops(hops);
                result
                    .path_indices_by_id
                    .entry(path.id)
                    .or_insert(path_index);
                result.paths.push(UniswapArbPathEntry {
                    path,
                    last_simulation: None,
                });
            }
        }
        pb.inc(1);
    }
    pb.finish_with_message(format!(
        "Generated {} 3-hop arbitrage paths in {} seconds",
        result.paths.len(),
        start_time.elapsed().as_secs()
    ));
    result
}

#[cfg(test)]
#[path = "tests/path.rs"]
mod tests;

use std::{collections::HashMap, time::Instant};

use crate::amms::{
    amm::{AutomatedMarketMaker, UniswapPool, AMM},
    error::AMMError,
    uniswap_v2::UniswapV2Pool,
};
use alloy::primitives::{map::AddressMap, Address, BlockHash, U256};
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};

type PathId = usize;

#[derive(Debug, Clone)]
pub struct SwapHop {
    pub amm: AMM,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapHop {
    pub pool: UniswapPool,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct ArbPath {
    pub hops: Vec<SwapHop>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapArbPath {
    pub hops: Vec<UniswapHop>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapV2SimulationResult {
    pub spread_pct: f64,
    pub amount_in: U256,
    pub amount_out: U256,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub simulated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapArbPathEntry {
    pub path: UniswapArbPath,
    pub last_simulation: Option<UniswapV2SimulationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UniswapArbPaths {
    pub paths: Vec<UniswapArbPathEntry>,
    paths_by_pool: AddressMap<Vec<PathId>>,
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

impl UniswapArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.pool.simulate_swap(hop.base, hop.quote, amount)?;
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
                let path_id = result.paths.len();
                let hops = [(first, token_in, a), (second, a, b), (third, b, token_in)]
                    .into_iter()
                    .map(|(id, base, quote)| {
                        result
                            .paths_by_pool
                            .entry(pools[id].address)
                            .or_default()
                            .push(path_id);
                        UniswapHop {
                            pool: UniswapPool::V2(pools[id].clone().into()),
                            base,
                            quote,
                        }
                    }).collect();
                result.paths.push(UniswapArbPathEntry {
                    path: UniswapArbPath { hops },
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

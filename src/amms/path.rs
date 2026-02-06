use std::time::Instant;

use alloy::primitives::{Address, U256};
use indicatif::{ProgressBar, ProgressStyle};
use crate::amms::{amm::{AMM, AutomatedMarketMaker, UniswapPool}, error::AMMError, uniswap_v2::UniswapV2Pool};
use itertools::Itertools;




#[derive(Debug, Clone)]
pub struct SwapHop {
    pub amm: AMM,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct UniswapHop {
    pub pool: UniswapPool,
    pub base: Address,
    pub quote: Address,
}

#[derive(Debug, Clone)]
pub struct ArbPath {
    pub hops: Vec<SwapHop>,
}


#[derive(Debug, Clone)]
pub struct UniswapArbPath {
    pub hops: Vec<UniswapHop>,
}

impl ArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.amm.simulate_swap(
                hop.base,
                hop.quote,
                amount,
            )?;
        }

        Ok(amount)
    }
}

impl UniswapArbPath {
    pub fn simulate(&self, amount_in: U256) -> Result<U256, AMMError> {
        let mut amount = amount_in;

        for hop in &self.hops {
            amount = hop.pool.simulate_swap(
                hop.base,
                hop.quote,
                amount,
            )?;
        }

        Ok(amount)
    }
}


pub fn find_arb_paths_v2(pools: Vec<UniswapV2Pool>, token_in: Address) -> Vec<UniswapArbPath> {
    let start_time = Instant::now();

    let token_out = token_in.clone();
    let mut paths = Vec::new();

    let pb = ProgressBar::new(pools.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

        for i in 0..pools.len() {
        let pool_1 = &pools[i];
        let tokens_1 = [pool_1.token_a.address, pool_1.token_b.address];
        //let can_trade_1 = (pool_1.token_a.address == token_in) || (pool_1.token_b.address == token_in);
        let can_trade_1 = tokens_1.contains(&token_in);

        if can_trade_1 {
            let zero_for_one_1 = tokens_1[0] == token_in;
            let (token_in_1, token_out_1) = if zero_for_one_1 {
                (pool_1.token_a.address, pool_1.token_b.address)
            } else {
                (pool_1.token_b.address, pool_1.token_a.address)
            };
            if token_in_1 != token_in {
                continue;
            }

            for j in 0..pools.len() {
                let pool_2 = &pools[j];
                let tokens_2 = [pool_2.token_a.address, pool_2.token_b.address];
                //let can_trade_2 = (pool_2.token_a.address == token_out_1) || (pool_2.token_b.address == token_out_1);
                let can_trade_2 = tokens_2.contains(&token_out_1);

                if can_trade_2 {
                    let zero_for_one_2 = pool_2.token_a.address == token_out_1;
                    let (token_in_2, token_out_2) = if zero_for_one_2 {
                        (pool_2.token_a.address, pool_2.token_b.address)
                    } else {
                        (pool_2.token_b.address, pool_2.token_a.address)
                    };
                    if token_out_1 != token_in_2 {
                        continue;
                    }

                    for k in 0..pools.len() {
                        let pool_3 = &pools[k];
                        let tokens_3 = [pool_3.token_a.address, pool_3.token_b.address];
                        //let can_trade_3 = (pool_3.token_a.address == token_out_2) || (pool_3.token_b.address == token_out_2);

                        let can_trade_3 = tokens_3.contains(&token_out_2) && pool_1.address != pool_3.address;

                        if can_trade_3 {
                            let zero_for_one_3 =
                                (pool_3.token_a.address == token_out_2) || (pool_3.token_b.address == token_out_2);
                            let (token_in_3, token_out_3) = if zero_for_one_3 {
                                (pool_3.token_a.address, pool_3.token_b.address)
                            } else {
                                (pool_3.token_b.address, pool_3.token_a.address)
                            };
                            if token_out_2 != token_in_3 {
                                continue;
                            }

                            if token_out_3 == token_out {
                                let unique_pool_cnt =
                                    vec![pool_1.address, pool_2.address, pool_3.address]
                                        .into_iter()
                                        .unique()
                                        .collect::<Vec<Address>>()
                                        .len();

                                if unique_pool_cnt < 3 {
                                    continue;
                                }

                                let hops = vec![
                                    UniswapHop {
                                        pool: UniswapPool::V2(pool_1.clone().into()),
                                        base: token_in_1,
                                        quote: token_out_1,
                                    },
                                    UniswapHop {
                                        pool: UniswapPool::V2(pool_2.clone().into()),
                                        base: token_in_2,
                                        quote: token_out_2,
                                    },
                                    UniswapHop {
                                        pool: UniswapPool::V2(pool_3.clone().into()),
                                        base: token_in_3,
                                        quote: token_out_3,
                                    },
                                ];

                                let arb_path = UniswapArbPath {
                                    hops,
                                };

                                paths.push(arb_path);
                            }
                        }
                    }
                }
            }
        }

        pb.inc(1);
    }

    pb.finish_with_message(format!(
        "Generated {} 3-hop arbitrage paths in {} seconds",
        paths.len(),
        start_time.elapsed().as_secs()
    ));
    paths
}


use futures::{stream::FuturesUnordered, StreamExt};
use std::{collections::VecDeque, future::Future};
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

pub enum RetryQueueOutcome<T, K> {
    Success(T),
    Retry(K),
}

pub async fn run_retry_queue<K, T, E, F, Fut>(
    keys: Vec<K>,
    mut make_future: F,
    max_retry_attempts: usize,
    retry_delay: Duration,
    target: &'static str,
) -> Result<(Vec<T>, Vec<K>), E>
where
    K: Clone,
    F: FnMut(K) -> Fut,
    Fut: Future<Output = Result<RetryQueueOutcome<T, K>, E>>,
{
    let mut futures = FuturesUnordered::new();
    for key in keys {
        futures.push(make_future(key));
    }

    let mut successes = Vec::new();
    let mut retry_queue = VecDeque::new();

    while let Some(res) = futures.next().await {
        match res? {
            RetryQueueOutcome::Success(value) => successes.push(value),
            RetryQueueOutcome::Retry(key) => retry_queue.push_back(key),
        }
    }

    for attempt in 0..max_retry_attempts {
        if retry_queue.is_empty() {
            break;
        }

        let retries_this_round = retry_queue.len();
        let mut retry_futures = FuturesUnordered::new();

        warn!(
            failed_pairs = retries_this_round,
            retry_attempt = attempt + 1,
            target,
            "call_raw failed",
        );

        for _ in 0..retries_this_round {
            let key = match retry_queue.pop_front() {
                Some(key) => key,
                None => break,
            };
            retry_futures.push(make_future(key));
        }

        while let Some(res) = retry_futures.next().await {
            match res? {
                RetryQueueOutcome::Success(value) => successes.push(value),
                RetryQueueOutcome::Retry(key) => retry_queue.push_back(key),
            }
        }

        if !retry_queue.is_empty() && attempt + 1 < max_retry_attempts {
            sleep(retry_delay).await;
        } else {
            info!(
                target,
                retries = attempt + 1,
                max_retry_attempts,
                "call_raw succeeded"
            );
        }
    }

    if !retry_queue.is_empty() {
        warn!(
            target,
            failed_batches = retry_queue.len(),
            max_retry_attempts,
            "call_raw failed after retries"
        );
    }

    Ok((successes, retry_queue.into_iter().collect()))
}

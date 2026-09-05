use sockudo_core::error::{Error, Result};
use std::future::Future;

pub(super) struct PurgeOutcome {
    pub deleted: u64,
    pub has_more: bool,
    pub error: Option<Error>,
}

/// Bound both deleted records and storage calls. A page can make cursor
/// progress without deleting anything, so deleted-count limits alone can spin.
pub(super) async fn purge_tick<F, Fut>(
    cutoff: i64,
    batch_size: usize,
    max_per_tick: usize,
    mut purge: F,
) -> PurgeOutcome
where
    F: FnMut(i64, usize) -> Fut,
    Fut: Future<Output = Result<(u64, bool)>>,
{
    let mut outcome = PurgeOutcome {
        deleted: 0,
        has_more: false,
        error: None,
    };
    let batch_size = batch_size.max(1);
    let max_per_tick = max_per_tick.max(1);
    for _ in 0..max_per_tick.div_ceil(batch_size) {
        let remaining = max_per_tick.saturating_sub(outcome.deleted as usize);
        match purge(cutoff, batch_size.min(remaining)).await {
            Ok((deleted, has_more)) => {
                outcome.deleted = outcome.deleted.saturating_add(deleted);
                outcome.has_more = has_more;
                if !has_more || outcome.deleted >= max_per_tick as u64 {
                    break;
                }
            }
            Err(error) => {
                outcome.error = Some(error);
                break;
            }
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn progressing_empty_pages_cannot_monopolize_a_tick() {
        let mut calls = 0;
        let outcome = purge_tick(42, 10, 25, |cutoff, limit| {
            assert_eq!((cutoff, limit), (42, 10));
            calls += 1;
            std::future::ready(Ok((0, true)))
        })
        .await;
        assert_eq!(calls, 3);
        assert_eq!(outcome.deleted, 0);
        assert!(outcome.has_more);
    }
    #[tokio::test]
    async fn final_batch_honors_remaining_budget_and_preserves_partial_failure() {
        let mut limits = Vec::new();
        let outcome = purge_tick(42, 10, 25, |_, limit| {
            limits.push(limit);
            std::future::ready(Ok((limit as u64, true)))
        })
        .await;
        assert_eq!(limits, [10, 10, 5]);
        assert_eq!(outcome.deleted, 25);
        let mut calls = 0;
        let outcome = purge_tick(42, 10, 25, |_, _| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Ok((4, true))
            } else {
                Err(Error::Internal("fixture failure".into()))
            })
        })
        .await;
        assert_eq!(calls, 2);
        assert_eq!(outcome.deleted, 4);
        assert!(outcome.error.is_some());
    }
}

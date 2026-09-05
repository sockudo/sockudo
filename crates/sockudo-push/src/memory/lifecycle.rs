use super::*;
use crate::lifecycle::{
    LifecycleScan, PLANNER_RECEIPT_ID, PublishTombstone, log_is_old_enough, status_is_old_enough,
};

pub(super) fn cleanup(
    inner: &mut MemoryPushState,
    request: &PushCleanupRequest,
    report: &mut PushCleanupReport,
    remaining: &mut usize,
) -> PushStorageResult<()> {
    // Finite tombstones fence delayed duplicate work while children are removed over several ticks.
    let retired: Vec<_> = inner
        .lifecycle_tombstones
        .range((
            Excluded(inner.lifecycle_retired_cursor.clone().unwrap_or_default()),
            Unbounded,
        ))
        .take(request.policy.batch_size)
        .map(|(key, _)| key.clone())
        .collect();
    if retired.is_empty() {
        inner.lifecycle_retired_cursor = None;
    }
    for key in retired {
        if *remaining == 0 {
            break;
        }
        inner.lifecycle_retired_cursor = Some(key.clone());
        let shards: Vec<_> = inner
            .fanout_shards
            .range((
                Excluded((key.0.clone(), key.1.clone(), String::new())),
                Unbounded,
            ))
            .take_while(|((app, publish, _), _)| app == &key.0 && publish == &key.1)
            .take(request.limit_for(*remaining))
            .map(|(key, _)| key.clone())
            .collect();
        for shard in &shards {
            inner.fanout_shards.remove(shard);
        }
        report
            .fanout_shards
            .record(shards.len() as u64, shards.len() as u64);
        *remaining = remaining.saturating_sub(shards.len());
        if *remaining == 0 {
            break;
        }
        let logs: Vec<_> = inner
            .publish_log_by_publish
            .range((
                Excluded((key.0.clone(), key.1.clone(), 0, String::new())),
                Unbounded,
            ))
            .take_while(|(app, publish, _, _)| app == &key.0 && publish == &key.1)
            .take(request.limit_for(*remaining))
            .cloned()
            .collect();
        for log in &logs {
            inner
                .publish_log
                .remove(&(log.0.clone(), log.2, log.3.clone()));
            inner.publish_log_by_publish.remove(log);
        }
        report
            .publish_logs
            .record(logs.len() as u64, logs.len() as u64);
        *remaining = remaining.saturating_sub(logs.len());
        let no_shards = inner
            .fanout_shards
            .range((
                Excluded((key.0.clone(), key.1.clone(), String::new())),
                Unbounded,
            ))
            .next()
            .is_none_or(|((app, publish, _), _)| app != &key.0 || publish != &key.1);
        let no_logs = inner
            .publish_log_by_publish
            .range((
                Excluded((key.0.clone(), key.1.clone(), 0, String::new())),
                Unbounded,
            ))
            .next()
            .is_none_or(|(app, publish, _, _)| app != &key.0 || publish != &key.1);
        if no_shards && no_logs && inner.lifecycle_tombstones[&key].keep_until_ms <= request.now_ms
        {
            inner.lifecycle_tombstones.remove(&key);
        }
    }
    if *remaining == 0 {
        return Ok(());
    }
    let start = inner.lifecycle_cursor.clone().unwrap_or_default();
    let candidates: Vec<_> = inner
        .publish_status
        .range((Excluded(start), Unbounded))
        .take(request.policy.batch_size)
        .map(|(key, status)| (key.clone(), status.clone()))
        .collect();
    if candidates.is_empty() {
        inner.lifecycle_cursor = None;
        return Ok(());
    }
    let mut scan_budget = request.policy.batch_size.clamp(2, 64);
    for (key, status) in candidates {
        if scan_budget == 0 || *remaining == 0 {
            break;
        }
        scan_budget -= 1;
        inner.lifecycle_cursor = Some(key.clone());
        if !status_is_old_enough(
            &status,
            request.now_ms,
            request.policy.publish_status_retention_ms,
        ) || !inner.fanout_shards.contains_key(&(
            key.0.clone(),
            key.1.clone(),
            PLANNER_RECEIPT_ID.to_owned(),
        )) || inner.scheduled_by_id.contains_key(&key)
            || [
                key.1.clone(),
                format!("planner:publish-log:{}", key.1),
                format!("repair:publish-log:{}", key.1),
            ]
            .iter()
            .any(|id| {
                inner
                    .scheduler_locks
                    .get(&(key.0.clone(), id.clone()))
                    .is_some_and(|lock| lock.expires_at_ms > request.now_ms)
            })
        {
            inner.lifecycle_scans.remove(&key);
            continue;
        }
        let mut scan = inner
            .lifecycle_scans
            .remove(&key)
            .filter(|scan| scan.revision == status.revision)
            .unwrap_or_else(|| LifecycleScan::new(status.revision));
        if scan_budget == 0 {
            inner.lifecycle_scans.insert(key, scan);
            break;
        }
        let rows: Vec<_> = inner
            .fanout_shards
            .range((
                Excluded((
                    key.0.clone(),
                    key.1.clone(),
                    scan.after_shard.clone().unwrap_or_default(),
                )),
                Unbounded,
            ))
            .take_while(|((app, publish, _), _)| app == &key.0 && publish == &key.1)
            .take(scan_budget)
            .map(|(_, shard)| shard.clone())
            .collect();
        for shard in &rows {
            scan.observe(shard);
        }
        scan_budget -= rows.len();
        let more = inner
            .fanout_shards
            .range((
                Excluded((
                    key.0.clone(),
                    key.1.clone(),
                    scan.after_shard.clone().unwrap_or_default(),
                )),
                Unbounded,
            ))
            .next()
            .is_some_and(|((app, publish, _), _)| app == &key.0 && publish == &key.1);
        if more {
            inner.lifecycle_scans.insert(key, scan);
            continue;
        }
        if !scan.proves_complete(&status) {
            continue;
        }
        let (after_ms, after_id) = scan.log_position()?;
        let mut logs: Vec<_> = inner
            .publish_log_by_publish
            .range((
                Excluded((key.0.clone(), key.1.clone(), after_ms, after_id)),
                Unbounded,
            ))
            .take_while(|(app, publish, _, _)| app == &key.0 && publish == &key.1)
            .take(scan_budget.saturating_add(1))
            .cloned()
            .collect();
        let more_logs = logs.len() > scan_budget;
        logs.truncate(scan_budget);
        scan_budget -= logs.len();
        for log in logs {
            scan.after_log = Some(format!("{:020}:{}", log.2, log.3));
            scan.has_log = true;
            scan.has_unsafe_log |= !log_is_old_enough(
                &inner.publish_log[&(log.0.clone(), log.2, log.3.clone())],
                request.now_ms,
            );
        }
        if !scan.finish_log_page(more_logs) {
            inner.lifecycle_scans.insert(key, scan);
            continue;
        }
        let encoded = sonic_rs::to_string(&(status.revision, status.updated_at_ms, &status.status))
            .map_err(|_| {
                PushStorageError::Backend("terminal status serialization failed".to_owned())
            })?;
        inner.lifecycle_tombstones.insert(
            key.clone(),
            PublishTombstone::new(&status, encoded, request.now_ms),
        );
        inner.publish_status.remove(&key);
        report.publish_statuses.record(1, 1);
        *remaining = remaining.saturating_sub(1);
    }
    Ok(())
}

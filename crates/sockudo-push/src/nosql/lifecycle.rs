use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::{from_json_str, to_json_string};
use super::ordered::FAMILY_PUBLISH_LOG_ORDERED;
use crate::cleanup::{PushCleanupReport, PushCleanupRequest};
use crate::domain::{PublishLogEvent, ShardJob};
use crate::lifecycle::{
    LifecycleScan, PLANNER_RECEIPT_ID, PublishTombstone, log_is_old_enough, status_is_old_enough,
};
use crate::storage::{PushStorageResult, SchedulerLock};

const FAMILY_SCAN: &str = "lifecycle-scan-v1";
const FAMILY_WALK: &str = "lifecycle-walk-v1";

pub(super) fn tombstone(data: &str) -> Option<PublishTombstone> {
    from_json_str::<PublishTombstone>(data)
        .ok()
        .filter(|value| value.lifecycle_version == 1)
}

pub(super) async fn cleanup<B: DocumentBackend>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    request: &PushCleanupRequest,
    remaining: &mut usize,
    report: &mut PushCleanupReport,
) -> PushStorageResult<()> {
    if *remaining == 0 {
        return Ok(());
    }
    let start = store
        .get_json::<String>(FAMILY_WALK, app_id, "statuses", DEFAULT_SK)
        .await?;
    let rows = store
        .backend
        .scan_pk_page(
            FAMILY_STATUS_UPDATED_TIME,
            app_id,
            "time",
            start.as_deref(),
            request.policy.batch_size.max(1),
        )
        .await?;
    if rows.is_empty() {
        store
            .backend
            .delete(FAMILY_WALK, app_id, "statuses", DEFAULT_SK)
            .await?;
        return Ok(());
    }
    let mut scan_budget = request.policy.batch_size.clamp(2, 64);
    for row in rows {
        if scan_budget == 0 || *remaining == 0 {
            break;
        }
        scan_budget -= 1;
        let publish_id: String = from_json_str(&row.data)?;
        let raw = store
            .backend
            .get_consistent(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK)
            .await?;
        store
            .put_json(FAMILY_WALK, app_id, "statuses", DEFAULT_SK, &row.sk)
            .await?;
        let Some(raw) = raw else {
            continue;
        };
        if let Some(retired) = tombstone(&raw) {
            let shards = store
                .backend
                .scan_pk_page(
                    FAMILY_FANOUT_SHARD,
                    app_id,
                    &publish_id,
                    None,
                    request.limit_for(*remaining),
                )
                .await?;
            let no_shards = shards.is_empty();
            for shard in shards {
                if store
                    .backend
                    .compare_and_delete(
                        FAMILY_FANOUT_SHARD,
                        app_id,
                        &publish_id,
                        &shard.sk,
                        &shard.data,
                    )
                    .await?
                {
                    report.fanout_shards.record(1, 1);
                    *remaining -= 1;
                }
            }
            if *remaining == 0 {
                break;
            }
            let logs = store
                .backend
                .scan_pk_page(
                    FAMILY_PUBLISH_LOG,
                    app_id,
                    &publish_id,
                    None,
                    request.limit_for(*remaining),
                )
                .await?;
            let no_logs = logs.is_empty();
            for log in logs {
                if store
                    .backend
                    .compare_and_delete(FAMILY_PUBLISH_LOG, app_id, &publish_id, &log.sk, &log.data)
                    .await?
                {
                    store
                        .delete_ordered_reference(
                            FAMILY_PUBLISH_LOG_ORDERED,
                            app_id,
                            &log.sk,
                            &publish_id,
                            &log.sk,
                        )
                        .await?;
                    report.publish_logs.record(1, 1);
                    *remaining -= 1;
                }
            }
            if no_shards
                && no_logs
                && retired.keep_until_ms <= request.now_ms
                && store
                    .backend
                    .compare_and_delete(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK, &raw)
                    .await?
            {
                store
                    .backend
                    .compare_and_delete(
                        FAMILY_STATUS_UPDATED_TIME,
                        app_id,
                        "time",
                        &row.sk,
                        &row.data,
                    )
                    .await?;
                store
                    .backend
                    .delete(FAMILY_STATUS_UPDATED, app_id, &publish_id, DEFAULT_SK)
                    .await?;
                store
                    .backend
                    .delete(FAMILY_SCAN, app_id, &publish_id, DEFAULT_SK)
                    .await?;
            }
            continue;
        }
        let Some((current_raw, status)) = store
            .read_versioned_publish_status(app_id, &publish_id)
            .await?
        else {
            continue;
        };
        if !status_is_old_enough(
            &status,
            request.now_ms,
            request.policy.publish_status_retention_ms,
        ) || store
            .backend
            .get(FAMILY_FANOUT_SHARD, app_id, &publish_id, PLANNER_RECEIPT_ID)
            .await?
            .is_none()
            || store
                .backend
                .get(FAMILY_SCHEDULED_JOB, app_id, &publish_id, DEFAULT_SK)
                .await?
                .is_some()
        {
            continue;
        }
        let mut locked = false;
        for id in [
            publish_id.clone(),
            format!("planner:publish-log:{publish_id}"),
            format!("repair:publish-log:{publish_id}"),
        ] {
            if store
                .get_json::<SchedulerLock>(FAMILY_SCHEDULER_LOCK, app_id, &id, DEFAULT_SK)
                .await?
                .is_some_and(|lock| lock.expires_at_ms > request.now_ms)
            {
                locked = true;
                break;
            }
        }
        if locked || scan_budget == 0 {
            continue;
        }
        let mut scan = store
            .get_json::<LifecycleScan>(FAMILY_SCAN, app_id, &publish_id, DEFAULT_SK)
            .await?
            .filter(|scan| scan.version == 1 && scan.revision == status.revision)
            .unwrap_or_else(|| LifecycleScan::new(status.revision));
        let mut shards = store
            .backend
            .scan_pk_page(
                FAMILY_FANOUT_SHARD,
                app_id,
                &publish_id,
                scan.after_shard.as_deref(),
                scan_budget.saturating_add(1),
            )
            .await?;
        let more = shards.len() > scan_budget;
        shards.truncate(scan_budget);
        scan_budget -= shards.len();
        for shard in shards {
            scan.observe(&from_json_str::<ShardJob>(&shard.data)?);
        }
        store
            .put_json(FAMILY_SCAN, app_id, &publish_id, DEFAULT_SK, &scan)
            .await?;
        if more || !scan.proves_complete(&status) {
            continue;
        }
        if scan_budget == 0 {
            continue;
        }
        let mut logs = store
            .backend
            .scan_pk_page(
                FAMILY_PUBLISH_LOG,
                app_id,
                &publish_id,
                scan.after_log.as_deref(),
                scan_budget.saturating_add(1),
            )
            .await?;
        let more_logs = logs.len() > scan_budget;
        logs.truncate(scan_budget);
        scan_budget -= logs.len();
        for log in logs {
            scan.after_log = Some(log.sk);
            scan.has_log = true;
            if !log_is_old_enough(
                &from_json_str::<PublishLogEvent>(&log.data)?,
                request.now_ms,
            ) {
                scan.has_unsafe_log = true;
            }
        }
        let logs_are_safe = scan.finish_log_page(more_logs);
        store
            .put_json(FAMILY_SCAN, app_id, &publish_id, DEFAULT_SK, &scan)
            .await?;
        if !logs_are_safe {
            continue;
        }
        let retired = PublishTombstone::new(&status, current_raw.clone(), request.now_ms);
        if store
            .backend
            .compare_and_swap(
                FAMILY_STATUS,
                app_id,
                &publish_id,
                DEFAULT_SK,
                &current_raw,
                to_json_string(&retired)?,
            )
            .await?
        {
            report.publish_statuses.record(1, 1);
            *remaining -= 1;
        }
    }
    Ok(())
}

use super::helpers::*;
use crate::cleanup::{PushCleanupReport, PushCleanupRequest};
use crate::domain::{PublishLogEvent, ShardJob};
use crate::lifecycle::{
    LifecycleScan, PLANNER_RECEIPT_ID, PublishTombstone, RETIRED_SQL_STATE, log_is_old_enough,
    status_is_old_enough,
};
use crate::storage::{PushPublishStatusStore, PushStorageResult};
use sqlx::Row;

macro_rules! impl_lifecycle_sql {
    ($store:ty, $postgres:expr, $json_cast:expr, $json_text:expr) => {
        impl $store {
            pub(super) async fn cleanup_lifecycle(&self, request: &PushCleanupRequest, report: &mut PushCleanupReport, remaining: &mut usize) -> PushStorageResult<()> {
                if *remaining == 0 { return Ok(()); }
                let mut cursor: (i64, String, String) = sqlx::query("SELECT scan_json FROM push_publish_lifecycle_scan WHERE app_id = '' AND publish_id = 'walk-v1'")
                    .fetch_optional(&self.pool).await.map_err(sql_error)?
                    .map(|row| from_json_str(&row.try_get::<String, _>("scan_json").map_err(sql_error)?)).transpose()?.unwrap_or_default();
                let cutoff = request.now_ms.saturating_sub(request.policy.publish_status_retention_ms.max(crate::retry::MAX_RETRY_AGE_MS));
                let q = sql_query(format!("SELECT app_id,publish_id,{} AS counters_json,{} AS revision,{} AS updated_at_ms,state FROM push_publish_status
                    WHERE (updated_at_ms,app_id,publish_id) > (?,?,?) AND (state='retiredv1' OR (updated_at_ms < ? AND state IN ('succeeded','partiallysucceeded','failed','expired','cancelled','quotaexceeded','deadlettered')))
                    ORDER BY updated_at_ms,app_id,publish_id LIMIT ?", json_text_expr("counters_json", $json_text), signed_i64_expr("revision", $postgres), signed_i64_expr("updated_at_ms", $postgres)), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(cursor.0).bind(&cursor.1).bind(&cursor.2).bind(cutoff as i64).bind(request.policy.batch_size.max(1) as i64)
                    .fetch_all(&self.pool).await.map_err(sql_error)?;
                if rows.is_empty() {
                    sqlx::query("DELETE FROM push_publish_lifecycle_scan WHERE app_id = '' AND publish_id = 'walk-v1'").execute(&self.pool).await.map_err(sql_error)?;
                    return Ok(());
                }
                let mut scan_budget = request.policy.batch_size.clamp(2, 64);
                for row in rows {
                    if *remaining == 0 || scan_budget == 0 { break; }
                    scan_budget -= 1;
                    let app_id: String = row.try_get("app_id").map_err(sql_error)?;
                    let publish_id: String = row.try_get("publish_id").map_err(sql_error)?;
                    let raw: String = row.try_get("counters_json").map_err(sql_error)?;
                    let revision: i64 = row.try_get("revision").map_err(sql_error)?;
                    let updated: i64 = row.try_get("updated_at_ms").map_err(sql_error)?;
                    let state: String = row.try_get("state").map_err(sql_error)?;
                    cursor = (updated, app_id.clone(), publish_id.clone());
                    self.write_lifecycle_scan("", "walk-v1", &to_json_string(&cursor)?, request.now_ms).await?;
                    if state == RETIRED_SQL_STATE {
                        let retired: PublishTombstone = from_json_str(&raw)?;
                        let q = sql_query("SELECT shard_id FROM push_fanout_shards WHERE app_id=? AND publish_id=? ORDER BY shard_id LIMIT ?".to_owned(), $postgres);
                        let shards = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(request.limit_for(*remaining) as i64).fetch_all(&self.pool).await.map_err(sql_error)?;
                        let no_shards = shards.is_empty();
                        for shard in shards {
                            let id: String = shard.try_get("shard_id").map_err(sql_error)?;
                            let q = sql_query("DELETE FROM push_fanout_shards WHERE app_id=? AND publish_id=? AND shard_id=?".to_owned(), $postgres);
                            let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(id).execute(&self.pool).await.map_err(sql_error)?.rows_affected();
                            report.fanout_shards.record(1, deleted); *remaining = remaining.saturating_sub(deleted as usize);
                        }
                        if *remaining == 0 { break; }
                        let q = sql_query(format!("SELECT {} AS occurred_at_ms,event_id FROM push_publish_log WHERE app_id=? AND publish_id=? ORDER BY occurred_at_ms,event_id LIMIT ?", signed_i64_expr("occurred_at_ms", $postgres)), $postgres);
                        let logs = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(request.limit_for(*remaining) as i64).fetch_all(&self.pool).await.map_err(sql_error)?;
                        let no_logs = logs.is_empty();
                        for log in logs {
                            let at: i64 = log.try_get("occurred_at_ms").map_err(sql_error)?;
                            let id: String = log.try_get("event_id").map_err(sql_error)?;
                            let q = sql_query("DELETE FROM push_publish_log WHERE app_id=? AND publish_id=? AND occurred_at_ms=? AND event_id=?".to_owned(), $postgres);
                            let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(at).bind(id).execute(&self.pool).await.map_err(sql_error)?.rows_affected();
                            report.publish_logs.record(1, deleted); *remaining = remaining.saturating_sub(deleted as usize);
                        }
                        if no_shards && no_logs && retired.keep_until_ms <= request.now_ms {
                            let q = sql_query("DELETE FROM push_publish_status WHERE app_id=? AND publish_id=? AND revision=? AND state='retiredv1'".to_owned(), $postgres);
                            sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(revision).execute(&self.pool).await.map_err(sql_error)?;
                            let q = sql_query("DELETE FROM push_publish_lifecycle_scan WHERE app_id=? AND publish_id=?".to_owned(), $postgres);
                            sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).execute(&self.pool).await.map_err(sql_error)?;
                        }
                        continue;
                    }
                    let Some(status) = self.get_versioned_publish_status(&app_id, &publish_id).await? else { continue; };
                    if status.revision != revision as u64 || !status_is_old_enough(&status, request.now_ms, request.policy.publish_status_retention_ms) { continue; }
                    let q = sql_query("SELECT 1 FROM push_fanout_shards WHERE app_id=? AND publish_id=? AND shard_id=?".to_owned(), $postgres);
                    if sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(PLANNER_RECEIPT_ID).fetch_optional(&self.pool).await.map_err(sql_error)?.is_none() { continue; }
                    let q = sql_query("SELECT 1 FROM push_scheduled_jobs WHERE app_id=? AND publish_id=? LIMIT 1".to_owned(), $postgres);
                    if sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).fetch_optional(&self.pool).await.map_err(sql_error)?.is_some() { continue; }
                    let q = sql_query("SELECT 1 FROM push_scheduler_locks WHERE app_id=? AND publish_id IN (?,?,?) AND expires_at_ms>? LIMIT 1".to_owned(), $postgres);
                    if sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(format!("planner:publish-log:{publish_id}")).bind(format!("repair:publish-log:{publish_id}")).bind(request.now_ms as i64).fetch_optional(&self.pool).await.map_err(sql_error)?.is_some() || scan_budget == 0 { continue; }
                    let q = sql_query("SELECT scan_json FROM push_publish_lifecycle_scan WHERE app_id=? AND publish_id=?".to_owned(), $postgres);
                    let mut scan = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).fetch_optional(&self.pool).await.map_err(sql_error)?
                        .map(|row| from_json_str::<LifecycleScan>(&row.try_get::<String, _>("scan_json").map_err(sql_error)?)).transpose()?
                        .filter(|scan| scan.version == 1 && scan.revision == status.revision).unwrap_or_else(|| LifecycleScan::new(status.revision));
                    let q = sql_query(format!("SELECT {} AS shard_json FROM push_fanout_shards WHERE app_id=? AND publish_id=? AND shard_id>? ORDER BY shard_id LIMIT ?", json_text_expr("shard_json", $json_text)), $postgres);
                    let mut shards = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(scan.after_shard.as_deref().unwrap_or("")).bind(scan_budget.saturating_add(1) as i64).fetch_all(&self.pool).await.map_err(sql_error)?;
                    let more = shards.len() > scan_budget;
                    shards.truncate(scan_budget); scan_budget -= shards.len();
                    for shard in shards { scan.observe(&from_json_str::<ShardJob>(&shard.try_get::<String, _>("shard_json").map_err(sql_error)?)?); }
                    self.write_lifecycle_scan(&app_id, &publish_id, &to_json_string(&scan)?, request.now_ms).await?;
                    if more || !scan.proves_complete(&status) || scan_budget == 0 { continue; }
                    let (after_ms, after_id) = scan.log_position()?;
                    let q = sql_query(format!("SELECT {} AS event_json FROM push_publish_log WHERE app_id=? AND publish_id=? AND (occurred_at_ms,event_id) > (?,?) ORDER BY occurred_at_ms,event_id LIMIT ?", json_text_expr("event_json", $json_text)), $postgres);
                    let mut logs = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(&app_id).bind(&publish_id).bind(after_ms as i64).bind(after_id).bind(scan_budget.saturating_add(1) as i64).fetch_all(&self.pool).await.map_err(sql_error)?;
                    let more_logs = logs.len() > scan_budget;
                    logs.truncate(scan_budget); scan_budget -= logs.len();
                    for log in logs {
                        let event = from_json_str::<PublishLogEvent>(&log.try_get::<String, _>("event_json").map_err(sql_error)?)?;
                        scan.after_log = Some(format!("{:020}:{}", event.occurred_at_ms, event.event_id));
                        scan.has_log = true;
                        scan.has_unsafe_log |= !log_is_old_enough(&event, request.now_ms);
                    }
                    let logs_are_safe = scan.finish_log_page(more_logs);
                    self.write_lifecycle_scan(&app_id, &publish_id, &to_json_string(&scan)?, request.now_ms).await?;
                    if !logs_are_safe { continue; }
                    let retired = PublishTombstone::new(&status, raw, request.now_ms);
                    let q = sql_query(format!("UPDATE push_publish_status SET state='retiredv1',counters_json={},revision=revision+1 WHERE app_id=? AND publish_id=? AND revision=? AND updated_at_ms=? AND state<>'retiredv1'", $json_cast), $postgres);
                    let changed = sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(to_json_string(&retired)?).bind(&app_id).bind(&publish_id).bind(revision).bind(updated).execute(&self.pool).await.map_err(sql_error)?.rows_affected();
                    report.publish_statuses.record(1, changed); *remaining = remaining.saturating_sub(changed as usize);
                }
                Ok(())
            }
            async fn write_lifecycle_scan(&self, app_id: &str, publish_id: &str, encoded: &str, now_ms: u64) -> PushStorageResult<()> {
                let upsert = if $postgres { "ON CONFLICT (app_id,publish_id) DO UPDATE SET scan_json=EXCLUDED.scan_json,updated_at_ms=EXCLUDED.updated_at_ms" } else { "ON DUPLICATE KEY UPDATE scan_json=VALUES(scan_json),updated_at_ms=VALUES(updated_at_ms)" };
                let q = sql_query(format!("INSERT INTO push_publish_lifecycle_scan (app_id,publish_id,scan_json,updated_at_ms) VALUES (?,?,?,?) {upsert}"), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(app_id).bind(publish_id).bind(encoded).bind(now_ms as i64).execute(&self.pool).await.map_err(sql_error)?;
                Ok(())
            }
        }
    }
}
#[cfg(feature = "postgres")]
impl_lifecycle_sql!(
    super::stores::PostgresPushStore,
    true,
    "?::jsonb",
    "?::text"
);
#[cfg(feature = "mysql")]
impl_lifecycle_sql!(
    super::stores::MySqlPushStore,
    false,
    "CAST(? AS JSON)",
    "CAST(? AS CHAR)"
);

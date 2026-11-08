use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sockudo_core::rate_limiter::RateLimitResult;

static MEMBER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

pub(crate) fn window_start_ms(now_ms: u64, window_secs: u64) -> u64 {
    now_ms.saturating_sub(window_secs.saturating_mul(1_000))
}

pub(crate) fn entry_member(now_ms: u64) -> String {
    let sequence = MEMBER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{}:{}:{}", now_ms, rand::random::<u64>(), sequence)
}

/// Executes cleanup, admission, insertion, and expiry as one Redis operation.
/// Reusing `member` makes a retry safe even when the first response was lost
/// after Redis committed the script.
#[derive(Clone, Copy)]
pub(crate) struct SlidingWindowRequest<'a> {
    pub(crate) key: &'a str,
    pub(crate) now_ms: u64,
    pub(crate) window_start_ms: u64,
    pub(crate) window_secs: u64,
    pub(crate) max_requests: u32,
    /// Units consumed by this request; admitted only if all of them fit.
    pub(crate) cost: u32,
    pub(crate) increment: bool,
    pub(crate) member: &'a str,
}

pub(crate) async fn run_sliding_window<C>(
    connection: &mut C,
    request: SlidingWindowRequest<'_>,
) -> redis::RedisResult<RateLimitResult>
where
    C: redis::aio::ConnectionLike,
{
    let (allowed, remaining): (u8, u32) = sliding_window_script()
        .key(request.key)
        .arg(request.window_start_ms)
        .arg(request.now_ms)
        .arg(request.window_secs)
        .arg(request.max_requests)
        .arg(u8::from(request.increment))
        .arg(request.member)
        .arg(request.cost.max(1))
        .invoke_async(connection)
        .await?;

    Ok(RateLimitResult {
        allowed: allowed == 1,
        remaining,
        reset_after: request.window_secs,
        limit: request.max_requests,
    })
}

fn sliding_window_script() -> &'static redis::Script {
    static SCRIPT: OnceLock<redis::Script> = OnceLock::new();
    SCRIPT.get_or_init(|| redis::Script::new(SLIDING_WINDOW_SCRIPT))
}

const SLIDING_WINDOW_SCRIPT: &str = r#"
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])

local count = redis.call('ZCARD', KEYS[1])
local limit = tonumber(ARGV[4])
local should_increment = ARGV[5] == '1'
local cost = tonumber(ARGV[7])
local already_counted = should_increment and redis.call('ZSCORE', KEYS[1], ARGV[6]) ~= false
local allowed = 0

if already_counted then
  allowed = 1
elseif count + cost <= limit then
  allowed = 1
  if should_increment then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[6])
    for i = 2, cost do
      redis.call('ZADD', KEYS[1], ARGV[2], ARGV[6] .. ':' .. i)
    end
    count = count + cost
  end
end

if count > 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[3])
end

return {allowed, math.max(0, limit - count)}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_start_uses_milliseconds() {
        assert_eq!(window_start_ms(10_000, 3), 7_000);
    }

    #[test]
    fn window_start_saturates() {
        assert_eq!(window_start_ms(500, 3), 0);
    }

    #[test]
    fn entry_members_are_unique_for_same_timestamp() {
        let now_ms = 42;

        let first = entry_member(now_ms);
        let second = entry_member(now_ms);

        assert_ne!(first, second);
        assert!(first.starts_with("42:"));
        assert!(second.starts_with("42:"));
    }

    #[test]
    fn sliding_window_script_is_atomic_and_retry_aware() {
        assert!(SLIDING_WINDOW_SCRIPT.contains("ZREMRANGEBYSCORE"));
        assert!(SLIDING_WINDOW_SCRIPT.contains("ZSCORE"));
        assert!(SLIDING_WINDOW_SCRIPT.contains("already_counted"));
        assert!(SLIDING_WINDOW_SCRIPT.contains("ZADD"));
    }
}

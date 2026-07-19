//! Atomic Redis state transitions for the reliable queue backend.
//!
//! Every script receives only keys belonging to one Redis Cluster hash slot.
//! `redis::Script` executes EVALSHA first and automatically falls back to
//! SCRIPT LOAD + EVALSHA after NOSCRIPT.

use redis::Script;

pub(crate) struct QueueScripts {
    pub enqueue: Script,
    pub enqueue_batch: Script,
    pub enqueue_batch_fast: Script,
    pub claim: Script,
    pub ack_and_claim: Script,
    pub ack_batch_and_claim: Script,
    pub nack: Script,
    pub renew: Script,
    pub replay_dead_letters: Script,
}

impl Default for QueueScripts {
    fn default() -> Self {
        Self {
            enqueue: Script::new(ENQUEUE),
            enqueue_batch: Script::new(ENQUEUE_BATCH),
            enqueue_batch_fast: Script::new(ENQUEUE_BATCH_FAST),
            claim: Script::new(CLAIM),
            ack_and_claim: Script::new(ACK_AND_CLAIM),
            ack_batch_and_claim: Script::new(ACK_BATCH_AND_CLAIM),
            nack: Script::new(NACK),
            renew: Script::new(RENEW),
            replay_dead_letters: Script::new(REPLAY_DEAD_LETTERS),
        }
    }
}

// KEYS: jobs, ready, delayed, attempts, maxima, dedup, dedup_expiry,
//       notify, events
// ARGV: id, payload, max_attempts, now_ms, delay_ms, dedup_key,
//       dedup_expires_ms, event_retention
const ENQUEUE: &str = r#"
local id = ARGV[1]
local payload = ARGV[2]
local now = tonumber(ARGV[4])
local delay = tonumber(ARGV[5])
local dedup_key = ARGV[6]
local event_retention = tonumber(ARGV[8])

local expired = redis.call('ZRANGEBYSCORE', KEYS[7], '-inf', now, 'LIMIT', 0, 100)
for _, key in ipairs(expired) do
  redis.call('HDEL', KEYS[6], key)
  redis.call('ZREM', KEYS[7], key)
end

local existing_payload = redis.call('HGET', KEYS[1], id)
if existing_payload then
  if existing_payload == payload then
    return {0, id}
  end
  return {-1, id}
end

if dedup_key ~= '' then
  local existing_id = redis.call('HGET', KEYS[6], dedup_key)
  if existing_id and redis.call('HEXISTS', KEYS[1], existing_id) == 1 then
    return {0, existing_id}
  end
  if existing_id then
    redis.call('HDEL', KEYS[6], dedup_key)
    redis.call('ZREM', KEYS[7], dedup_key)
  end
end

redis.call('HSET', KEYS[1], id, payload)
redis.call('HSET', KEYS[4], id, 0)
redis.call('HSET', KEYS[5], id, ARGV[3])

if dedup_key ~= '' then
  redis.call('HSET', KEYS[6], dedup_key, id)
  redis.call('ZADD', KEYS[7], ARGV[7], dedup_key)
end

if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, id)
else
  redis.call('LPUSH', KEYS[2], id)
end

redis.call('LPUSH', KEYS[8], '1')
redis.call('LTRIM', KEYS[8], 0, 1023)
if event_retention > 0 then
  redis.call('XADD', KEYS[9], 'MAXLEN', '~', event_retention, '*',
    'event', 'enqueued', 'job_id', id, 'timestamp', ARGV[4])
end
return {1, id}
"#;

// KEYS match ENQUEUE.
// ARGV: now_ms, job_count, event_retention, followed by repeating groups:
//       id, payload, max_attempts, delay_ms, dedup_key, dedup_expires_ms
const ENQUEUE_BATCH: &str = r#"
local now = tonumber(ARGV[1])
local count = tonumber(ARGV[2])
local event_retention = tonumber(ARGV[3])
local decisions = {}
local seen_ids = {}
local seen_dedup = {}
local ids = {}

for index = 0, count - 1 do
  ids[index + 1] = ARGV[4 + (index * 6)]
end
local existing_payloads = redis.call('HMGET', KEYS[1], unpack(ids))

local expired = redis.call('ZRANGEBYSCORE', KEYS[7], '-inf', now, 'LIMIT', 0, 1000)
for _, key in ipairs(expired) do
  redis.call('HDEL', KEYS[6], key)
  redis.call('ZREM', KEYS[7], key)
end

-- Validate every entry before inserting any job so ID conflicts cannot leave a
-- partially applied batch. Dedup cleanup above is safe housekeeping.
for index = 0, count - 1 do
  local base = 4 + (index * 6)
  local id = ARGV[base]
  local payload = ARGV[base + 1]
  local dedup_key = ARGV[base + 4]
  local existing_payload = existing_payloads[index + 1]
  if existing_payload and existing_payload ~= payload then
    return {{-1, id}}
  end
  if seen_ids[id] and seen_ids[id] ~= payload then
    return {{-1, id}}
  end

  local duplicate_id = nil
  if existing_payload or seen_ids[id] then
    duplicate_id = id
  elseif dedup_key ~= '' then
    duplicate_id = seen_dedup[dedup_key]
    if not duplicate_id then
      local stored_id = redis.call('HGET', KEYS[6], dedup_key)
      if stored_id and redis.call('HEXISTS', KEYS[1], stored_id) == 1 then
        duplicate_id = stored_id
      elseif stored_id then
        redis.call('HDEL', KEYS[6], dedup_key)
        redis.call('ZREM', KEYS[7], dedup_key)
      end
    end
  end

  if duplicate_id then
    decisions[index + 1] = {0, duplicate_id}
  else
    decisions[index + 1] = {1, id}
    seen_ids[id] = payload
    if dedup_key ~= '' then seen_dedup[dedup_key] = id end
  end
end

local job_values = {}
local attempt_values = {}
local maximum_values = {}
local ready_ids = {}
local delayed_values = {}
local dedup_values = {}
local dedup_expiries = {}

for index = 0, count - 1 do
  local base = 4 + (index * 6)
  local decision = decisions[index + 1]
  local status = decision[1]
  local id = decision[2]
  if status == 1 then
    local payload = ARGV[base + 1]
    local maximum = ARGV[base + 2]
    local delay = tonumber(ARGV[base + 3])
    local dedup_key = ARGV[base + 4]
    local dedup_expires = ARGV[base + 5]
    job_values[#job_values + 1] = id
    job_values[#job_values + 1] = payload
    attempt_values[#attempt_values + 1] = id
    attempt_values[#attempt_values + 1] = 0
    maximum_values[#maximum_values + 1] = id
    maximum_values[#maximum_values + 1] = maximum
    if dedup_key ~= '' then
      dedup_values[#dedup_values + 1] = dedup_key
      dedup_values[#dedup_values + 1] = id
      dedup_expiries[#dedup_expiries + 1] = dedup_expires
      dedup_expiries[#dedup_expiries + 1] = dedup_key
    end
    if delay > 0 then
      delayed_values[#delayed_values + 1] = now + delay
      delayed_values[#delayed_values + 1] = id
    else
      ready_ids[#ready_ids + 1] = id
    end
    if event_retention > 0 then
      redis.call('XADD', KEYS[9], 'MAXLEN', '~', event_retention, '*',
        'event', 'enqueued', 'job_id', id, 'timestamp', ARGV[1])
    end
  end
end
if #job_values > 0 then
  redis.call('HSET', KEYS[1], unpack(job_values))
  redis.call('HSET', KEYS[4], unpack(attempt_values))
  redis.call('HSET', KEYS[5], unpack(maximum_values))
end
if #ready_ids > 0 then
  redis.call('LPUSH', KEYS[2], unpack(ready_ids))
end
if #delayed_values > 0 then
  redis.call('ZADD', KEYS[3], unpack(delayed_values))
end
if #dedup_values > 0 then
  redis.call('HSET', KEYS[6], unpack(dedup_values))
  redis.call('ZADD', KEYS[7], unpack(dedup_expiries))
end
redis.call('LPUSH', KEYS[8], '1')
redis.call('LTRIM', KEYS[8], 0, 1023)
return decisions
"#;

// Optimized common path for immediate jobs without a per-job dedup key or
// max-attempt override. Stable-ID validation remains atomic, but arguments,
// writes, and the response are vectorized for bulk throughput.
// KEYS: jobs, ready, attempts, maxima, notify, events
// ARGV: now_ms, job_count, event_retention, max_attempts, then id/payload pairs
// RETURN: inserted_count, conflicting_id (empty on success)
const ENQUEUE_BATCH_FAST: &str = r#"
local count = tonumber(ARGV[2])
local event_retention = tonumber(ARGV[3])
local ids = {}
local payloads = {}
for index = 0, count - 1 do
  local base = 5 + (index * 2)
  ids[index + 1] = ARGV[base]
  payloads[index + 1] = ARGV[base + 1]
end

local existing = redis.call('HMGET', KEYS[1], unpack(ids))
local seen = {}
local job_values = {}
local attempt_values = {}
local maximum_values = {}
local ready_ids = {}
local inserted = 0

for index = 1, count do
  local id = ids[index]
  local payload = payloads[index]
  if existing[index] and existing[index] ~= payload then
    return {-1, id}
  end
  if seen[id] and seen[id] ~= payload then
    return {-1, id}
  end
  if not existing[index] and not seen[id] then
    seen[id] = payload
    inserted = inserted + 1
    job_values[#job_values + 1] = id
    job_values[#job_values + 1] = payload
    attempt_values[#attempt_values + 1] = id
    attempt_values[#attempt_values + 1] = 0
    maximum_values[#maximum_values + 1] = id
    maximum_values[#maximum_values + 1] = ARGV[4]
    ready_ids[#ready_ids + 1] = id
  end
end

if inserted > 0 then
  redis.call('HSET', KEYS[1], unpack(job_values))
  redis.call('HSET', KEYS[3], unpack(attempt_values))
  redis.call('HSET', KEYS[4], unpack(maximum_values))
  redis.call('LPUSH', KEYS[2], unpack(ready_ids))
  redis.call('LPUSH', KEYS[5], '1')
  redis.call('LTRIM', KEYS[5], 0, 1023)
  if event_retention > 0 then
    for _, id in ipairs(ready_ids) do
      redis.call('XADD', KEYS[6], 'MAXLEN', '~', event_retention, '*',
        'event', 'enqueued', 'job_id', id, 'timestamp', ARGV[1])
    end
  end
end
return {inserted, ''}
"#;

// KEYS: jobs, ready, delayed, active, tokens, attempts, maxima, dlq,
//       failures, notify, completed, events, inline_ready, inline_ready_count
// ARGV: now_ms, lease_deadline_ms, token_prefix, stalled_batch_size,
//       failed_retention, event_retention, claim_limit, default_max_attempts
const CLAIM: &str = r#"
local now = tonumber(ARGV[1])
local stalled_limit = tonumber(ARGV[4])
local failed_retention = tonumber(ARGV[5])
local event_retention = tonumber(ARGV[6])
local claim_limit = tonumber(ARGV[7])

local function emit(event_name, id)
  if event_retention > 0 then
    redis.call('XADD', KEYS[12], 'MAXLEN', '~', event_retention, '*',
      'event', event_name, 'job_id', id, 'timestamp', ARGV[1])
  end
end

local function trim_dlq()
  if failed_retention <= 0 then return end
  local excess = redis.call('ZCARD', KEYS[8]) - failed_retention
  if excess > 0 then
    local victims = redis.call('ZRANGE', KEYS[8], 0, excess - 1)
    for _, victim in ipairs(victims) do
      redis.call('ZREM', KEYS[8], victim)
      redis.call('HDEL', KEYS[1], victim)
      redis.call('HDEL', KEYS[6], victim)
      redis.call('HDEL', KEYS[7], victim)
      redis.call('HDEL', KEYS[9], victim)
    end
  end
end

local stalled = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now,
  'LIMIT', 0, stalled_limit)
for _, id in ipairs(stalled) do
  redis.call('ZREM', KEYS[4], id)
  redis.call('HDEL', KEYS[5], id)
  local attempt = tonumber(redis.call('HGET', KEYS[6], id) or '0')
  local maximum = tonumber(redis.call('HGET', KEYS[7], id) or ARGV[8])
  if attempt >= maximum then
    redis.call('ZADD', KEYS[8], now, id)
    redis.call('HSET', KEYS[9], id, 'lease-expired')
    emit('failed', id)
  else
    redis.call('LPUSH', KEYS[2], id)
    emit('stalled', id)
  end
end
trim_dlq()

local due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now,
  'LIMIT', 0, stalled_limit)
for _, id in ipairs(due) do
  if redis.call('ZREM', KEYS[3], id) == 1 then
    redis.call('LPUSH', KEYS[2], id)
    emit('waiting', id)
  end
end

local claimed = {}

local function activate(id, payload, generated)
  if not payload or (not generated and (
      redis.call('HGET', KEYS[5], id)
      or redis.call('ZSCORE', KEYS[11], id))) then
    return
  end
  local attempt = redis.call('HINCRBY', KEYS[6], id, 1)
  local maximum = tonumber(ARGV[8])
  if not generated then
    maximum = tonumber(redis.call('HGET', KEYS[7], id) or ARGV[8])
  end
  if attempt <= maximum then
    local token = ARGV[3] .. ':' .. tostring(#claimed + 1)
    redis.call('HSET', KEYS[5], id, token)
    redis.call('ZADD', KEYS[4], ARGV[2], id)
    emit('active', id)
    claimed[#claimed + 1] = {
      id, payload, tostring(attempt), tostring(maximum), token
    }
  else
    redis.call('ZADD', KEYS[8], now, id)
    redis.call('HSET', KEYS[9], id, 'attempts-exhausted')
    emit('failed', id)
    trim_dlq()
  end
end

local function read_u32(value, position)
  local b1, b2, b3, b4 = string.byte(value, position, position + 3)
  if not b4 then return nil end
  return (((b1 * 256) + b2) * 256 + b3) * 256 + b4
end

local function write_u32(value)
  local b4 = value % 256
  value = math.floor(value / 256)
  local b3 = value % 256
  value = math.floor(value / 256)
  local b2 = value % 256
  local b1 = math.floor(value / 256) % 256
  return string.char(b1, b2, b3, b4)
end

for _ = 1, stalled_limit + claim_limit do
  if #claimed >= claim_limit then break end
  local id = redis.call('RPOP', KEYS[2])
  local payload = nil
  if id then
    payload = redis.call('HGET', KEYS[1], id)
    activate(id, payload, false)
  else
    local inline = redis.call('RPOP', KEYS[13])
    if not inline then break end
    if string.sub(inline, 1, 1) == 'B' then
      local batch_count = read_u32(inline, 2)
      local position = 6
      local consumed = 0
      local valid = batch_count ~= nil and batch_count > 0
      while valid and consumed < batch_count and #claimed < claim_limit do
        local length = read_u32(inline, position)
        if not length then valid = false break end
        position = position + 4
        local record_end = position + length - 1
        if length <= 33 or record_end > string.len(inline) then
          valid = false break
        end
        local marker = string.sub(inline, position, position)
        id = string.sub(inline, position + 1, position + 32)
        payload = string.sub(inline, position + 33, record_end)
        position = position + length
        consumed = consumed + 1
        if marker == 'J' and not redis.call('HGET', KEYS[1], id) then
          redis.call('HSET', KEYS[1], id, payload)
          activate(id, payload, true)
        end
      end
      if valid and consumed == batch_count and position ~= string.len(inline) + 1 then
        valid = false
      end
      if valid then
        redis.call('DECRBY', KEYS[14], consumed)
        local remaining = batch_count - consumed
        if remaining > 0 then
          redis.call('RPUSH', KEYS[13],
            'B' .. write_u32(remaining) .. string.sub(inline, position))
        end
      elseif batch_count then
        -- The producer is the only writer of packed frames. If a frame is
        -- corrupt, discard its full advertised count instead of leaving stats
        -- permanently inflated or repeatedly retrying malformed bytes.
        redis.call('DECRBY', KEYS[14], batch_count)
      end
    elseif string.sub(inline, 1, 1) == 'J' and string.len(inline) > 33 then
      redis.call('DECR', KEYS[14])
      id = string.sub(inline, 2, 33)
      payload = string.sub(inline, 34)
      if redis.call('HGET', KEYS[1], id)
          or redis.call('HGET', KEYS[5], id)
          or redis.call('ZSCORE', KEYS[11], id) then
        payload = nil
      else
        redis.call('HSET', KEYS[1], id, payload)
        activate(id, payload, true)
      end
    end
  end
end
return claimed
"#;

// Completes one job and leases the next ready job in one round trip. The
// optional maintenance pass keeps delayed/stalled recovery off the per-job hot
// path while preserving bounded recovery through a designated worker.
//
// KEYS: jobs, ready, delayed, active, tokens, attempts, maxima, dlq,
//       failures, completed, notify, events
// ARGV: current_id, worker_token, now_ms, lease_deadline_ms,
//       completed_retention, failed_retention, event_retention,
//       stalled_batch_size, run_maintenance, fetch_next, default_max_attempts
// RETURN: settled, next_id, next_payload, next_attempt, next_maximum
const ACK_AND_CLAIM: &str = r#"
local current_id = ARGV[1]
local worker_token = ARGV[2]
local now = tonumber(ARGV[3])
local completed_retention = tonumber(ARGV[5])
local failed_retention = tonumber(ARGV[6])
local event_retention = tonumber(ARGV[7])
local stalled_limit = tonumber(ARGV[8])

local function emit(event_name, id)
  if event_retention > 0 then
    redis.call('XADD', KEYS[12], 'MAXLEN', '~', event_retention, '*',
      'event', event_name, 'job_id', id, 'timestamp', ARGV[3])
  end
end

local function trim_dlq()
  if failed_retention <= 0 then return end
  local excess = redis.call('ZCARD', KEYS[8]) - failed_retention
  if excess > 0 then
    local victims = redis.call('ZRANGE', KEYS[8], 0, excess - 1)
    for _, victim in ipairs(victims) do
      redis.call('ZREM', KEYS[8], victim)
      redis.call('HDEL', KEYS[1], victim)
      redis.call('HDEL', KEYS[6], victim)
      redis.call('HDEL', KEYS[7], victim)
      redis.call('HDEL', KEYS[9], victim)
    end
  end
end

if redis.call('HGET', KEYS[5], current_id) ~= worker_token then
  return {0, '', '', 0, 0}
end

redis.call('ZREM', KEYS[4], current_id)
redis.call('HDEL', KEYS[5], current_id)
redis.call('HDEL', KEYS[9], current_id)

if completed_retention <= 0 then
  redis.call('HDEL', KEYS[1], current_id)
  redis.call('HDEL', KEYS[6], current_id)
  redis.call('HDEL', KEYS[7], current_id)
else
  redis.call('ZADD', KEYS[10], now, current_id)
  local excess = redis.call('ZCARD', KEYS[10]) - completed_retention
  if excess > 0 then
    local victims = redis.call('ZRANGE', KEYS[10], 0, excess - 1)
    for _, victim in ipairs(victims) do
      redis.call('ZREM', KEYS[10], victim)
      redis.call('HDEL', KEYS[1], victim)
      redis.call('HDEL', KEYS[6], victim)
      redis.call('HDEL', KEYS[7], victim)
      redis.call('HDEL', KEYS[9], victim)
    end
  end
end
emit('completed', current_id)

if tonumber(ARGV[9]) == 1 then
  local stalled = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now,
    'LIMIT', 0, stalled_limit)
  for _, id in ipairs(stalled) do
    redis.call('ZREM', KEYS[4], id)
    redis.call('HDEL', KEYS[5], id)
    local attempt = tonumber(redis.call('HGET', KEYS[6], id) or '0')
    local maximum = tonumber(redis.call('HGET', KEYS[7], id) or ARGV[11])
    if attempt >= maximum then
      redis.call('ZADD', KEYS[8], now, id)
      redis.call('HSET', KEYS[9], id, 'lease-expired')
      emit('failed', id)
    else
      redis.call('LPUSH', KEYS[2], id)
      emit('stalled', id)
    end
  end

  local due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now,
    'LIMIT', 0, stalled_limit)
  for _, id in ipairs(due) do
    if redis.call('ZREM', KEYS[3], id) == 1 then
      redis.call('LPUSH', KEYS[2], id)
      emit('waiting', id)
    end
  end
  trim_dlq()
end

if tonumber(ARGV[10]) ~= 1 then
  return {1, '', '', 0, 0}
end

for _ = 1, stalled_limit do
  local id = redis.call('RPOP', KEYS[2])
  if not id then
    return {1, '', '', 0, 0}
  end
  local payload = redis.call('HGET', KEYS[1], id)
  if payload then
    local attempt = redis.call('HINCRBY', KEYS[6], id, 1)
    local maximum = tonumber(redis.call('HGET', KEYS[7], id) or ARGV[11])
    if attempt <= maximum then
      redis.call('HSET', KEYS[5], id, worker_token)
      redis.call('ZADD', KEYS[4], ARGV[4], id)
      emit('active', id)
      return {1, id, payload, tostring(attempt), tostring(maximum)}
    end
    redis.call('ZADD', KEYS[8], now, id)
    redis.call('HSET', KEYS[9], id, 'attempts-exhausted')
    emit('failed', id)
    trim_dlq()
  end
end
return {1, '', '', 0, 0}
"#;

// Batches successful acknowledgements from all workers and leases one
// replacement per worker. This keeps callback concurrency exact while
// amortizing the Redis round trip and Lua invocation across the worker group.
//
// KEYS match ACK_AND_CLAIM.
// ARGV: now_ms, lease_deadline_ms, completed_retention, failed_retention,
//       event_retention, stalled_batch_size, run_maintenance, count,
//       default_max_attempts,
//       followed by repeating current_id, worker_token, fetch_next triples.
// RETURN: repeating {settled, next_id, next_payload, next_attempt, next_maximum}
const ACK_BATCH_AND_CLAIM: &str = r#"
local now = tonumber(ARGV[1])
local completed_retention = tonumber(ARGV[3])
local failed_retention = tonumber(ARGV[4])
local event_retention = tonumber(ARGV[5])
local stalled_limit = tonumber(ARGV[6])
local count = tonumber(ARGV[8])
local default_maximum = ARGV[9]
local valid = {}
local tokens = {}
local fetch_next = {}
local results = {}

local function emit(event_name, id)
  if event_retention > 0 then
    redis.call('XADD', KEYS[12], 'MAXLEN', '~', event_retention, '*',
      'event', event_name, 'job_id', id, 'timestamp', ARGV[1])
  end
end

local function trim_dlq()
  if failed_retention <= 0 then return end
  local excess = redis.call('ZCARD', KEYS[8]) - failed_retention
  if excess > 0 then
    local victims = redis.call('ZRANGE', KEYS[8], 0, excess - 1)
    for _, victim in ipairs(victims) do
      redis.call('ZREM', KEYS[8], victim)
      redis.call('HDEL', KEYS[1], victim)
      redis.call('HDEL', KEYS[6], victim)
      redis.call('HDEL', KEYS[7], victim)
      redis.call('HDEL', KEYS[9], victim)
    end
  end
end

for index = 0, count - 1 do
  local base = 10 + (index * 3)
  local id = ARGV[base]
  local token = ARGV[base + 1]
  tokens[index + 1] = token
  fetch_next[index + 1] = tonumber(ARGV[base + 2]) == 1
  if redis.call('HGET', KEYS[5], id) == token then
    valid[index + 1] = true
    redis.call('ZREM', KEYS[4], id)
    redis.call('HDEL', KEYS[5], id)
    redis.call('HDEL', KEYS[9], id)
    if completed_retention <= 0 then
      redis.call('HDEL', KEYS[1], id)
      redis.call('HDEL', KEYS[6], id)
      redis.call('HDEL', KEYS[7], id)
    else
      redis.call('ZADD', KEYS[10], now, id)
    end
    emit('completed', id)
  else
    valid[index + 1] = false
  end
end

if completed_retention > 0 then
  local excess = redis.call('ZCARD', KEYS[10]) - completed_retention
  if excess > 0 then
    local victims = redis.call('ZRANGE', KEYS[10], 0, excess - 1)
    for _, victim in ipairs(victims) do
      redis.call('ZREM', KEYS[10], victim)
      redis.call('HDEL', KEYS[1], victim)
      redis.call('HDEL', KEYS[6], victim)
      redis.call('HDEL', KEYS[7], victim)
      redis.call('HDEL', KEYS[9], victim)
    end
  end
end

if tonumber(ARGV[7]) == 1 then
  local stalled = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now,
    'LIMIT', 0, stalled_limit)
  for _, id in ipairs(stalled) do
    redis.call('ZREM', KEYS[4], id)
    redis.call('HDEL', KEYS[5], id)
    local attempt = tonumber(redis.call('HGET', KEYS[6], id) or '0')
    local maximum = tonumber(redis.call('HGET', KEYS[7], id) or default_maximum)
    if attempt >= maximum then
      redis.call('ZADD', KEYS[8], now, id)
      redis.call('HSET', KEYS[9], id, 'lease-expired')
      emit('failed', id)
    else
      redis.call('LPUSH', KEYS[2], id)
      emit('stalled', id)
    end
  end

  local due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now,
    'LIMIT', 0, stalled_limit)
  for _, id in ipairs(due) do
    if redis.call('ZREM', KEYS[3], id) == 1 then
      redis.call('LPUSH', KEYS[2], id)
      emit('waiting', id)
    end
  end
  trim_dlq()
end

for index = 1, count do
  if not valid[index] then
    results[index] = {0, '', '', 0, 0}
  elseif not fetch_next[index] then
    results[index] = {1, '', '', 0, 0}
  else
    local claimed = nil
    for _ = 1, stalled_limit do
      local id = redis.call('RPOP', KEYS[2])
      if not id then break end
      local payload = redis.call('HGET', KEYS[1], id)
      if payload then
        local attempt = redis.call('HINCRBY', KEYS[6], id, 1)
        local maximum = tonumber(redis.call('HGET', KEYS[7], id) or default_maximum)
        if attempt <= maximum then
          redis.call('HSET', KEYS[5], id, tokens[index])
          redis.call('ZADD', KEYS[4], ARGV[2], id)
          emit('active', id)
          claimed = {1, id, payload, tostring(attempt), tostring(maximum)}
          break
        end
        redis.call('ZADD', KEYS[8], now, id)
        redis.call('HSET', KEYS[9], id, 'attempts-exhausted')
        emit('failed', id)
        trim_dlq()
      end
    end
    results[index] = claimed or {1, '', '', 0, 0}
  end
end
return results
"#;

// KEYS: jobs, active, tokens, attempts, maxima, delayed, ready, dlq,
//       failures, notify, events
// ARGV: id, token, now_ms, retry_at_ms, reason_code, force_dead_letter,
//       failed_retention, event_retention, default_max_attempts
const NACK: &str = r#"
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[9], ARGV[1], ARGV[5])

local attempt = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local maximum = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or ARGV[9])
local force = tonumber(ARGV[6])
local event_retention = tonumber(ARGV[8])
local function emit(event_name)
  if event_retention > 0 then
    redis.call('XADD', KEYS[11], 'MAXLEN', '~', event_retention, '*',
      'event', event_name, 'job_id', ARGV[1], 'timestamp', ARGV[3])
  end
end

if force == 1 or attempt >= maximum then
  redis.call('ZADD', KEYS[8], ARGV[3], ARGV[1])
  emit('failed')
  local retention = tonumber(ARGV[7])
  if retention > 0 then
    local excess = redis.call('ZCARD', KEYS[8]) - retention
    if excess > 0 then
      local victims = redis.call('ZRANGE', KEYS[8], 0, excess - 1)
      for _, victim in ipairs(victims) do
        redis.call('ZREM', KEYS[8], victim)
        redis.call('HDEL', KEYS[1], victim)
        redis.call('HDEL', KEYS[4], victim)
        redis.call('HDEL', KEYS[5], victim)
        redis.call('HDEL', KEYS[9], victim)
      end
    end
  end
  return 2
end

local retry_at = tonumber(ARGV[4])
if retry_at > tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[6], retry_at, ARGV[1])
else
  redis.call('LPUSH', KEYS[7], ARGV[1])
end
redis.call('LPUSH', KEYS[10], '1')
redis.call('LTRIM', KEYS[10], 0, 1023)
emit('retrying')
return 1
"#;

// KEYS: active, tokens
// ARGV: lease_deadline_ms, count, followed by repeating id, token pairs
const RENEW: &str = r#"
local results = {}
for index = 0, tonumber(ARGV[2]) - 1 do
  local base = 3 + (index * 2)
  local id = ARGV[base]
  local token = ARGV[base + 1]
  if redis.call('HGET', KEYS[2], id) == token then
    redis.call('ZADD', KEYS[1], ARGV[1], id)
    results[index + 1] = 1
  else
    results[index + 1] = 0
  end
end
return results
"#;

// KEYS: jobs, dlq, failures, attempts, ready, notify, events
// ARGV: limit, now_ms, event_retention
const REPLAY_DEAD_LETTERS: &str = r#"
local ids = redis.call('ZRANGE', KEYS[2], 0, tonumber(ARGV[1]) - 1)
local replayed = 0
for _, id in ipairs(ids) do
  if redis.call('HEXISTS', KEYS[1], id) == 1 then
    redis.call('ZREM', KEYS[2], id)
    redis.call('HDEL', KEYS[3], id)
    redis.call('HSET', KEYS[4], id, 0)
    redis.call('LPUSH', KEYS[5], id)
    replayed = replayed + 1
    if tonumber(ARGV[3]) > 0 then
      redis.call('XADD', KEYS[7], 'MAXLEN', '~', tonumber(ARGV[3]), '*',
        'event', 'replayed', 'job_id', id, 'timestamp', ARGV[2])
    end
  else
    redis.call('ZREM', KEYS[2], id)
    redis.call('HDEL', KEYS[3], id)
    redis.call('HDEL', KEYS[4], id)
  end
end
if replayed > 0 then
  redis.call('LPUSH', KEYS[6], '1')
  redis.call('LTRIM', KEYS[6], 0, 1023)
end
return replayed
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_have_stable_hashes_and_are_non_empty() {
        let scripts = QueueScripts::default();
        for script in [
            scripts.enqueue,
            scripts.enqueue_batch,
            scripts.enqueue_batch_fast,
            scripts.claim,
            scripts.ack_and_claim,
            scripts.ack_batch_and_claim,
            scripts.nack,
            scripts.renew,
            scripts.replay_dead_letters,
        ] {
            assert_eq!(script.get_hash().len(), 40);
        }
    }
}

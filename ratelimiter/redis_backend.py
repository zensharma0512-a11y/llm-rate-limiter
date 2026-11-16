"""
Distributed rate limiter backed by Redis.

Same API and same semantics as the in-memory RateLimiter, but state lives in
Redis so every gateway instance shares one limit. Each decision runs as a
single Lua script, which Redis executes atomically: the read-refill-decide-
write sequence can never interleave with another request, from any process
on any machine.

Time comes from the Redis server's clock (TIME), not from the gateway
hosts, so clock skew between gateway machines cannot affect decisions.

Works with any client that has redis-py's interface:
    client.evalsha(sha, numkeys, *keys_and_args)
    client.eval(script, numkeys, *keys_and_args)
    client.hgetall(key) -> dict
    client.delete(*keys)
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Hashable, Optional

from .memory import Decision, Stats, Strategy

# Shared Lua helper: current time in seconds (server clock unless overridden).
_NOW = """
local now
if ARGV[4] ~= nil and ARGV[4] ~= '' then
  now = tonumber(ARGV[4])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) + tonumber(t[2]) / 1000000
end
"""

_STATS = """
local function record(ok)
  local field = ok and 'allowed' or 'rejected'
  for _, k in ipairs({STATS_KEY, GLOBAL_KEY}) do
    redis.call('HINCRBY', k, 'total', 1)
    redis.call('HINCRBY', k, field, 1)
  end
end
"""

# KEYS: 1 = event log (zset), 2 = meta hash {used, seq}, 3 = key stats, 4 = global stats
# ARGV: 1 = limit, 2 = window seconds, 3 = cost, 4 = optional now
SLIDING_WINDOW_LUA = _NOW + """
local STATS_KEY, GLOBAL_KEY = KEYS[3], KEYS[4]
""" + _STATS + """
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])
local cutoff = now - window

local meta = redis.call('HMGET', KEYS[2], 'used', 'seq')
local used = tonumber(meta[1] or '0')
local seq  = tonumber(meta[2] or '0')

-- Expire old events; members are "seq:cost" so weighted costs can be undone.
local old = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff)
for _, m in ipairs(old) do
  used = used - tonumber(string.match(m, ':(%d+)$'))
end
if #old > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
end

if used + cost <= limit then
  seq = seq + 1
  used = used + cost
  redis.call('ZADD', KEYS[1], now, seq .. ':' .. cost)
  redis.call('HSET', KEYS[2], 'used', used, 'seq', seq)
  local ttl = math.ceil(window * 1000) + 1000
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('PEXPIRE', KEYS[2], ttl)
  record(true)
  return {1, tostring(limit - used), '0'}
end

redis.call('HSET', KEYS[2], 'used', used)
local need, freed, retry = used + cost - limit, 0, 0
local events = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
for i = 1, #events, 2 do
  freed = freed + tonumber(string.match(events[i], ':(%d+)$'))
  if freed >= need then
    retry = math.max(0, tonumber(events[i + 1]) + window - now)
    break
  end
end
record(false)
return {0, tostring(limit - used), string.format('%.17g', retry)}
"""

# KEYS: 1 = bucket hash {tokens, last}, 2 = key stats, 3 = global stats
# ARGV: 1 = capacity, 2 = refill rate, 3 = cost, 4 = optional now
TOKEN_BUCKET_LUA = _NOW + """
local STATS_KEY, GLOBAL_KEY = KEYS[2], KEYS[3]
""" + _STATS + """
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])

local b = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(b[1] or capacity)
local last   = tonumber(b[2] or now)

local elapsed = now - last
if elapsed > 0 then                      -- a backwards clock is ignored
  tokens = math.min(capacity, tokens + elapsed * rate)
  last = now
end

local ok = tokens >= cost
if ok then tokens = tokens - cost end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens),
                            'last',   string.format('%.17g', last))
-- An idle bucket is full again after capacity/rate seconds; let Redis drop it.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
record(ok)

local remaining = math.floor(tokens + 1e-9)
if ok then return {1, tostring(remaining), '0'} end
return {0, tostring(remaining), string.format('%.17g', (cost - tokens) / rate)}
"""


def _sha(script: str) -> str:
    return hashlib.sha1(script.encode()).hexdigest()


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


class RedisRateLimiter:
    """
    RedisRateLimiter(client, Strategy.SLIDING_WINDOW, limit=100, window=60)
    RedisRateLimiter(client, Strategy.TOKEN_BUCKET, capacity=20, refill_rate=5)

    `clock` is for tests only; by default the Redis server's clock is used.
    """

    def __init__(
        self,
        client: Any,
        strategy: Strategy,
        *,
        limit: Optional[int] = None,
        window: Optional[float] = None,
        capacity: Optional[float] = None,
        refill_rate: Optional[float] = None,
        prefix: str = "rl",
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.client = client
        self.strategy = Strategy(strategy)
        self.prefix = prefix
        self._clock = clock

        if self.strategy is Strategy.SLIDING_WINDOW:
            if not limit or limit <= 0 or not window or window <= 0:
                raise ValueError("sliding window needs limit > 0 and window > 0")
            self.limit, self.window = int(limit), float(window)
            self._script, self._max_cost = SLIDING_WINDOW_LUA, self.limit
            self._params = (self.limit, repr(self.window))
        else:
            if not capacity or capacity <= 0 or not refill_rate or refill_rate <= 0:
                raise ValueError("token bucket needs capacity > 0 and refill_rate > 0")
            self.capacity, self.refill_rate = float(capacity), float(refill_rate)
            self._script, self._max_cost = TOKEN_BUCKET_LUA, self.capacity
            self._params = (repr(self.capacity), repr(self.refill_rate))
        self._sha = _sha(self._script)

    # Keys for one (client, resource) share a {hash tag}, so in Redis Cluster
    # they land in the same slot and the script can touch them together.
    def _base(self, client_id: Hashable, resource_id: Hashable) -> str:
        return f"{self.prefix}:{{{client_id}|{resource_id}}}"

    def _keys(self, client_id: Hashable, resource_id: Hashable) -> list:
        b = self._base(client_id, resource_id)
        state = [f"{b}:log", f"{b}:meta"] if self.strategy is Strategy.SLIDING_WINDOW else [f"{b}:bucket"]
        return state + [f"{b}:stats", f"{self.prefix}:global:stats"]

    def _run(self, keys: list, args: list) -> Any:
        try:
            return self.client.evalsha(self._sha, len(keys), *keys, *args)
        except Exception as exc:  # first call on this server: script not cached yet
            if "NOSCRIPT" not in str(exc):
                raise
            return self.client.eval(self._script, len(keys), *keys, *args)

    def allow(self, client_id: Hashable, resource_id: Hashable, cost: int = 1) -> Decision:
        if cost <= 0:
            raise ValueError("cost must be positive")
        if cost > self._max_cost:
            raise ValueError(f"cost {cost} exceeds maximum capacity {self._max_cost}; "
                             "it could never be accepted")
        now = repr(float(self._clock())) if self._clock else ""
        ok, remaining, retry = self._run(self._keys(client_id, resource_id),
                                         [*self._params, int(cost), now])
        return Decision(bool(int(ok)), int(_s(remaining)), float(_s(retry)))

    def stats(self, client_id: Hashable = None, resource_id: Hashable = None) -> Stats:
        if client_id is None and resource_id is None:
            key = f"{self.prefix}:global:stats"
        else:
            key = f"{self._base(client_id, resource_id)}:stats"
        raw = {_s(k): int(_s(v)) for k, v in (self.client.hgetall(key) or {}).items()}
        return Stats(raw.get("total", 0), raw.get("allowed", 0), raw.get("rejected", 0))

    def reset(self, client_id: Hashable, resource_id: Hashable) -> None:
        self.client.delete(*self._keys(client_id, resource_id)[:-1])

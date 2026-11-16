# llm-rate-limiter

A rate limiter for LLM API gateways, where many clients share limited model
capacity. It has two algorithms (**sliding window** and **token bucket**) and
two backends: **in-memory** for a single process, and **Redis** for many
gateway instances that need to share one limit.

Every request gets a decision, the remaining capacity, and how long until it
would succeed:

```python
from ratelimiter import RateLimiter, Strategy

rl = RateLimiter(Strategy.TOKEN_BUCKET, capacity=20, refill_rate=5)   # 20 burst, 5/s sustained

d = rl.allow("client-42", "gpt-large", cost=3)   # cost = weight of this request
if not d.allowed:
    return 429, {"Retry-After": round(d.retry_after, 2)}
print(d.remaining)                               # whole units left right now
print(rl.stats("client-42", "gpt-large"))        # Stats(total=…, allowed=…, rejected=…)
```

The same code runs across servers by swapping in the Redis backend:

```python
import redis
from ratelimiter import RedisRateLimiter, Strategy

rl = RedisRateLimiter(redis.Redis(), Strategy.SLIDING_WINDOW, limit=100, window=60)
```

There are no runtime dependencies. The Redis backend takes any client with
redis-py's interface.

## What it guarantees

| Property | How |
|---|---|
| Never admits more than the limit, even under concurrency | Each decision is one critical section: a per-key lock (memory) or one Lua script (Redis) |
| Clients and resources are isolated | State is keyed by `(client_id, resource_id)`; one hot client can't slow another |
| Statistics always match the decisions | Counters are updated inside the same critical section as the decision |
| Rejected requests cost nothing | Only admitted requests are recorded or deduct tokens |
| Accurate `retry_after`, including weighted requests | Sliding window walks the oldest events until enough cost would expire; token bucket computes `(cost − tokens) / rate` |
| Safe when the clock jumps | Uses monotonic time (memory) or the Redis server clock (Redis); a backwards step is ignored |
| Requests that could never fit fail loudly | `cost > capacity` raises `ValueError` instead of being rejected forever |

## Design decisions

**Exact sliding log rather than an approximate counter.** The sliding window
stores one entry per admitted request (with its cost), so "at most N in any
rolling W seconds" holds exactly, including at window edges, where a fixed
window lets through up to 2N. The cost is memory that grows with the limit for
each key. For very high limits, a two-bucket sliding counter would trade
exactness for O(1) memory (see *Future work*).

**A lock per key, not one global lock.** A global lock would make every
client wait on every other client. Each `(client, resource)` key has its own
`threading.Lock`. A short registry lock is held only when a new key is first
created, with double-checked creation so the common path takes no shared lock.
In the benchmark, the "spread" workload (every thread on its own key) runs as
fast as the "hot" one (all threads on one key).

**The time is read inside the lock.** Reading the clock before acquiring the
lock would let a thread that read an earlier time commit after a later one,
which breaks the event log's ordering. Reading inside the lock keeps time
monotonic for each key.

**Lua scripts for the distributed backend.** A read-modify-write over the
network (GET tokens → compute → SET) has a race between servers. Each decision
is a Lua script that Redis runs atomically, so expire → decide → write → count
is one indivisible step across all gateway processes. Scripts are called with
`EVALSHA` and fall back to `EVAL` on the first `NOSCRIPT`.

**Redis's clock, not the gateways'.** The scripts call `TIME` on the Redis
server, so clock drift between gateway machines can't give one server extra
capacity.

**Keys clean themselves up.** Every Redis key gets a TTL equal to how long its
state stays meaningful: the window length for sliding window, and the time to
refill completely for token bucket (an expired bucket is the same as a full
one). Idle clients cost no Redis memory. The keys for one `(client, resource)`
share a `{hash tag}`, so in Redis Cluster they map to the same slot.

## Testing

```bash
python -m unittest discover -s tests -t .
```

There are 27 tests. The Redis tests start a throwaway `redis-server` on a free
port, or use `REDIS_PORT` if it is set (as CI does).

- **Algorithm tests** use an injected fake clock, so every expiry and refill
  can be checked to exact timestamps: partial recovery, weighted costs, the
  refill cap, and a clock that goes backwards.
- **Concurrency:** 32 threads send 6,400 requests at a limit of 100, and
  exactly 100 are admitted. The stats add up to the number of calls.
- **Parity:** the same 2,000-step random workload (random timing, keys and
  costs) runs through both backends, which must make identical decisions,
  remaining counts, retry times and stats at every step. This proves the Lua
  is a faithful port of the Python.
- **Distributed:** 4 separate OS processes, each with its own Redis connection,
  send 800 requests at one shared limit of 150, and exactly 150 are admitted.

## Benchmark

```bash
python -m bench.benchmark
```

Decisions per second on Python 3.13, a 2-vCPU cloud sandbox, with Redis 7.0 on
the same machine. Full output is in [`bench/results.txt`](bench/results.txt).

| Backend | Strategy | Workload | 1 thread | 4 threads | 16 threads |
|---|---|---|---:|---:|---:|
| memory | sliding window | hot key | 839,750 | 878,250 | 835,692 |
| memory | sliding window | spread keys | 895,056 | 884,376 | 861,467 |
| memory | token bucket | hot key | 879,851 | 881,156 | 813,334 |
| memory | token bucket | spread keys | 927,877 | 893,247 | 793,032 |
| redis | sliding window | hot key | 16,071 | 32,547 | 43,598 |
| redis | token bucket | hot key | 14,694 | 41,957 | 28,555 |

How to read it:

- **In-memory** handles about 0.8–0.9 million decisions per second, and
  throughput stays flat from 1 to 16 threads instead of collapsing under lock
  contention. That is about 1 µs of overhead per request, which is negligible
  next to an LLM call.
- **Redis** is limited by network round-trips, at one round-trip per decision.
  Throughput rises with concurrency as round-trips overlap. On this 2-CPU box
  the Redis server competes with the benchmark threads for CPU, which explains
  the noise at 16 threads. Expect higher numbers on separate hardware.

## Limitations and future work

- The in-memory backend never evicts idle keys. A gateway with millions of
  short-lived clients would need a background sweep or an LRU bound.
- The global stats key sits outside the per-key hash tag. On Redis Cluster,
  disable it or aggregate per-slot counters instead.
- Sliding-log memory grows with the limit. Add a `SLIDING_COUNTER` strategy
  (weighted current and previous window) for very high limits.
- Add LLM-specific cost: charge by tokens (prompt + completion) and settle the
  difference after the response, since output length isn't known in advance.
- Return standard `RateLimit-*` response headers and add an async (`asyncio`)
  API.

## Layout

```
ratelimiter/
  memory.py          in-memory limiter (both strategies, per-key locking, stats)
  redis_backend.py   Redis limiter (Lua scripts, server clock, TTL cleanup)
tests/
  test_memory.py     algorithm + thread-safety tests
  test_redis.py      Redis tests, parity test, multi-process test
  mini_redis.py      dependency-free Redis client and test server launcher
bench/
  benchmark.py       throughput benchmark
  results.txt        results quoted above
```

## License

MIT © 2026 Aryan Sharma

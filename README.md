# ratekeeper

[![tests](https://github.com/zensharma0512-a11y/ratekeeper/actions/workflows/ci.yml/badge.svg)](https://github.com/zensharma0512-a11y/ratekeeper/actions/workflows/ci.yml)

**Per-tenant request quotas for AI model APIs.** When many customers call the
same expensive models, one runaway script shouldn't starve everyone else.
ratekeeper gives each tenant its own budget per model, enforces it with either
of two classic throttling algorithms, and tells the caller exactly when to try
again. Run it inside one process, or across a whole fleet of servers through
Redis.

```python
from ratekeeper import RateLimiter, Strategy

quota = RateLimiter(Strategy.TOKEN_BUCKET, capacity=20, refill_rate=5)  # bursts of 20, 5/s steady

v = quota.allow("acme-corp", "model-large", cost=3)   # heavier calls can cost more
if not v.allowed:
    return 429, {"Retry-After": round(v.retry_after, 2)}
v.remaining                                           # budget left after this call
quota.stats("acme-corp", "model-large")               # Stats(total, allowed, rejected)
```

To share one budget across every server, pass a Redis client:

```python
import redis
from ratekeeper import RedisRateLimiter, Strategy

quota = RedisRateLimiter(redis.Redis(), Strategy.SLIDING_WINDOW, limit=100, window=60)
```

Pure Python standard library; the Redis version works with any redis-py-style client.

## Two algorithms, two personalities

- **Token bucket**: tolerant of bursts. A tenant can spend its saved-up
  budget at once, then is held to the steady refill rate. Good for
  interactive users.
- **Sliding window (exact log)**: strict. No more than *N* units in *any*
  rolling *W*-second span, even across window edges, where a naive fixed
  window would let through up to 2N. Good for hard contractual caps.

## Promises the code keeps

1. **No overspending, ever.** Each verdict happens in one locked step (a lock
   per tenant-model pair in-process, one Lua script in Redis), so concurrent
   calls can't both take the last unit.
2. **Tenants can't hurt each other.** Budgets are keyed by `(tenant, model)`,
   and so are the locks.
3. **Counters tell the truth.** Allowed/rejected totals update in the same
   step as the verdict.
4. **Refusals are free.** A rejected call spends nothing and doesn't push the
   retry time further out.
5. **`retry_after` is exact.** It's computed, not guessed, even for calls that
   cost more than 1.
6. **Clock trouble is contained.** Monotonic time in-process; Redis's own
   clock across servers; a clock that steps backwards is ignored.
7. **Impossible requests fail loudly.** A cost larger than the whole budget
   raises an error instead of being refused forever.

## Engineering notes

**Fine-grained locking.** A single global lock would make every tenant queue
behind every other tenant. Instead, each pair gets its own lock, created
lazily behind a double-checked registry, so the hot path never touches shared
state. In the benchmark below, one tenant hammering one model performs the
same as many tenants spread across many keys.

**Timestamps are taken under the lock.** Read the clock before locking and a
thread holding an older time can commit after a newer one, scrambling the
event log. Reading it inside the lock keeps each key's history in order.

**Atomic Lua instead of read-then-write.** Across servers, "fetch budget,
decide, store budget" is a race. Each verdict is a Lua script, which Redis
runs without interruption, so expiring, deciding, writing and counting are a
single step from every server's point of view. Calls use `EVALSHA` and fall
back to `EVAL` the first time a server hasn't cached the script yet.

**Self-cleaning keys.** Every Redis key expires once its contents stop
mattering: after one window for the sliding log, after a full refill for the
bucket (a vanished bucket and a full bucket behave identically). Idle tenants
cost no memory. A tenant's keys share a `{hash tag}` so they stay in one
Redis Cluster slot.

**The trade-off I accepted.** The exact sliding log stores one entry per
admitted call, so its memory grows with the limit. That buys precision; for
very large limits an approximate two-bucket counter would be the better
choice (see the roadmap).

## How it's verified

```bash
python -m unittest discover -s tests -t .
```

27 tests, run on every push against Python 3.10, 3.12 and 3.13 with a real
Redis server.

- **Deterministic timing.** A fake clock is injected, so expiry and refill are
  checked to the exact second: budget trickling back, weighted calls, the
  refill ceiling, a clock jumping backwards.
- **Thread storm.** 32 threads fire 6,400 calls at a budget of 100, and
  exactly 100 succeed, with counters that add up.
- **Twin check.** One 2,000-step randomized workload is replayed against both
  versions, which must agree on every verdict, balance, retry time and
  counter. This is the proof that the Lua matches the Python.
- **Multi-process.** 4 separate OS processes, each with its own Redis
  connection, send 800 calls at one shared budget of 150, and exactly 150
  succeed.

## Performance

```bash
python -m bench.benchmark
```

Verdicts per second; Python 3.13 on a 2-vCPU cloud VM, Redis 7.0 on the same
machine. Raw output: [`bench/results.txt`](bench/results.txt).

| Version | Algorithm | Load | 1 thread | 4 threads | 16 threads |
|---|---|---|---:|---:|---:|
| in-process | sliding window | one hot key | 839,750 | 878,250 | 835,692 |
| in-process | sliding window | key per thread | 895,056 | 884,376 | 861,467 |
| in-process | token bucket | one hot key | 879,851 | 881,156 | 813,334 |
| in-process | token bucket | key per thread | 927,877 | 893,247 | 793,032 |
| Redis | sliding window | one hot key | 16,071 | 32,547 | 43,598 |
| Redis | token bucket | one hot key | 14,694 | 41,957 | 28,555 |

The in-process engine sustains roughly 850K verdicts/s, about a microsecond
each, and holds steady from 1 to 16 threads instead of degrading under lock
contention. The Redis engine is bound by network round-trips (one per
verdict) and climbs as more of them overlap. On this small box Redis competes
with the benchmark for CPU, hence the noisy 16-thread column.

## Roadmap

- Evict idle tenants from the in-process engine (TTL sweep or LRU cap).
- Make the fleet-wide counters Redis Cluster friendly (per-slot aggregation).
- A `SLIDING_COUNTER` algorithm for O(1) memory at very large limits.
- Charge by model tokens rather than calls, reconciling after the response
  arrives since output length isn't known upfront.
- Emit standard `RateLimit-*` headers; add an `asyncio` API.

## Repository map

```
ratekeeper/
  memory.py          in-process engine
  redis_backend.py   fleet engine (Lua, server clock, auto-expiry)
tests/
  test_memory.py     algorithm and thread tests
  test_redis.py      Redis, twin and multi-process tests
  mini_redis.py      zero-dependency Redis client + throwaway server for tests
bench/
  benchmark.py       throughput benchmark
  results.txt        raw numbers behind the table above
```

## License

MIT © 2026 Aryan Sharma

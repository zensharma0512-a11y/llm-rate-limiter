"""
Throughput benchmark.

    python -m bench.benchmark            # from the repo root

Measures decisions per second for each backend and strategy, at several
thread counts, in two workloads:
  * hot   - every thread hits the same (client, resource) key: worst-case contention
  * spread - each thread uses its own key: shows per-key locks don't serialize clients
Limits are set high enough that most requests are admitted, so the numbers
measure the full accept path (state update + stats), not the cheap reject path.
"""

import argparse
import platform
import threading
import time

from ratekeeper import RateLimiter, RedisRateLimiter, Strategy
from tests.mini_redis import MiniRedis, RedisServer

CONFIGS = {
    Strategy.SLIDING_WINDOW: dict(limit=10_000_000, window=3600),
    Strategy.TOKEN_BUCKET: dict(capacity=10_000_000, refill_rate=1_000_000),
}


def run(make_limiter, threads, per_thread, spread):
    limiters = [make_limiter() for _ in range(threads)]
    barrier = threading.Barrier(threads + 1)

    def worker(i):
        rl = limiters[i]
        key = (f"client{i}" if spread else "client", "model-a")
        barrier.wait()
        for _ in range(per_thread):
            rl.allow(*key)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    [t.start() for t in ts]
    barrier.wait()
    t0 = time.perf_counter()
    [t.join() for t in ts]
    return threads * per_thread / (time.perf_counter() - t0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", default="1,4,16")
    ap.add_argument("--n", type=int, default=20_000, help="requests per thread (memory)")
    ap.add_argument("--redis-n", type=int, default=3_000, help="requests per thread (redis)")
    args = ap.parse_args()
    thread_counts = [int(x) for x in args.threads.split(",")]

    print(f"Python {platform.python_version()} on {platform.machine()}, "
          f"{__import__('os').cpu_count()} CPUs\n")
    print(f"{'backend':8} {'strategy':15} {'workload':7} " +
          " ".join(f"{t:>3} thr  " for t in thread_counts))
    print("-" * (34 + 10 * len(thread_counts)))

    for strategy, cfg in CONFIGS.items():
        for spread in (False, True):
            shared = RateLimiter(strategy, **cfg)
            rates = [run(lambda: shared, t, args.n, spread) for t in thread_counts]
            print(f"{'memory':8} {strategy.value:15} {'spread' if spread else 'hot':7} " +
                  " ".join(f"{r:>9,.0f}" for r in rates))

    with RedisServer() as srv:
        clients = []

        def make(strategy, cfg):
            def f():
                c = MiniRedis(port=srv.port)
                clients.append(c)
                return RedisRateLimiter(c, strategy, **cfg)
            return f

        for strategy, cfg in CONFIGS.items():
            for spread in (False, True):
                MiniRedis(port=srv.port).flushdb()
                rates = [run(make(strategy, cfg), t, args.redis_n, spread) for t in thread_counts]
                print(f"{'redis':8} {strategy.value:15} {'spread' if spread else 'hot':7} " +
                      " ".join(f"{r:>9,.0f}" for r in rates))
        for c in clients:
            c.close()
    print("\nUnits: decisions per second (higher is better).")


if __name__ == "__main__":
    main()

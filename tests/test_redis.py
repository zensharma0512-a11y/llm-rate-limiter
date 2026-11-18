"""
Tests for the Redis backend against a real redis-server.

Includes a parity test (Redis and in-memory must make identical decisions
for the same request sequence) and a multi-process test (several OS
processes, each with its own connection, sharing one limit).
"""

import multiprocessing as mp
import random
import threading
import unittest

from ratekeeper import RateLimiter, RedisRateLimiter, Strategy
from tests.mini_redis import MiniRedis, RedisServer
from tests.test_memory import FakeClock

_server = RedisServer()


def setUpModule():
    if not _server.available:
        raise unittest.SkipTest("redis-server not installed")
    _server.__enter__()


def tearDownModule():
    _server.__exit__(None, None, None)


def _client():
    return MiniRedis(port=_server.port)


class RedisTestCase(unittest.TestCase):
    def setUp(self):
        self.r = _client()
        self.r.flushdb()
        self.clk = FakeClock()

    def tearDown(self):
        self.r.close()


class RedisSlidingWindowTests(RedisTestCase):
    def make(self, **kw):
        return RedisRateLimiter(self.r, Strategy.SLIDING_WINDOW, limit=3, window=10,
                                clock=self.clk, **kw)

    def test_accepts_up_to_limit_then_rejects(self):
        rl = self.make()
        for remaining in (2, 1, 0):
            d = rl.allow("a", "model-a")
            self.assertEqual((d.allowed, d.remaining), (True, remaining))
        d = rl.allow("a", "model-a")
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 10.0)

    def test_capacity_returns_gradually(self):
        rl = self.make()
        rl.allow("a", "model-a")
        self.clk.advance(4)
        rl.allow("a", "model-a")
        rl.allow("a", "model-a")
        d = rl.allow("a", "model-a")
        self.assertAlmostEqual(d.retry_after, 6.0)
        self.clk.advance(6)
        self.assertTrue(rl.allow("a", "model-a").allowed)
        self.assertFalse(rl.allow("a", "model-a").allowed)

    def test_weighted_cost(self):
        rl = self.make()
        rl.allow("a", "model-a")
        self.clk.advance(2)
        rl.allow("a", "model-a")
        self.clk.advance(2)
        d = rl.allow("a", "model-a", cost=3)
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 8.0)
        self.clk.advance(8)
        self.assertTrue(rl.allow("a", "model-a", cost=3).allowed)

    def test_stats_and_isolation(self):
        rl = RedisRateLimiter(self.r, Strategy.SLIDING_WINDOW, limit=1, window=60, clock=self.clk)
        rl.allow("a", "model-a"); rl.allow("a", "model-a"); rl.allow("a", "model-b"); rl.allow("b", "model-a")
        s = rl.stats("a", "model-a")
        self.assertEqual((s.total, s.allowed, s.rejected), (2, 1, 1))
        g = rl.stats()
        self.assertEqual((g.total, g.allowed, g.rejected), (4, 3, 1))

    def test_reset(self):
        rl = self.make()
        for _ in range(4):
            rl.allow("a", "model-a")
        rl.reset("a", "model-a")
        self.assertEqual(rl.allow("a", "model-a").remaining, 2)

    def test_keys_get_ttl(self):
        rl = self.make()
        rl.allow("a", "model-a")
        ttl = self.r.execute("PTTL", "rl:{a|model-a}:log")
        self.assertTrue(10_000 < ttl <= 11_000)


class RedisTokenBucketTests(RedisTestCase):
    def make(self):
        return RedisRateLimiter(self.r, Strategy.TOKEN_BUCKET, capacity=5, refill_rate=2,
                                clock=self.clk)

    def test_burst_refill_and_cap(self):
        rl = self.make()
        for _ in range(5):
            self.assertTrue(rl.allow("a", "model-a").allowed)
        d = rl.allow("a", "model-a")
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 0.5)
        self.clk.advance(0.75)
        d = rl.allow("a", "model-a")
        self.assertEqual((d.allowed, d.remaining), (True, 0))
        self.clk.advance(1000)
        self.assertEqual(rl.allow("a", "model-a").remaining, 4)

    def test_backwards_clock(self):
        rl = self.make()
        rl.allow("a", "model-a")
        self.clk.advance(-50)
        self.assertEqual(rl.allow("a", "model-a").remaining, 3)

    def test_server_clock_used_by_default(self):
        rl = RedisRateLimiter(self.r, Strategy.TOKEN_BUCKET, capacity=2, refill_rate=0.001)
        self.assertTrue(rl.allow("a", "model-a").allowed)
        self.assertTrue(rl.allow("a", "model-a").allowed)
        d = rl.allow("a", "model-a")
        self.assertFalse(d.allowed)
        self.assertGreater(d.retry_after, 900)


class ParityTests(RedisTestCase):
    """Same random workload through both backends -> identical decisions."""

    def _compare(self, strategy, **cfg):
        rng = random.Random(42)
        mem_clk, red_clk = FakeClock(), FakeClock()
        mem = RateLimiter(strategy, clock=mem_clk, **cfg)
        red = RedisRateLimiter(self.r, strategy, clock=red_clk, **cfg)
        for step in range(2000):
            dt = rng.choice([0, 0, 0, 0.25, 0.5, 1, 3])
            mem_clk.advance(dt); red_clk.advance(dt)
            key = (rng.choice("ab"), rng.choice(["model-a", "model-b"]))
            cost = rng.choice([1, 1, 1, 2, 3])
            a, b = mem.allow(*key, cost=cost), red.allow(*key, cost=cost)
            self.assertEqual((a.allowed, a.remaining), (b.allowed, b.remaining), f"step {step}")
            self.assertAlmostEqual(a.retry_after, b.retry_after, places=6, msg=f"step {step}")
        self.assertEqual(mem.stats(), red.stats())

    def test_sliding_window_parity(self):
        self._compare(Strategy.SLIDING_WINDOW, limit=7, window=5)

    def test_token_bucket_parity(self):
        self._compare(Strategy.TOKEN_BUCKET, capacity=6, refill_rate=2)


def hammer_process(port, strategy, cfg, n, start, out):
    """Runs in a separate OS process with its own Redis connection."""
    from ratekeeper import RedisRateLimiter
    from tests.mini_redis import MiniRedis
    c = MiniRedis(port=port)
    rl = RedisRateLimiter(c, strategy, **cfg)
    start.wait()
    out.put(sum(rl.allow("shared", "model-a").allowed for _ in range(n)))
    c.close()


class DistributedConcurrencyTests(RedisTestCase):
    def test_threads_never_overadmit(self):
        rl_cfg = dict(limit=100, window=60)
        admitted, lock = [0], threading.Lock()
        barrier = threading.Barrier(16)

        def worker():
            c = _client()
            rl = RedisRateLimiter(c, Strategy.SLIDING_WINDOW, **rl_cfg)
            barrier.wait()
            n = sum(rl.allow("shared", "model-a").allowed for _ in range(100))
            c.close()
            with lock:
                admitted[0] += n

        ts = [threading.Thread(target=worker) for _ in range(16)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(admitted[0], 100)

    def test_processes_share_one_limit(self):
        ctx = mp.get_context("spawn")
        start, out = ctx.Event(), ctx.Queue()
        for strategy, cfg, expected in [
            (Strategy.SLIDING_WINDOW, dict(limit=150, window=600), 150),
            (Strategy.TOKEN_BUCKET, dict(capacity=150, refill_rate=0.0001), 150),
        ]:
            self.r.flushdb()
            start.clear()
            procs = [ctx.Process(target=hammer_process,
                                 args=(_server.port, strategy, cfg, 200, start, out))
                     for _ in range(4)]
            [p.start() for p in procs]
            start.set()
            total = sum(out.get(timeout=60) for _ in procs)
            [p.join() for p in procs]
            self.assertEqual(total, expected, strategy)
            rl = RedisRateLimiter(self.r, strategy, **cfg)
            g = rl.stats()
            self.assertEqual((g.total, g.allowed), (800, expected))


if __name__ == "__main__":
    unittest.main(verbosity=2)

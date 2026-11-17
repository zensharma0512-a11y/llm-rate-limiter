import threading
import unittest

from ratekeeper import RateLimiter, Strategy


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self.t

    def advance(self, dt):
        with self._lock:
            self.t += dt


class SlidingWindowTests(unittest.TestCase):
    def setUp(self):
        self.clk = FakeClock()
        self.rl = RateLimiter(Strategy.SLIDING_WINDOW, limit=3, window=10, clock=self.clk)

    def test_accepts_up_to_limit_then_rejects(self):
        for remaining in (2, 1, 0):
            d = self.rl.allow("a", "gpt")
            self.assertTrue(d.allowed)
            self.assertEqual(d.remaining, remaining)
        d = self.rl.allow("a", "gpt")
        self.assertFalse(d.allowed)
        self.assertEqual(d.remaining, 0)
        self.assertAlmostEqual(d.retry_after, 10.0)

    def test_capacity_returns_gradually(self):
        self.rl.allow("a", "gpt")
        self.clk.advance(4)
        self.rl.allow("a", "gpt")
        self.rl.allow("a", "gpt")
        d = self.rl.allow("a", "gpt")
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 6.0)   # first event expires at t+10
        self.clk.advance(6)
        d = self.rl.allow("a", "gpt")
        self.assertTrue(d.allowed)                    # only one slot came back
        self.assertEqual(d.remaining, 0)
        self.assertFalse(self.rl.allow("a", "gpt").allowed)

    def test_rejected_requests_do_not_consume(self):
        for _ in range(3):
            self.rl.allow("a", "gpt")
        for _ in range(50):
            self.rl.allow("a", "gpt")
        self.clk.advance(10)
        self.assertEqual(self.rl.allow("a", "gpt").remaining, 2)

    def test_weighted_cost_retry_after(self):
        self.rl.allow("a", "gpt")          # t=0
        self.clk.advance(2)
        self.rl.allow("a", "gpt")          # t=2
        self.clk.advance(2)
        d = self.rl.allow("a", "gpt", cost=3)   # needs both earlier events gone
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 8.0)  # second event expires at 2+10-4


class TokenBucketTests(unittest.TestCase):
    def setUp(self):
        self.clk = FakeClock()
        self.rl = RateLimiter(Strategy.TOKEN_BUCKET, capacity=5, refill_rate=2, clock=self.clk)

    def test_burst_then_reject(self):
        for i in range(5):
            self.assertTrue(self.rl.allow("a", "gpt").allowed)
        d = self.rl.allow("a", "gpt")
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 0.5)

    def test_continuous_refill_and_cap(self):
        for _ in range(5):
            self.rl.allow("a", "gpt")
        self.clk.advance(0.75)              # 1.5 tokens
        d = self.rl.allow("a", "gpt")
        self.assertTrue(d.allowed)
        self.assertEqual(d.remaining, 0)    # 0.5 left, floored
        d = self.rl.allow("a", "gpt")
        self.assertFalse(d.allowed)
        self.assertAlmostEqual(d.retry_after, 0.25)
        self.clk.advance(1000)              # never exceeds capacity
        self.assertEqual(self.rl.allow("a", "gpt").remaining, 4)

    def test_clock_going_backwards_is_safe(self):
        self.rl.allow("a", "gpt")
        self.clk.advance(-50)
        d = self.rl.allow("a", "gpt")
        self.assertTrue(d.allowed)
        self.assertEqual(d.remaining, 3)

    def test_cost_larger_than_capacity_raises(self):
        with self.assertRaises(ValueError):
            self.rl.allow("a", "gpt", cost=6)


class IsolationAndStatsTests(unittest.TestCase):
    def test_clients_and_resources_are_isolated(self):
        clk = FakeClock()
        rl = RateLimiter(Strategy.SLIDING_WINDOW, limit=1, window=60, clock=clk)
        self.assertTrue(rl.allow("a", "gpt").allowed)
        self.assertFalse(rl.allow("a", "gpt").allowed)
        self.assertTrue(rl.allow("a", "claude").allowed)
        self.assertTrue(rl.allow("b", "gpt").allowed)
        s = rl.stats("a", "gpt")
        self.assertEqual((s.total, s.allowed, s.rejected), (2, 1, 1))
        g = rl.stats()
        self.assertEqual((g.total, g.allowed, g.rejected), (4, 3, 1))

    def test_bad_config(self):
        with self.assertRaises(ValueError):
            RateLimiter(Strategy.SLIDING_WINDOW, limit=0, window=1)
        with self.assertRaises(ValueError):
            RateLimiter(Strategy.TOKEN_BUCKET, capacity=1)


class ConcurrencyTests(unittest.TestCase):
    def _hammer(self, rl, threads=32, per_thread=200, keys=(("a", "gpt"),)):
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(threads)

        def worker(i):
            barrier.wait()
            local = []
            for j in range(per_thread):
                k = keys[(i + j) % len(keys)]
                local.append((k, rl.allow(*k).allowed))
            with lock:
                results.extend(local)

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        return results

    def test_sliding_window_never_overadmits(self):
        rl = RateLimiter(Strategy.SLIDING_WINDOW, limit=100, window=60, clock=FakeClock())
        res = self._hammer(rl)
        self.assertEqual(sum(ok for _, ok in res), 100)
        g = rl.stats()
        self.assertEqual(g.total, len(res))
        self.assertEqual(g.allowed, 100)
        self.assertEqual(g.allowed + g.rejected, g.total)

    def test_token_bucket_never_overadmits(self):
        rl = RateLimiter(Strategy.TOKEN_BUCKET, capacity=50, refill_rate=1, clock=FakeClock())
        res = self._hammer(rl)
        self.assertEqual(sum(ok for _, ok in res), 50)

    def test_many_keys_concurrently(self):
        keys = [(c, r) for c in "abcd" for r in ("gpt", "claude")]
        rl = RateLimiter(Strategy.SLIDING_WINDOW, limit=10, window=60, clock=FakeClock())
        res = self._hammer(rl, keys=keys)
        for k in keys:
            self.assertEqual(sum(ok for kk, ok in res if kk == k), 10)
            s = rl.stats(*k)
            self.assertEqual(s.allowed, 10)
            self.assertEqual(s.total, sum(1 for kk, _ in res if kk == k))

    def test_real_clock_refill_under_load(self):
        rl = RateLimiter(Strategy.TOKEN_BUCKET, capacity=5, refill_rate=1000)
        res = self._hammer(rl, threads=8, per_thread=500)
        g = rl.stats()
        self.assertEqual(g.total, 4000)
        self.assertEqual(g.allowed + g.rejected, 4000)
        self.assertGreater(g.allowed, 5)  # refill actually happened


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""
In-memory rate limiter for an LLM API gateway (single process).

Two strategies:
  * SlidingWindow  - exact sliding log: at most `limit` units of cost in any
                     rolling `window` seconds.
  * TokenBucket    - bucket of `capacity` tokens refilled continuously at
                     `refill_rate` tokens/second.

State is kept independently per (client_id, resource_id). Each key has its own
lock, so unrelated clients never contend; a short registry lock guards only
key creation. Statistics are updated inside the same critical section as the
decision, so they always agree with what callers observed.
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, Dict, Hashable, Optional, Tuple

Clock = Callable[[], float]
Key = Tuple[Hashable, Hashable]


class Strategy(str, Enum):
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: int          # whole units still available right after this call
    retry_after: float      # seconds until this same request could succeed (0 if allowed)


@dataclass
class Stats:
    total: int = 0
    allowed: int = 0
    rejected: int = 0

    def record(self, ok: bool) -> None:
        self.total += 1
        if ok:
            self.allowed += 1
        else:
            self.rejected += 1

    def snapshot(self) -> "Stats":
        return Stats(self.total, self.allowed, self.rejected)


# --------------------------------------------------------------------------- #
# Per-key state objects. They are NOT thread-safe on their own; the limiter
# calls them while holding the key's lock.
# --------------------------------------------------------------------------- #

class _SlidingWindowState:
    __slots__ = ("events", "used")

    def __init__(self) -> None:
        self.events: Deque[Tuple[float, int]] = deque()  # (timestamp, cost)
        self.used = 0

    def _expire(self, now: float, window: float) -> None:
        cutoff = now - window
        ev = self.events
        while ev and ev[0][0] <= cutoff:
            _, c = ev.popleft()
            self.used -= c

    def try_acquire(self, now: float, cost: int, limit: int, window: float) -> Decision:
        self._expire(now, window)
        if self.used + cost <= limit:
            self.events.append((now, cost))
            self.used += cost
            return Decision(True, limit - self.used, 0.0)

        # Walk the oldest events until enough capacity would be freed.
        need = self.used + cost - limit
        freed = 0
        retry = 0.0
        for ts, c in self.events:
            freed += c
            if freed >= need:
                retry = max(0.0, ts + window - now)
                break
        return Decision(False, limit - self.used, retry)


class _TokenBucketState:
    __slots__ = ("tokens", "last")

    def __init__(self, capacity: float, now: float) -> None:
        self.tokens = float(capacity)
        self.last = now

    def _refill(self, now: float, capacity: float, rate: float) -> None:
        elapsed = now - self.last
        if elapsed > 0:
            self.tokens = min(capacity, self.tokens + elapsed * rate)
            self.last = now
        # A clock that goes backwards is ignored rather than draining tokens.

    def try_acquire(self, now: float, cost: int, capacity: float, rate: float) -> Decision:
        self._refill(now, capacity, rate)
        if self.tokens >= cost:
            self.tokens -= cost
            return Decision(True, int(math.floor(self.tokens + 1e-9)), 0.0)
        retry = (cost - self.tokens) / rate
        return Decision(False, int(math.floor(self.tokens + 1e-9)), retry)


@dataclass
class _Entry:
    lock: threading.Lock = field(default_factory=threading.Lock)
    state: object = None
    stats: Stats = field(default_factory=Stats)


# --------------------------------------------------------------------------- #
# Public limiter
# --------------------------------------------------------------------------- #

class RateLimiter:
    """
    Sliding window:  RateLimiter(Strategy.SLIDING_WINDOW, limit=100, window=60)
    Token bucket:    RateLimiter(Strategy.TOKEN_BUCKET, capacity=20, refill_rate=5)
    """

    def __init__(
        self,
        strategy: Strategy,
        *,
        limit: Optional[int] = None,
        window: Optional[float] = None,
        capacity: Optional[float] = None,
        refill_rate: Optional[float] = None,
        clock: Clock = time.monotonic,
    ) -> None:
        self.strategy = Strategy(strategy)
        self._clock = clock

        if self.strategy is Strategy.SLIDING_WINDOW:
            if not limit or limit <= 0 or not window or window <= 0:
                raise ValueError("sliding window needs limit > 0 and window > 0")
            self.limit, self.window = int(limit), float(window)
            self._max_cost = self.limit
        else:
            if not capacity or capacity <= 0 or not refill_rate or refill_rate <= 0:
                raise ValueError("token bucket needs capacity > 0 and refill_rate > 0")
            self.capacity, self.refill_rate = float(capacity), float(refill_rate)
            self._max_cost = self.capacity

        self._entries: Dict[Key, _Entry] = {}
        self._registry_lock = threading.Lock()
        self._global_lock = threading.Lock()
        self._global = Stats()

    # -- internals ---------------------------------------------------------- #

    def _entry(self, key: Key) -> _Entry:
        e = self._entries.get(key)          # fast path, no lock
        if e is not None:
            return e
        with self._registry_lock:           # double-checked creation
            e = self._entries.get(key)
            if e is None:
                e = _Entry()
                self._entries[key] = e
            return e

    # -- API ---------------------------------------------------------------- #

    def allow(self, client_id: Hashable, resource_id: Hashable, cost: int = 1) -> Decision:
        if cost <= 0:
            raise ValueError("cost must be positive")
        if cost > self._max_cost:
            raise ValueError(f"cost {cost} exceeds maximum capacity {self._max_cost}; "
                             "it could never be accepted")

        e = self._entry((client_id, resource_id))
        with e.lock:
            now = self._clock()  # read under the lock so time is monotone per key
            if self.strategy is Strategy.SLIDING_WINDOW:
                if e.state is None:
                    e.state = _SlidingWindowState()
                d = e.state.try_acquire(now, cost, self.limit, self.window)
            else:
                if e.state is None:
                    e.state = _TokenBucketState(self.capacity, now)
                d = e.state.try_acquire(now, cost, self.capacity, self.refill_rate)
            e.stats.record(d.allowed)

        with self._global_lock:
            self._global.record(d.allowed)
        return d

    def stats(self, client_id: Hashable = None, resource_id: Hashable = None) -> Stats:
        """Global stats with no args; per-key stats when both ids are given."""
        if client_id is None and resource_id is None:
            with self._global_lock:
                return self._global.snapshot()
        e = self._entries.get((client_id, resource_id))
        if e is None:
            return Stats()
        with e.lock:
            return e.stats.snapshot()

    def reset(self, client_id: Hashable, resource_id: Hashable) -> None:
        """Drop state for one key (its stats too). Global stats are kept."""
        with self._registry_lock:
            self._entries.pop((client_id, resource_id), None)

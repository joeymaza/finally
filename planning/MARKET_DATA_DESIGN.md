# Market Data Backend — Implementation Design

Authoritative design reference for the FinAlly market data subsystem. Covers every module
in `backend/app/market/` with full code, explanations, and examples. Use this as the
implementation guide; the archive documents are superseded by this file.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Data — `seed_prices.py`](#6-seed-data)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming — `stream.py`](#10-sse-streaming)
11. [Package Init — `__init__.py`](#11-package-init)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing](#14-testing)
15. [Error Handling Reference](#15-error-handling-reference)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture Overview

```
MASSIVE_API_KEY set?
    YES → MassiveDataSource  (Polygon.io REST polling, real prices)
    NO  → SimulatorDataSource (GBM simulation, 500ms ticks)

Both sources implement the same MarketDataSource ABC and write into PriceCache.

MarketDataSource
├── SimulatorDataSource → GBMSimulator (numpy, correlated GBM)
└── MassiveDataSource   → asyncio.to_thread → RESTClient.get_snapshot_all()
         │
         ▼
    PriceCache (threading.Lock, version counter)
         │
         ├──→ GET /api/stream/prices  (SSE, EventSource, send-on-change)
         ├──→ GET /api/portfolio      (valuation)
         ├──→ POST /api/portfolio/trade (fill price)
         └──→ GET /api/watchlist      (latest prices)
```

### Key Design Choices

| Choice | Rationale |
|--------|-----------|
| Push model (source → cache) | Decouples timing: simulator at 500ms, Massive at 15s, SSE at 500ms — all independent |
| `threading.Lock` in cache | `asyncio.to_thread` puts Massive polls in real OS threads; `asyncio.Lock` would not protect against that |
| `frozen=True` on `PriceUpdate` | Safe to share across tasks without copying; computed properties stay consistent |
| Version counter on cache | SSE skips sending when nothing changed (avoids redundant payloads during Massive 15s gap) |
| Factory pattern | One env var (`MASSIVE_API_KEY`) selects the source; no other code changes needed |
| SSE over WebSockets | One-way push is all we need; simpler, universal browser support, native `EventSource` reconnect |

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py          Re-exports public API
      models.py            PriceUpdate dataclass
      cache.py             PriceCache (thread-safe in-memory store)
      interface.py         MarketDataSource ABC
      seed_prices.py       SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py         GBMSimulator + SimulatorDataSource
      massive_client.py    MassiveDataSource (Polygon.io REST poller)
      factory.py           create_market_data_source() factory function
      stream.py            SSE endpoint (FastAPI router factory)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

---

## 3. Data Model

**`backend/app/market/models.py`**

`PriceUpdate` is the only data structure that crosses module boundaries. Every consumer
(SSE, portfolio valuation, trade execution) works exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design notes

- **`frozen=True`** — Price updates are immutable value objects. Once created they never
  change, making them safe to share across async tasks without copying or locking.
- **`slots=True`** — Minor memory win; we create thousands of these objects over a session.
- **Computed properties** (`change`, `change_percent`, `direction`) — Derived from the two
  price fields so they can never be stale or inconsistent. No risk of a wrong `direction`
  stored on the object.
- **`to_dict()`** — Single serialization point used by both the SSE endpoint and REST
  responses. Never serialize `PriceUpdate` manually elsewhere.

### Usage examples

```python
from app.market.models import PriceUpdate

# Created by PriceCache.update() — you rarely construct these directly
update = PriceUpdate(ticker="AAPL", price=191.50, previous_price=190.00)

print(update.change)          # 1.5
print(update.change_percent)  # 0.7895
print(update.direction)       # "up"
print(update.to_dict())
# {
#   "ticker": "AAPL", "price": 191.5, "previous_price": 190.0,
#   "timestamp": 1707580800.0, "change": 1.5, "change_percent": 0.7895,
#   "direction": "up"
# }
```

---

## 4. Price Cache

**`backend/app/market/cache.py`**

The price cache is the central data hub. Data sources write to it; SSE streaming and
portfolio valuation read from it. It is the only piece of shared mutable state in the
market data layer.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one active at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why `threading.Lock`, not `asyncio.Lock`?

The Massive API client runs `RESTClient.get_snapshot_all()` inside `asyncio.to_thread()`,
which executes in a real OS thread from the default thread pool executor. An `asyncio.Lock`
only protects against concurrent access within a single event loop thread; it provides no
protection against access from a separate OS thread. `threading.Lock` is the correct choice
here. The lock is held only for the duration of a single dict read or write — microseconds —
so contention is negligible.

### Version counter and SSE change detection

The SSE generator polls the cache every 500ms. Without a version counter it would send all
prices on every tick even if nothing changed (e.g., during Massive's 15s polling gap).
The version counter lets SSE skip redundant sends:

```python
last_version = -1
while True:
    current_version = price_cache.version
    if current_version != last_version:
        last_version = current_version
        # Only serialize and send when something actually changed
        data = price_cache.get_all()
        yield format_sse(data)
    await asyncio.sleep(0.5)
```

### Usage examples

```python
from app.market.cache import PriceCache

cache = PriceCache()

# Write
update = cache.update("AAPL", 190.50)
print(update.direction)           # "flat" (first write)

update2 = cache.update("AAPL", 191.00)
print(update2.direction)          # "up"
print(update2.change)             # 0.5

# Read
latest = cache.get("AAPL")        # PriceUpdate or None
price = cache.get_price("AAPL")   # float or None
all_prices = cache.get_all()      # dict[str, PriceUpdate]

# Watchlist removal
cache.remove("AAPL")
assert cache.get("AAPL") is None

# Change detection
v0 = cache.version
cache.update("AAPL", 192.00)
assert cache.version == v0 + 1
```

---

## 5. Abstract Interface

**`backend/app/market/interface.py`**

Both `SimulatorDataSource` and `MassiveDataSource` implement this ABC. All downstream code
that needs to modify the active ticker set uses this interface — never the concrete types.

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices;
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Must also remove the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Push vs pull design

The source writes to the cache rather than returning prices on demand. This means the SSE
layer never knows or cares which data source is active or what its update cadence is. The
cache is always current; the source handles its own timing. Adding a third data source in
the future (e.g., a WebSocket feed) requires only implementing this interface.

---

## 6. Seed Data

**`backend/app/market/seed_prices.py`**

Constants only — no logic, no imports. Used by `GBMSimulator` for initial prices and GBM
parameters. Also used by `MassiveDataSource` as fallback prices before the first poll.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu:    annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the table above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition.
# Tickers in the same group have higher intra-group correlation.
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Pairwise correlation coefficients
INTRA_TECH_CORR    = 0.6   # Within the tech group
INTRA_FINANCE_CORR = 0.5   # Within the finance group
CROSS_GROUP_CORR   = 0.3   # Between sectors, and for unknown tickers
TSLA_CORR          = 0.3   # TSLA is in the tech set but moves independently
```

### Volatility rationale

| Ticker | sigma | Why |
|--------|-------|-----|
| TSLA | 0.50 | Historically the most volatile large-cap |
| NVDA | 0.40 | High-beta semiconductor play |
| NFLX | 0.35 | Growth stock, earnings-driven swings |
| AMZN | 0.28 | Diversified but still high-growth |
| META | 0.30 | Social media regulatory/ad risk |
| AAPL | 0.22 | Large, mature, lower beta |
| MSFT | 0.20 | Stable enterprise, cloud |
| JPM  | 0.18 | Rate-sensitive but mature bank |
| V    | 0.17 | Payments network, very stable |

---

## 7. GBM Simulator

**`backend/app/market/simulator.py`**

Two classes in one file:

- **`GBMSimulator`** — Pure math engine. Stateful, holds current prices, advances by one
  time step on each `step()` call.
- **`SimulatorDataSource`** — `MarketDataSource` implementation. Wraps `GBMSimulator` in
  an `asyncio` background task and writes to `PriceCache`.

### 7.1 GBM Math

Geometric Brownian Motion produces lognormal price paths. Prices are always positive
(the exponential function can never reach zero), matching real stock behavior.

At each tick:

```
S(t+dt) = S(t) × exp((μ - σ²/2) × dt + σ × √dt × Z)
```

Where:
- `S(t)` = current price
- `μ` (mu) = annualized drift (expected return, e.g. 0.05 for 5%/year)
- `σ` (sigma) = annualized volatility (e.g. 0.22 for 22%/year)
- `dt` = time step as fraction of a trading year
- `Z` = standard normal random variable (correlated across tickers via Cholesky)

For 500ms ticks with 252 trading days × 6.5 hours/day:
```
dt = 0.5 / (252 × 6.5 × 3600) ≈ 8.48 × 10⁻⁸
```

This tiny `dt` produces sub-cent per-tick moves that accumulate naturally into realistic
intraday price paths.

### 7.2 Correlated Moves via Cholesky Decomposition

Real stocks are correlated — tech stocks move together, banks move together. To model
this we:

1. Build an N×N correlation matrix `C` where `C[i,j]` is the pairwise correlation
   between tickers i and j (from `_pairwise_correlation`).
2. Compute the Cholesky factor `L = cholesky(C)`.
3. On each tick, draw N independent standard normals `Z_ind`, then compute
   `Z_corr = L @ Z_ind`. The vector `Z_corr` has the desired correlation structure.
4. Use `Z_corr[i]` as the random term for ticker i.

The matrix is rebuilt whenever tickers are added or removed. With N < 50 tickers this
O(N²) operation is fast enough to be unnoticeable.

### 7.3 GBMSimulator Implementation

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    # 500ms as a fraction of one trading year (252 days × 6.5 h/day × 3600 s/h)
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # ── Public API ──────────────────────────────────────────────────────────

    def step(self) -> dict[str, float]:
        """Advance all tickers one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Avoid allocations inside the loop.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # ~0.1% chance of a sudden 2–5% move per ticker per tick.
            # With 10 tickers at 2 ticks/sec, expect one event roughly every 50 seconds.
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    abs(shock) * 100,
                    "up" if shock > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. Rebuilds the Cholesky matrix. No-op if already tracked."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Rebuilds the Cholesky matrix. No-op if not tracked."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # ── Internals ───────────────────────────────────────────────────────────

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — for use during batch initialization."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky factor of the ticker correlation matrix."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Correlation between two tickers based on sector membership."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### 7.4 SimulatorDataSource Implementation

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task calling GBMSimulator.step() every
    `update_interval` seconds and writing results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Populate cache immediately so SSE has data on its very first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — `start()` populates the cache from seed prices before the loop
  begins. SSE has data to send immediately; no blank-screen delay.
- **Graceful cancellation** — `stop()` cancels and awaits the task, catching
  `CancelledError`. This ensures a clean shutdown during FastAPI lifespan teardown.
- **Exception resilience** — The loop catches exceptions per-step so a transient numpy
  error doesn't kill the entire data feed.
- **`add_ticker` seeds cache immediately** — When a new ticker is added mid-session,
  its seed price goes into the cache right away. The watchlist endpoint can return a
  price in the same request rather than returning `null`.

---

## 8. Massive API Client

**`backend/app/market/massive_client.py`**

Polls the Massive (Polygon.io) REST API. One batch call per interval covers all watched
tickers. The synchronous `RESTClient` runs in `asyncio.to_thread()` to avoid blocking
the event loop.

### 8.1 Massive API Basics

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="your_key")  # or reads MASSIVE_API_KEY env var

# Get snapshot for multiple tickers in one call
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    print(snap.ticker)                          # "AAPL"
    print(snap.last_trade.price)               # 190.50
    print(snap.last_trade.timestamp)           # Unix ms, e.g. 1707580800000
    print(snap.day.previous_close)             # 188.00
    print(snap.day.change_percent)             # 1.33
```

Key facts about the API:
- All tickers fetched in **one call** — critical for staying within free-tier rate limits
- Timestamps are **Unix milliseconds** — divide by 1000 to get Unix seconds
- Outside US market hours, `last_trade.price` is the last traded price (stale is expected)
- Free tier: 5 requests/min → poll every 15s; paid tiers: poll every 2-5s

### 8.2 MassiveDataSource Implementation

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Rate limits:
      Free tier:  5 req/min → poll every 15s (default)
      Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll — cache has data before SSE sends anything
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ── Internals ───────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll cycle: fetch snapshots → update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # RESTClient is synchronous — run in a thread pool to avoid blocking
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — loop retries on next interval.
            # Common causes: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous REST call. Runs in asyncio.to_thread()."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Error handling matrix

| Failure | Behavior |
|---------|----------|
| 401 Unauthorized | Logged. Poller keeps retrying each interval. |
| 429 Rate Limited | Logged. Retries after `poll_interval` seconds. |
| Network timeout | Logged. Retries automatically next cycle. |
| Malformed snapshot | Per-ticker `AttributeError` caught; other tickers still processed. |
| All tickers fail | Cache retains last-known prices; SSE streams stale-but-stable data. |

### Note on `asyncio.to_thread`

The `RESTClient` makes blocking HTTP calls. Running it directly on the event loop would
freeze all SSE streaming, trade execution, and other requests for the duration of the
HTTP call (up to several seconds). `asyncio.to_thread()` moves the blocking call to a
thread pool executor, keeping the event loop free. The result is awaited back on the
event loop.

---

## 9. Factory

**`backend/app/market/factory.py`**

Single function that reads the environment and returns the right implementation.

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the market data source from the environment.

    MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    Otherwise                         → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Typical startup sequence

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)

# Load tickers from the database watchlist
initial_tickers = await db.get_watchlist_tickers()
await source.start(initial_tickers)
```

---

## 10. SSE Streaming

**`backend/app/market/stream.py`**

A FastAPI router factory that creates the `GET /api/stream/prices` SSE endpoint. The
factory pattern injects the `PriceCache` without globals.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a bound reference to the price cache."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator yielding SSE-formatted price events."""
    yield "retry: 1000\n\n"   # Browser EventSource reconnects after 1s on drop

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each event on the wire looks like:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Note the blank line after each `data:` line — that's the SSE event delimiter.

### Frontend consumption

```javascript
const es = new EventSource('/api/stream/prices');

es.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices: { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp } }
    for (const [ticker, update] of Object.entries(prices)) {
        updateWatchlistRow(ticker, update);
        if (ticker === selectedTicker) updateMainChart(update);
    }
};

es.onerror = () => {
    // EventSource auto-reconnects after retry ms (1000ms per the retry directive)
    showConnectionStatus('yellow');
};

es.onopen = () => {
    showConnectionStatus('green');
};
```

### Keepalive

The SSE spec requires servers to send something periodically or proxies will close the
connection. The version-counter design already ensures events flow every 500ms during
simulator operation. For the Massive 15s polling gap, the `asyncio.sleep(interval)` loop
wakes every 500ms but only yields `data:` events when the version changes. During the
gap, no data events are sent — this is fine for modern browsers (EventSource spec has
its own keepalive) but add a `:keepalive\n\n` comment if your deployment uses a proxy
with aggressive idle timeouts.

---

## 11. Package Init

**`backend/app/market/__init__.py`**

Re-exports the complete public API so callers import from `app.market` directly.

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate              — Immutable price snapshot dataclass
    PriceCache               — Thread-safe in-memory price store
    MarketDataSource         — Abstract interface for data providers
    create_market_data_source — Factory that selects simulator or Massive
    create_stream_router     — FastAPI router factory for the SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

---

## 12. FastAPI Lifecycle Integration

**`backend/app/main.py`** (excerpt)

Use FastAPI's `lifespan` context manager. Create and start the market data system on
startup; stop it on shutdown.

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter, Depends, HTTPException

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Load initial tickers from the database watchlist
    initial_tickers = await db.get_watchlist_tickers()   # e.g. ["AAPL", "GOOGL", ...]
    await source.start(initial_tickers)

    # Register the SSE router (must happen after creating price_cache)
    stream_router = create_stream_router(price_cache)
    app.include_router(stream_router)

    yield   # ── App is running ──────────────────────────────────────────

    # ── Shutdown ─────────────────────────────────────────────────────────
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# ── Dependency helpers ───────────────────────────────────────────────────

def get_price_cache() -> PriceCache:
    return app.state.price_cache

def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Injecting into route handlers

```python
api_router = APIRouter(prefix="/api")


@api_router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    # Read price once at the start of handling — this becomes the fill price
    fill_price = price_cache.get_price(trade.ticker)
    if fill_price is None:
        raise HTTPException(status_code=409, detail={"error": "price_unavailable"})

    # Validate, write trade to DB, update positions, snapshot portfolio ...


@api_router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    price_cache: PriceCache = Depends(get_price_cache),
):
    await db.insert_watchlist_entry(payload.ticker)
    await source.add_ticker(payload.ticker)

    # Simulator seeds cache immediately; Massive waits for next poll
    current = price_cache.get(payload.ticker)
    return {"ticker": payload.ticker, "price": current.price if current else None}


@api_router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)
    # Keep tracking if the user still holds a position (portfolio valuation needs it)
    position = await db.get_position(ticker)
    if not position or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}


@api_router.get("/watchlist")
async def get_watchlist(
    price_cache: PriceCache = Depends(get_price_cache),
):
    tickers = await db.get_watchlist_tickers()
    entries = []
    for ticker in tickers:
        update = price_cache.get(ticker)
        entries.append({
            "ticker": ticker,
            "price": update.price if update else None,
            "direction": update.direction if update else None,
            "change_percent": update.change_percent if update else None,
        })
    return entries
```

---

## 13. Watchlist Coordination

When the watchlist changes (REST or LLM chat), the data source must be kept in sync so
it tracks the right set of tickers.

### Adding a ticker

```
POST /api/watchlist {ticker: "PYPL"}
  1. Validate ticker format (alphanumeric, 1-5 chars)
  2. INSERT into watchlist table (UNIQUE constraint → 409 if duplicate)
  3. await source.add_ticker("PYPL")
       Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache with a price
       Massive:   appends to ticker list, appears on next poll (no immediate price)
  4. Read price_cache.get("PYPL") for the response
  5. Return {ticker, price}  (price may be null for Massive until next poll)
```

### Removing a ticker

```
DELETE /api/watchlist/PYPL
  1. DELETE from watchlist table
  2. Check positions table: does the user hold PYPL shares?
       YES → skip source.remove_ticker (need price for portfolio valuation)
       NO  → await source.remove_ticker("PYPL")
               Both sources: remove from tracker list, cache.remove("PYPL")
  3. Return {status: "ok"}
```

### Position-held edge case

If the user removes PYPL from their watchlist but still holds 50 shares, the price must
remain in the cache for portfolio valuation (`GET /api/portfolio`) and the P&L chart
snapshot task. Do not remove the ticker from the data source in this case.

```python
@api_router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, ...):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if not position or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

---

## 14. Testing

### Project setup

```bash
cd backend
uv sync --extra dev
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app --cov-report=term-missing
uv run --extra dev ruff check app/ tests/
```

### 14.1 Models tests

```python
# tests/market/test_models.py
from app.market.models import PriceUpdate


class TestPriceUpdate:

    def test_direction_up(self):
        u = PriceUpdate(ticker="AAPL", price=191.0, previous_price=190.0)
        assert u.direction == "up"
        assert u.change == 1.0

    def test_direction_down(self):
        u = PriceUpdate(ticker="AAPL", price=189.0, previous_price=190.0)
        assert u.direction == "down"
        assert u.change == -1.0

    def test_direction_flat(self):
        u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0)
        assert u.direction == "flat"

    def test_change_percent(self):
        u = PriceUpdate(ticker="AAPL", price=191.0, previous_price=190.0)
        assert abs(u.change_percent - 0.5263) < 0.001

    def test_change_percent_zero_previous(self):
        u = PriceUpdate(ticker="AAPL", price=100.0, previous_price=0.0)
        assert u.change_percent == 0.0

    def test_to_dict_keys(self):
        u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0)
        d = u.to_dict()
        assert set(d.keys()) == {
            "ticker", "price", "previous_price", "timestamp",
            "change", "change_percent", "direction",
        }

    def test_frozen(self):
        u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0)
        try:
            u.price = 200.0   # Should raise FrozenInstanceError
            assert False, "Should have raised"
        except Exception:
            pass
```

### 14.2 Cache tests

```python
# tests/market/test_cache.py
from app.market.cache import PriceCache


class TestPriceCache:

    def test_first_update_is_flat(self):
        cache = PriceCache()
        u = cache.update("AAPL", 190.0)
        assert u.direction == "flat"
        assert u.previous_price == 190.0

    def test_subsequent_update_tracks_direction(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        u = cache.update("AAPL", 191.0)
        assert u.direction == "up"
        assert u.previous_price == 190.0

    def test_get_returns_none_for_unknown(self):
        cache = PriceCache()
        assert cache.get("NOPE") is None
        assert cache.get_price("NOPE") is None

    def test_get_all_shallow_copy(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        all1 = cache.get_all()
        cache.update("AAPL", 191.0)
        all2 = cache.get_all()
        # all1 should not reflect the second update
        assert all1["AAPL"].price == 190.0
        assert all2["AAPL"].price == 191.0

    def test_remove(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent_is_noop(self):
        cache = PriceCache()
        cache.remove("NOPE")  # Must not raise

    def test_version_increments_on_update(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.0)
        cache.update("AAPL", 191.0)
        assert cache.version == v0 + 2

    def test_version_does_not_increment_on_remove(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        v = cache.version
        cache.remove("AAPL")
        assert cache.version == v  # remove does not bump version

    def test_len_and_contains(self):
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.0)
        assert len(cache) == 1
        assert "AAPL" in cache
        assert "GOOGL" not in cache
```

### 14.3 Simulator unit tests

```python
# tests/market/test_simulator.py
import pytest
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:

    def test_step_returns_all_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_always_positive(self):
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            result = sim.step()
            assert result["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_add_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        result = sim.step()
        assert "TSLA" in result

    def test_add_duplicate_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim.get_tickers()) == 1

    def test_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_remove_nonexistent_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")   # Must not raise

    def test_unknown_ticker_random_seed(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        assert 50.0 <= sim.get_price("ZZZZ") <= 300.0

    def test_empty_step(self):
        sim = GBMSimulator(tickers=[])
        assert sim.step() == {}

    def test_cholesky_exists_with_multiple_tickers(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None       # Single ticker: no matrix needed
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None   # Two tickers: matrix built

    def test_all_ten_default_tickers(self):
        """Cholesky decomposition must succeed for the full default watchlist."""
        tickers = list(SEED_PRICES.keys())
        sim = GBMSimulator(tickers=tickers)
        result = sim.step()
        assert len(result) == 10

    def test_get_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        assert set(sim.get_tickers()) == {"AAPL", "GOOGL"}
```

### 14.4 SimulatorDataSource integration tests

```python
# tests/market/test_simulator_source.py
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


class TestSimulatorDataSource:

    async def test_start_populates_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL", "GOOGL"])
        try:
            # Seed prices must be in the cache before the first loop tick
            assert cache.get("AAPL") is not None
            assert cache.get("GOOGL") is not None
        finally:
            await source.stop()

    async def test_cache_updates_over_time(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])
        try:
            v0 = cache.version
            await asyncio.sleep(0.3)
            assert cache.version > v0
        finally:
            await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()  # Must not raise

    async def test_add_ticker_seeds_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL"])
        try:
            await source.add_ticker("TSLA")
            assert "TSLA" in source.get_tickers()
            assert cache.get("TSLA") is not None
        finally:
            await source.stop()

    async def test_remove_ticker_clears_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL", "TSLA"])
        try:
            await source.remove_ticker("TSLA")
            assert "TSLA" not in source.get_tickers()
            assert cache.get("TSLA") is None
        finally:
            await source.stop()
```

### 14.5 MassiveDataSource tests (mocked)

```python
# tests/market/test_massive.py
from unittest.mock import MagicMock, patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


class TestMassiveDataSource:

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "GOOGL"]

        snapshots = [
            _make_snapshot("AAPL", 190.50, 1707580800000),
            _make_snapshot("GOOGL", 175.25, 1707580800000),
        ]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_timestamp_converted_from_ms_to_seconds(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        snapshots = [_make_snapshot("AAPL", 190.0, 1707580800000)]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update.timestamp == pytest.approx(1707580800.0)

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]

        good = _make_snapshot("AAPL", 190.50, 1707580800000)
        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None   # Will cause AttributeError on .price

        with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network")):
            await source._poll_once()  # Must not raise

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

    async def test_stop_cancels_task(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        # Bypass the real start() which would call _poll_once and need the API
        import asyncio
        source._task = asyncio.create_task(asyncio.sleep(9999), name="test-task")
        await source.stop()
        assert source._task is None
```

### 14.6 Factory tests

```python
# tests/market/test_factory.py
import os
from unittest.mock import patch
from app.market.factory import create_market_data_source
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


class TestFactory:

    def test_no_api_key_returns_simulator(self):
        cache = PriceCache()
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("MASSIVE_API_KEY", None)
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_empty_api_key_returns_simulator(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}):
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_whitespace_api_key_returns_simulator(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_valid_api_key_returns_massive(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "real_key_here"}):
            source = create_market_data_source(cache)
        assert isinstance(source, MassiveDataSource)
```

---

## 15. Error Handling Reference

### Price not in cache at trade time

The Simulator seeds the cache immediately in `add_ticker()`, so this is mainly a concern
with Massive after a watchlist addition (brief gap until next poll).

```python
price = price_cache.get_price(trade.ticker)
if price is None:
    raise HTTPException(
        status_code=409,
        detail={"error": "price_unavailable"},
    )
```

The PLAN.md specifies 409 Conflict for this case. The frontend should display a transient
message ("Waiting for first price…") rather than a permanent error.

### Simulator step exception

The simulator's `_run_loop` catches all exceptions per step. A transient numpy error
(extremely rare) does not kill the background task or affect the cache; the next tick
runs normally.

### Massive poll failure

`_poll_once` catches all exceptions at the top level and logs them. The cache retains
its last-known prices. The poller retries automatically at the next interval. Progressive
failure visibility:

- **< ~1 min** of failures: Cache has stale-but-present prices; SSE streams normally;
  header dot stays green (SSE is connected; it just has no fresh data).
- **> ~1 min** (TBD by frontend): Frontend may flip header dot to yellow/red based on
  the `timestamp` field in SSE payloads becoming stale.

### Empty watchlist

Both data sources handle an empty ticker list gracefully. `start([])` is a no-op for
the loop logic; the cache starts empty; SSE sends no events. The first `add_ticker()`
call starts the data flowing.

---

## 16. Configuration Reference

All tunable parameters with their defaults:

| Parameter | Where | Default | Notes |
|-----------|-------|---------|-------|
| `MASSIVE_API_KEY` | Environment | `""` | If set and non-empty, use Massive; else simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Time between Massive API polls |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock per ticker per tick |
| `dt` | `GBMSimulator.DEFAULT_DT` | `≈ 8.48e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` s | How often the SSE loop checks for cache changes |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser EventSource reconnect delay |

### `pyproject.toml` requirements

```toml
[project]
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.32.0",
    "numpy>=2.0.0",
    "massive>=1.0.0",
    "rich>=13.0.0",      # only for market_data_demo.py
]

[tool.hatch.build.targets.wheel]
packages = ["app"]       # Required for hatchling to find the app package

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

The `massive` package is a hard dependency in `pyproject.toml` because the `MassiveDataSource`
class uses top-level imports. If you want to make it optional (so users without a Massive key
don't need the package), move `from massive import RESTClient` and the
`from massive.rest.models import SnapshotMarketType` import inside `start()` and
`_fetch_snapshots()` respectively. This also affects how tests mock those names — use
`patch.object(source, "_fetch_snapshots", ...)` rather than `patch("...RESTClient")`.

---

*End of document. All code in this file reflects the implementation in `backend/app/market/`.*

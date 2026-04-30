# FinAlly — AI Trading Workstation

## Project Specification

## 1. Vision

FinAlly (Finance Ally) is a visually stunning AI-powered trading workstation that streams live market data, lets users trade a simulated portfolio, and integrates an LLM chat assistant that can analyze positions and execute trades on the user's behalf. It looks and feels like a modern Bloomberg terminal with an AI copilot.

This is the capstone project for an agentic AI coding course. It is built entirely by Coding Agents demonstrating how orchestrated AI agents can produce a production-quality full-stack application. Agents interact through files in `planning/`.

## 2. User Experience

### First Launch

The user runs a single Docker command (or a provided start script). A browser opens to `http://localhost:8000`. No login, no signup. They immediately see:

- A watchlist of 10 default tickers with live-updating prices in a grid
- $10,000 in virtual cash
- A dark, data-rich trading terminal aesthetic
- An AI chat panel ready to assist

### What the User Can Do

- **Watch prices stream** — prices flash green (uptick) or red (downtick) with subtle CSS animations that fade
- **View sparkline mini-charts** — price action beside each ticker in the watchlist, accumulated on the frontend from the SSE stream since page load (sparklines fill in progressively)
- **Click a ticker** to see a larger detailed chart in the main chart area
- **Buy and sell shares** — market orders only, instant fill at current price, no fees, no confirmation dialog
- **Monitor their portfolio** — a heatmap (treemap) showing positions sized by weight and colored by P&L, plus a P&L chart tracking total portfolio value over time
- **View a positions table** — ticker, quantity, average cost, current price, unrealized P&L, % change
- **Chat with the AI assistant** — ask about their portfolio, get analysis, and have the AI execute trades and manage the watchlist through natural language
- **Manage the watchlist** — add/remove tickers manually or via the AI chat

### Visual Design

- **Dark theme**: backgrounds around `#0d1117` or `#1a1a2e`, muted gray borders, no pure black
- **Price flash animations**: brief green/red background highlight on price change, fading over ~500ms via CSS transitions
- **Connection status indicator**: a small colored dot (green = connected, yellow = reconnecting, red = disconnected) visible in the header
- **Professional, data-dense layout**: inspired by Bloomberg/trading terminals — every pixel earns its place
- **Responsive but desktop-first**: optimized for wide screens, functional on tablet

### Color Scheme
- Accent Yellow: `#ecad0a`
- Blue Primary: `#209dd7`
- Purple Secondary: `#753991` (submit buttons)

## 3. Architecture Overview

### Single Container, Single Port

```
┌─────────────────────────────────────────────────┐
│  Docker Container (port 8000)                   │
│                                                 │
│  FastAPI (Python/uv)                            │
│  ├── /api/*          REST endpoints             │
│  ├── /api/stream/*   SSE streaming              │
│  └── /*              Static file serving         │
│                      (Next.js export)            │
│                                                 │
│  SQLite database (volume-mounted)               │
│  Background task: market data polling/sim        │
└─────────────────────────────────────────────────┘
```

- **Frontend**: Next.js with TypeScript, built as a static export (`output: 'export'`), served by FastAPI as static files
- **Backend**: FastAPI (Python), managed as a `uv` project
- **Database**: SQLite, single file at `db/finally.db`, volume-mounted for persistence
- **Real-time data**: Server-Sent Events (SSE) — simpler than WebSockets, one-way server→client push, works everywhere
- **AI integration**: LiteLLM → OpenRouter (Cerebras for fast inference), with structured outputs for trade execution
- **Market data**: Environment-variable driven — simulator by default, real data via Massive API if key provided

### Why These Choices

| Decision | Rationale |
|---|---|
| SSE over WebSockets | One-way push is all we need; simpler, no bidirectional complexity, universal browser support |
| Static Next.js export | Single origin, no CORS issues, one port, one container, simple deployment |
| SQLite over Postgres | No auth = no multi-user = no need for a database server; self-contained, zero config |
| Single Docker container | Students run one command; no docker-compose for production, no service orchestration |
| uv for Python | Fast, modern Python project management; reproducible lockfile; what students should learn |
| Market orders only | Eliminates order book, limit order logic, partial fills — dramatically simpler portfolio math |

---

## 4. Directory Structure

```
finally/
├── frontend/                 # Next.js TypeScript project (static export)
├── backend/                  # FastAPI uv project (Python)
│   └── db/                   # Schema definitions, seed data, migration logic
├── planning/                 # Project-wide documentation for agents
│   ├── PLAN.md               # This document
│   └── ...                   # Additional agent reference docs
├── scripts/
│   ├── start_mac.sh          # Launch Docker container (macOS/Linux)
│   ├── stop_mac.sh           # Stop Docker container (macOS/Linux)
│   ├── start_windows.ps1     # Launch Docker container (Windows PowerShell)
│   └── stop_windows.ps1      # Stop Docker container (Windows PowerShell)
├── test/                     # Playwright E2E tests + docker-compose.test.yml
├── db/                       # Volume mount target (SQLite file lives here at runtime)
│   └── .gitkeep              # Directory exists in repo; finally.db is gitignored
├── Dockerfile                # Multi-stage build (Node → Python)
├── docker-compose.yml        # Optional convenience wrapper
├── .env                      # Environment variables (gitignored, .env.example committed)
└── .gitignore
```

### Key Boundaries

- **`frontend/`** is a self-contained Next.js project. It knows nothing about Python. It talks to the backend via `/api/*` endpoints and `/api/stream/*` SSE endpoints. Internal structure is up to the Frontend Engineer agent.
- **`backend/`** is a self-contained uv project with its own `pyproject.toml`. It owns all server logic including database initialization, schema, seed data, API routes, SSE streaming, market data, and LLM integration. Internal structure is up to the Backend/Market Data agents.
- **`backend/db/`** contains schema SQL definitions and seed logic. The backend lazily initializes the database on first request — creating tables and seeding default data if the SQLite file doesn't exist or is empty.
- **`db/`** at the top level is the runtime volume mount point. The SQLite file (`db/finally.db`) is created here by the backend and persists across container restarts via Docker volume.
- **`planning/`** contains project-wide documentation, including this plan. All agents reference files here as the shared contract.
- **`test/`** contains Playwright E2E tests and supporting infrastructure (e.g., `docker-compose.test.yml`). Unit tests live within `frontend/` and `backend/` respectively, following each framework's conventions.
- **`scripts/`** contains start/stop scripts that wrap Docker commands.

---

## 5. Environment Variables

```bash
# Required: OpenRouter API key for LLM chat functionality
OPENROUTER_API_KEY=your-openrouter-api-key-here

# Optional: Massive (Polygon.io) API key for real market data
# If not set, the built-in market simulator is used (recommended for most users)
MASSIVE_API_KEY=your-massive-api-key-here

# Optional: Set to "true" for deterministic mock LLM responses (testing)
LLM_MOCK=false
```

### Behavior

- If `MASSIVE_API_KEY` is set and non-empty → backend uses Massive REST API for market data
- If `MASSIVE_API_KEY` is absent or empty → backend uses the built-in market simulator
- If `LLM_MOCK=true` → backend returns deterministic mock LLM responses (for E2E tests)
- The backend reads `.env` from the project root (mounted into the container or read via docker `--env-file`)

---

## 6. Market Data

### Two Implementations, One Interface

Both the simulator and the Massive client implement the same abstract interface. The backend selects which to use based on the environment variable. All downstream code (SSE streaming, price cache, frontend) is agnostic to the source.

### Simulator (Default)

- Generates prices using geometric Brownian motion (GBM) with configurable drift and volatility per ticker
- Updates at ~500ms intervals
- Correlated moves across tickers (e.g., tech stocks move together)
- Occasional random "events" — sudden 2-5% moves on a ticker for drama
- Starts from realistic seed prices (e.g., AAPL ~$190, GOOGL ~$175, etc.)
- Runs as an in-process background task — no external dependencies

### Massive API (Optional)

- REST API polling (not WebSocket) — simpler, works on all tiers
- Polls for the union of all watched tickers on a configurable interval
- Free tier (5 calls/min): poll every 15 seconds
- Paid tiers: poll every 2-15 seconds depending on tier
- Parses REST response into the same format as the simulator

### Shared Price Cache

- A single background task (simulator or Massive poller) writes to an in-memory price cache
- The cache holds the latest price, previous price, and timestamp for each ticker
- SSE streams read from this cache and push updates to connected clients
- This architecture supports future multi-user scenarios without changes to the data layer

### SSE Streaming

- Endpoint: `GET /api/stream/prices`
- Long-lived SSE connection; client uses native `EventSource` API
- **Send-on-change, not fixed cadence.** The server pushes a price event only when the cache's version counter advances for a ticker. At ~500ms simulator ticks this still produces frequent updates; on the Massive 15s poll it produces one batch every 15s. In addition, the server emits a `:keepalive` comment every 5 seconds to keep proxies and browsers from timing out the connection.
- Each SSE event contains ticker, price, previous price, timestamp, and change direction
- Client handles reconnection automatically (EventSource has built-in retry)

### Market Hours & Degraded Data

- The simulator runs 24/7 regardless of US market hours.
- The Massive REST path surfaces whatever the provider returns — typically the last trade price, which means prices appear frozen outside US market hours. This is expected behavior, not a bug.
- If the Massive poller fails (network error, rate limit, auth failure), the price cache keeps its last-known values, the poller backs off, and the header connection dot turns **yellow** until the next successful poll. Consecutive failures beyond ~1 minute flip the dot **red**.

### Abstract Interface

The simulator and Massive client both implement `app.market.interface.MarketDataSource` (ABC with `start / stop / add_ticker / remove_ticker / get_tickers`). See `planning/MARKET_DATA_SUMMARY.md` for the full signature and module layout — the market-data subsystem is complete and should be treated as a stable contract by downstream work.

---

## 7. Database

### SQLite with Lazy Initialization

The backend checks for the SQLite database on startup (or first request). If the file doesn't exist or tables are missing, it creates the schema and seeds default data. This means:

- No separate migration step
- No manual database setup
- Fresh Docker volumes start with a clean, seeded database automatically

### Operational Settings

- On initialization the backend sets `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`. WAL mode is required because FastAPI serves requests concurrently and background tasks (snapshot recorder, trade writes, chat writes) share the database. Without WAL, intermittent `database is locked` errors are likely.
- Writes funnel through a single connection/executor (short transactions, one writer); reads may use a separate connection.
- No automatic pruning for MVP: `portfolio_snapshots` (~2.9k rows/day at 30s cadence) and `chat_messages` are kept unbounded. SQLite handles months of data easily at this scale; retention can be added later if size becomes a concern.

### Schema

All tables include a `user_id` column defaulting to `"default"`. This is hardcoded for now (single-user) but enables future multi-user support without schema migration.

**users_profile** — User state (cash balance)
- `id` TEXT PRIMARY KEY (default: `"default"`)
- `cash_balance` REAL (default: `10000.0`)
- `created_at` TEXT (ISO timestamp)

**watchlist** — Tickers the user is watching
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `added_at` TEXT (ISO timestamp)
- UNIQUE constraint on `(user_id, ticker)`

**positions** — Current holdings (one row per ticker per user)
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `quantity` REAL (fractional shares supported)
- `avg_cost` REAL
- `updated_at` TEXT (ISO timestamp)
- UNIQUE constraint on `(user_id, ticker)`

**trades** — Trade history (append-only log)
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `side` TEXT (`"buy"` or `"sell"`)
- `quantity` REAL (fractional shares supported)
- `price` REAL (fill price — the cache value read during request validation, see §8)
- `executed_at` TEXT (ISO timestamp)

**portfolio_snapshots** — Portfolio value over time (for P&L chart). Recorded every 30 seconds by a background task, and immediately after each trade execution. `total_value` is computed from the latest price in the cache at snapshot time plus current cash.
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `user_id` TEXT (default: `"default"`)
- `total_value` REAL
- `recorded_at` TEXT (ISO timestamp)

**chat_messages** — Conversation history with LLM
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `user_id` TEXT (default: `"default"`)
- `role` TEXT (`"user"` or `"assistant"`)
- `content` TEXT
- `actions` TEXT (JSON — trades executed, watchlist changes made; null for user messages)
- `created_at` TEXT (ISO timestamp)

### Position Lifecycle

When a sell brings `quantity` to zero, the row in `positions` is deleted. A subsequent re-buy inserts a fresh row with a new `avg_cost` derived solely from the re-buy trade. This keeps the positions table and heatmap clean, and makes P&L attribution per "open position" unambiguous. Historical cost basis is always reconstructible from `trades` if needed.

### Default Seed Data

- One user profile: `id="default"`, `cash_balance=10000.0`
- Ten watchlist entries: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX

---

## 8. API Endpoints

### Market Data
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/stream/prices` | SSE stream of live price updates |

### Portfolio
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/portfolio` | Current positions, cash balance, total value, unrealized P&L |
| POST | `/api/portfolio/trade` | Execute a trade: `{ticker, quantity, side}` |
| GET | `/api/portfolio/history` | Portfolio value snapshots over time (for P&L chart) |

### Watchlist
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/watchlist` | Current watchlist tickers with latest prices |
| POST | `/api/watchlist` | Add a ticker: `{ticker}` |
| DELETE | `/api/watchlist/{ticker}` | Remove a ticker |

### Chat
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/chat` | Send a message, receive complete JSON response (message + executed actions) |
| GET | `/api/chat/history` | Return the most recent chat messages (both roles) for replay on page load |

### System
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check (for Docker/deployment) |

### Trade Execution Semantics

`POST /api/portfolio/trade` captures the fill price by reading `PriceCache.get(ticker)` **once, at the start of request handling**, before any validation or DB write. That same price is used for (a) cash sufficiency checks, (b) `avg_cost` recomputation, and (c) the `price` column written to `trades`. This single read point makes behavior reproducible under concurrent cache updates and gives tests a clean seam to mock.

If the ticker is unknown to the cache (e.g., freshly added watchlist entry before the first tick), the endpoint returns **409 Conflict** with `{"error": "price_unavailable"}`. The frontend should surface this as a transient state ("waiting for first price") rather than an error.

### Chat Response Semantics

`POST /api/chat` runs with a server-side timeout of **30 seconds** around the LLM call. On timeout or upstream failure the endpoint returns **504 Gateway Timeout** with body `{"error": "llm_unavailable", "detail": "<short reason>"}`. The frontend renders this inline in the chat panel as a single retryable failure bubble; no partial message is stored to `chat_messages`.

---

## 9. LLM Integration

When writing code to make calls to LLMs, use the cerebras-inference skill to call LiteLLM via OpenRouter against the `openrouter/openai/gpt-oss-120b` model with Cerebras as the inference provider. Structured output should be used to interpret the results.

There is an `OPENROUTER_API_KEY` in the `.env` file at the project root.

### How It Works

When the user sends a chat message, the backend:

1. Loads the user's current portfolio context (cash, positions with P&L, watchlist with live prices, total portfolio value)
2. Loads the **last 20 messages** from the `chat_messages` table as conversation history
3. Constructs a prompt with a system message, portfolio context, conversation history, and the user's new message
4. Calls the LLM via LiteLLM → OpenRouter, requesting structured output, using the cerebras-inference skill
5. Parses the complete structured JSON response
6. Auto-executes any trades or watchlist changes specified in the response
7. Stores the message and executed actions in `chat_messages`
8. Returns the complete JSON response to the frontend (no token-by-token streaming — Cerebras inference is fast enough that a loading indicator is sufficient)

### Structured Output Schema

The LLM is instructed to respond with JSON matching this schema:

```json
{
  "message": "Your conversational response to the user",
  "trades": [
    {"ticker": "AAPL", "side": "buy", "quantity": 10}
  ],
  "watchlist_changes": [
    {"ticker": "PYPL", "action": "add"}
  ]
}
```

- `message` (required): The conversational text shown to the user
- `trades` (optional): Array of trades to auto-execute. Each trade goes through the same validation as manual trades (sufficient cash for buys, sufficient shares for sells). `side` is `"buy"` or `"sell"`; `quantity` is a positive number.
- `watchlist_changes` (optional): Array of watchlist modifications. `action` is one of `"add"` or `"remove"` (the only supported values).

### Auto-Execution

Trades specified by the LLM execute automatically — no confirmation dialog. This is a deliberate design choice:
- It's a simulated environment with fake money, so the stakes are zero
- It creates an impressive, fluid demo experience
- It demonstrates agentic AI capabilities — the core theme of the course

**Guardrail:** The system prompt instructs the LLM to emit `trades` only when the user has explicitly asked for a trade or confirmed a prior suggestion in the current turn. Analytical questions ("tell me about Apple", "how is my portfolio?") must not produce `trades`. This is a prompt-level discipline — there is no additional server-side intent check.

If a trade fails validation (e.g., insufficient cash), the error is included in the chat response so the LLM can inform the user.

### System Prompt Guidance

The LLM should be prompted as "FinAlly, an AI trading assistant" with instructions to:
- Analyze portfolio composition, risk concentration, and P&L
- Suggest trades with reasoning
- Execute trades when the user asks or agrees (see Guardrail above)
- Manage the watchlist proactively
- Be concise and data-driven in responses
- Always respond with valid structured JSON

**Draft system prompt** (starting point for the Backend Engineer to iterate):

```
You are FinAlly, an AI assistant embedded in a simulated trading workstation. You have
live read access to the user's cash balance, positions (with unrealized P&L), watchlist,
and latest prices. You reply to every turn with a single JSON object matching this schema:

  {
    "message": string,                               // required, shown verbatim to the user
    "trades": [{"ticker", "side", "quantity"}],      // optional; "side" ∈ {"buy","sell"}
    "watchlist_changes": [{"ticker", "action"}]      // optional; "action" ∈ {"add","remove"}
  }

Rules:
- Emit "trades" ONLY when the user has asked for or confirmed a trade in this turn. Do
  not trade on analytical questions. If unsure, describe the trade in "message" and wait.
- Emit "watchlist_changes" when the user asks to add or remove tickers, or when you
  would genuinely benefit from watching a ticker to answer their next question.
- Be concise and data-driven. Cite numbers from the provided portfolio context.
- Never invent prices, tickers, or positions not present in the context.
- Never exceed available cash (for buys) or available shares (for sells). If the user
  asks for more, say so in "message" and omit the trade.
- The market is simulated; fills are instant at the current price.

The user's current portfolio and watchlist will be provided in the user turn.
```

### LLM Mock Mode

When `LLM_MOCK=true`, the backend returns deterministic mock responses instead of calling OpenRouter. This enables:
- Fast, free, reproducible E2E tests
- Development without an API key
- CI/CD pipelines

The mock responder must honor the same structured output schema so the frontend and the auto-execution pipeline exercise identical code paths.

---

## 10. Frontend Design

### Layout

The frontend is a single-page application with a dense, terminal-inspired layout. The specific component architecture and layout system is up to the Frontend Engineer, but the UI should include these elements:

- **Watchlist panel** — grid/table of watched tickers with: ticker symbol, current price (flashing green/red on change), daily change %, and a sparkline mini-chart (accumulated from SSE since page load — intentionally ephemeral; a page refresh resets sparklines)
- **Main chart area** — larger chart for the currently selected ticker, plotting price over time. Data is accumulated client-side from the SSE stream (same mechanism as sparklines), so the chart starts sparse and fills in as ticks arrive. Clicking a ticker in the watchlist selects it here. No backend historical-price endpoint is required.
- **Portfolio heatmap** — treemap visualization where each rectangle is a position, sized by portfolio weight, colored by P&L (green = profit, red = loss)
- **P&L chart** — line chart showing total portfolio value over time, using data from `portfolio_snapshots`
- **Positions table** — tabular view of all positions: ticker, quantity, avg cost, current price, unrealized P&L, % change
- **Trade bar** — simple input area: ticker field, quantity field, buy button, sell button. Market orders, instant fill.
- **AI chat panel** — docked/collapsible sidebar. Message input, scrolling conversation history, loading indicator while waiting for LLM response. Trade executions and watchlist changes shown inline as confirmations.
- **Header** — portfolio total value (updating live), connection status indicator, cash balance

### Technical Notes

- Use `EventSource` for SSE connection to `/api/stream/prices`
- Canvas-based charting library preferred (Lightweight Charts or Recharts) for performance
- Price flash effect: on receiving a new price, briefly apply a CSS class with background color transition, then remove it
- All API calls go to the same origin (`/api/*`) — no CORS configuration needed
- Tailwind CSS for styling with a custom dark theme

### Page-Load Sequence

On first render the frontend fires these requests in parallel, then opens the SSE stream:

1. `GET /api/portfolio` — cash, positions, total value
2. `GET /api/watchlist` — tickers with latest prices
3. `GET /api/portfolio/history` — snapshots for the P&L chart
4. `GET /api/chat/history` — prior chat messages for the chat panel
5. `EventSource('/api/stream/prices')` — live price updates

The UI renders skeletons until each response resolves. The header connection dot starts yellow, turns green once the first SSE event arrives.

---

## 11. Docker & Deployment

### Multi-Stage Dockerfile

```
Stage 1: Node 20 slim
  - Copy frontend/
  - npm install && npm run build (produces static export)

Stage 2: Python 3.12 slim
  - Install uv
  - Copy backend/
  - uv sync (install Python dependencies from lockfile)
  - Copy frontend build output into a static/ directory
  - Expose port 8000
  - CMD: uvicorn serving FastAPI app
```

FastAPI serves the static frontend files and all API routes on port 8000.

### Docker Volume

The SQLite database persists via a named Docker volume:

```bash
docker run -v finally-data:/app/db -p 8000:8000 --env-file .env finally
```

The `db/` directory in the project root maps to `/app/db` in the container. The backend writes `finally.db` to this path.

### Start/Stop Scripts

**`scripts/start_mac.sh`** (macOS/Linux):
- Builds the Docker image if not already built (or if `--build` flag passed)
- Runs the container with the volume mount, port mapping, and `.env` file
- Prints the URL to access the app
- Optionally opens the browser

**`scripts/stop_mac.sh`** (macOS/Linux):
- Stops and removes the running container
- Does NOT remove the volume (data persists)

**`scripts/start_windows.ps1`** / **`scripts/stop_windows.ps1`**: PowerShell equivalents for Windows.

All scripts should be idempotent — safe to run multiple times.

### Optional Cloud Deployment

The container is designed to deploy to AWS App Runner, Render, or any container platform. A Terraform configuration for App Runner may be provided in a `deploy/` directory as a stretch goal, but is not part of the core build.

---

## 12. Testing Strategy

### Unit Tests (within `frontend/` and `backend/`)

**Backend (pytest)**:
- Market data: simulator generates valid prices, GBM math is correct, Massive API response parsing works, both implementations conform to the abstract interface
- Portfolio: trade execution logic, P&L calculations, edge cases (selling more than owned, buying with insufficient cash, selling at a loss)
- LLM: structured output parsing handles all valid schemas, graceful handling of malformed responses, trade validation within chat flow
- API routes: correct status codes, response shapes, error handling

**Frontend (React Testing Library or similar)**:
- Component rendering with mock data
- Price flash animation triggers correctly on price changes
- Watchlist CRUD operations
- Portfolio display calculations
- Chat message rendering and loading state

### E2E Tests (in `test/`)

**Infrastructure**: A separate `docker-compose.test.yml` in `test/` that spins up the app container plus a Playwright container. This keeps browser dependencies out of the production image.

**Environment**: Tests run with `LLM_MOCK=true` by default for speed and determinism.

**Key Scenarios**:
- Fresh start: default watchlist appears, $10k balance shown, prices are streaming
- Add and remove a ticker from the watchlist
- Buy shares: cash decreases, position appears, portfolio updates
- Sell shares: cash increases, position updates or disappears
- Portfolio visualization: heatmap renders with correct colors, P&L chart has data points
- AI chat (mocked): send a message, receive a response, trade execution appears inline
- SSE resilience: disconnect and verify reconnection

---

## 13. Decision Log

_Recorded 2026-04-24 during doc review. Items here capture choices baked into the sections above, so future agents don't have to rediscover the reasoning._

| Topic | Decision | Lives in |
|---|---|---|
| SSE cadence | Send-on-change via cache version counter; `:keepalive` every 5s | §6 |
| Market hours (Massive) | Provider last-trade price; stale outside hours is expected | §6 |
| Rate-limit / poller failure | Keep last-known prices, back off, header dot yellow → red after ~1 min | §6 |
| Main chart data source | Accumulated client-side from SSE (same as sparklines); no historical endpoint | §10 |
| Sparkline / chart persistence | Ephemeral; page refresh resets them | §2, §10 |
| Chat history bootstrap | `GET /api/chat/history` replayed on page load | §8, §10 |
| Chat context window | Last 20 messages loaded into the LLM prompt | §9 |
| Watchlist action enum | `"add"` \| `"remove"` only | §9 |
| Auto-execution guardrail | Prompt-level: trades only on explicit ask/confirm in current turn | §9 |
| Position lifecycle | Row deleted at `quantity = 0`; re-buy starts fresh `avg_cost` | §7 |
| Trade fill price | Cache read once at request handling start; reused for validation and write | §7, §8 |
| Snapshot `total_value` source | Latest cache prices + current cash, captured at snapshot time | §7 |
| Retention policy | Unbounded for MVP; revisit if size matters | §7 |
| SQLite concurrency | `PRAGMA journal_mode=WAL` + `synchronous=NORMAL`; single-writer discipline | §7 |
| ID strategy | `INTEGER PRIMARY KEY AUTOINCREMENT` for `trades`, `portfolio_snapshots`, `chat_messages` (UUID retained on `users_profile`, `watchlist`, `positions`) | §7 |
| LLM unavailability | 30s server timeout; `504 {"error":"llm_unavailable"}`; no partial persistence | §8 |
| Price unavailable at trade time | `409 {"error":"price_unavailable"}` | §8 |
| ABC interface doc | Linked to `planning/MARKET_DATA_SUMMARY.md` | §6 |
| System prompt | Draft included for Backend Engineer to iterate | §9 |

### Remaining Open Items

- **Docker volume mechanism.** §4 lists `db/` as a repo-level bind-mount target; §11 uses a named volume (`finally-data`). These are inconsistent. Keep as-is for now — to be reconciled when the Docker/deployment work begins. The backend code should not care which is used; it just writes to `/app/db/finally.db`.
- **`LLM_MOCK` env var retained.** Kept separate from `OPENROUTER_API_KEY` presence so tests can exercise the mock path even when a key is configured in the developer's `.env`.


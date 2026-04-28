# DelayKit

[![CI](https://github.com/delaykit/delaykit/actions/workflows/ci.yml/badge.svg)](https://github.com/delaykit/delaykit/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/delaykit.svg)](https://www.npmjs.com/package/delaykit)
[![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](./package.json)
[![types](https://img.shields.io/npm/types/delaykit.svg)](https://www.npmjs.com/package/delaykit)
[![license](https://img.shields.io/npm/l/delaykit.svg)](./LICENSE)

**Durable wake-ups for TypeScript apps and agents.**
Reminders, expirations, retries, debounces, and agent resumes — backed by Postgres or SQLite.

> **Status:** pre-1.0 — minor releases may include breaking changes. See the [changelog](CHANGELOG.md).

## Contents

- [Quick start](#quick-start)
- [What you can build with it](#what-you-can-build-with-it)
- [Deploy to production](#deploy-to-production)
- [Design](#design)
- [How it compares](#how-it-compares)
- [API reference](#api-reference)

## Quick start

```bash
npm install delaykit   # Node
bun add delaykit       # Bun
```

Try it locally with MemoryStore — no database needed:

```typescript
import { DelayKit } from "delaykit";
import { MemoryStore } from "delaykit/memory";
import { PollingScheduler } from "delaykit/polling";

const dk = new DelayKit({
  store: new MemoryStore(), // swap to PostgresStore or SQLiteStore for production
  scheduler: new PollingScheduler(),
});

dk.handle("send-reminder", async ({ key }) => {
  const user = await db.users.find(key);
  if (user.onboarded) return; // already acted, skip
  await sendEmail(user.email, "Complete your profile");
});

await dk.start(); // for serverless (Vercel), use poll() instead — see Deploy to production below

// Send a reminder if the user hasn't onboarded after 24 hours
await dk.schedule("send-reminder", {
  key: "user_123",
  delay: "24h",
});

// User completed onboarding — cancel the reminder
await dk.unschedule("send-reminder", "user_123");
```

MemoryStore is for local development. For jobs that survive restarts, swap in PostgresStore or SQLiteStore — see [Pick a store](#pick-a-store).

Want to run something end-to-end? See [`examples/basic.ts`](examples/basic.ts) (MemoryStore), [`examples/sqlite.ts`](examples/sqlite.ts), and [`examples/postgres.ts`](examples/postgres.ts).

## What you can build with it

### Expire a trial or reservation

```typescript
await dk.schedule("expire-trial", { key: "acct_456", delay: "14d" });

// Or use an absolute time
await dk.schedule("expire-trial", { key: "acct_456", at: trialEndsAt });

// User upgraded — cancel the expiration
await dk.unschedule("expire-trial", "acct_456");
```

### Reindex after a burst of edits

User updates several fields — reindex once after they stop, not on every change.

```typescript
await dk.debounce("reindex", { key: "project_789", wait: "5s" });
```

### Send a follow-up after inactivity

If the user comes back, the timer resets.

```typescript
await dk.schedule("follow-up", {
  key: "user_123",
  delay: "3d",
  onDuplicate: "replace", // resets the timer on each visit
});
```

### Wake an agent after a human-in-the-loop timeout

```typescript
await dk.schedule("approval-timeout", { key: "run_789", delay: "24h" });

// When approval arrives:
await dk.unschedule("approval-timeout", "run_789");
```

If approval doesn't come in time, the handler resumes the agent run with a "timed out" outcome. No polling loop, no idle worker.

### Safe to call from repeated requests

Same handler + same key won't create duplicate jobs. Call schedule from every request — only one pending job exists at a time.

```typescript
await dk.schedule("welcome-email", { key: "user_123", delay: "10m" });
```

## What DelayKit handles for you

- **Jobs survive restarts and deploys** — durable in Postgres or SQLite, not in memory
- **No duplicate jobs** — same handler + key won't create a second pending job
- **Fresh state at execution time** — handlers receive the key and fetch current data, no stale payloads
- **Automatic retries** — failed handlers retry with configurable backoff
- **Stalled job recovery** — crashed processes don't leave stuck jobs
- **Bounded concurrency** — `PollingScheduler` runs at most `maxConcurrent` handlers at once (default 10); the rest stay `pending` in the store and are claimed on subsequent polls
- **Zero runtime dependencies** — `postgres`, `better-sqlite3`, and `@posthook/node` are optional peers; bring whichever store and scheduler you need

### Tuning concurrency

`PollingScheduler` runs at most `maxConcurrent` handlers at a time. Default is `10`. Raise it for I/O-bound handlers, lower it for CPU-heavy ones:

```typescript
new PollingScheduler({ maxConcurrent: 25 });
```

Excess due jobs stay `pending` in the store and are claimed on subsequent polls.

**Cooperative timeouts.** Every handler has a timeout — `30s` by default, or whatever you set via `timeout:`. When the timer fires, DelayKit aborts `ctx.signal` and then waits for the handler to return before releasing its concurrency slot. Pass `signal` through to whatever the handler is calling (most modern Node APIs — `fetch`, `pg`, etc. — accept one) so the handler exits on abort. Handlers that ignore the signal hold their slot until they return on their own:

```typescript
dk.handle("send-email", {
  handler: async ({ key, signal }) => {
    await fetch(`https://api.example.com/send/${key}`, { signal });
  },
  timeout: "10s",
});
```

## Deploy to production

DelayKit has two moving parts — pick each independently based on your infrastructure.

**Store — durable state, source of truth.** The store owns the job rows. Every execution decision (is this job still active? has it already run? what version is current?) reads from the store. A wake-up that arrives twice, one that fires after a process crash, a stale signal from before a job was cancelled — the row decides what actually happens. Implementations: `PostgresStore`, `SQLiteStore`, `MemoryStore` (dev only).

**Scheduler — wake-up signals.** The scheduler decides *when* to claim due jobs. `PollingScheduler` checks the store on an interval. `PosthookScheduler` registers a webhook with [Posthook](https://posthook.io) and receives each job as an HTTP callback at the right time. Wake-ups are disposable: losing one delays a job by at most one poll cycle; a duplicate is harmless because the store has the final say.

The interface between them is small — claim due rows, mark complete or failed, defer if no handler is registered — which is why you can pair them freely. The next two sections cover the supported combinations.

### Pick a store

**SQLite — local-first, zero infra.** For single-process apps — a Bun server, a Node backend on one VPS, a desktop or CLI tool — SQLite is the simplest path. No database service to run, no credentials to manage.

```bash
bun add delaykit                       # Bun: no driver install — bun:sqlite is built in
npm install delaykit better-sqlite3    # Node: better-sqlite3 is the optional peer
```

```typescript
import { SQLiteStore } from "delaykit/sqlite";

const store = await SQLiteStore.connect("./delaykit.db");
```

Auto-migrates on first connect. Drop into a Bun server:

```typescript
// server.ts
import { DelayKit } from "delaykit";
import { SQLiteStore } from "delaykit/sqlite";
import { PollingScheduler } from "delaykit/polling";

const store = await SQLiteStore.connect("./delaykit.db");
const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

dk.handle("send-reminder", async ({ key }) => { /* your handler */ });
await dk.start();

Bun.serve({
  port: 3000,
  async fetch() {
    await dk.schedule("send-reminder", { key: "user_123", delay: "24h" });
    return Response.json({ ok: true });
  },
});
```

Run: `bun run server.ts`. No native compilation, no separate database service.

*Single-process constraint.* Only one `PollingScheduler` instance can own a given SQLite file. That rules out Node cluster mode with polling on every worker, and multiple app replicas sharing the same file. Your app code can still read and write the same file alongside DelayKit — SQLite's WAL mode handles that cleanly. For horizontal-scale polling, use Postgres.

**Postgres — multi-replica, production-scale.** If your app already has a `postgres` (postgres.js) pool, pass it to DelayKit directly so both share one connection pool:

```typescript
// lib/db.ts
import postgres from "postgres";
export const sql = postgres(process.env.DATABASE_URL!);

// lib/delaykit.ts
import { sql } from "./db";
import { PostgresStore } from "delaykit/postgres";

const store = await PostgresStore.connect(sql);
```

A connection string works too — convenient for scripts and tests that don't already have a pool:

```typescript
const store = await PostgresStore.connect(process.env.DATABASE_URL!);
```

Either form auto-migrates on first connect. Works with Neon, Supabase, Railway — any Postgres. Multiple replicas can share the store; concurrent pollers claim disjoint jobs via `FOR UPDATE SKIP LOCKED`.

### Option 1: Long-running process (Node, Bun, Docker, VPS, Fly)

The simplest path. Works with both SQLite and Postgres. Call `dk.start()` to begin continuous polling:

```typescript
import { DelayKit } from "delaykit";
import { PollingScheduler } from "delaykit/polling";

const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

dk.handle("send-reminder", async ({ key }) => { /* ... */ });

await dk.start();
```

**Multi-instance polling (Postgres only).** Concurrent `PollingScheduler` instances sharing one Postgres store claim disjoint job sets via `FOR UPDATE SKIP LOCKED`, so throughput scales with replicas. `maxConcurrent` is per-instance — the cluster ceiling is `N × maxConcurrent`. For a strict global cap, or for SQLite, run one instance.

**Graceful shutdown.** On SIGTERM, call `dk.stop({ drainMs })` to wait for in-flight handlers to finish before the process exits:

```typescript
process.on("SIGTERM", async () => {
  await dk.stop({ drainMs: 30_000 });
  process.exit(0);
});
```

### Option 2: Serverless + cron (Vercel, Lambda)

Requires Postgres — SQLite is single-process and can't serve concurrent cold starts.

```bash
npm install delaykit postgres
```

Set up DelayKit with `PollingScheduler` (you won't call `.start()` — just `.poll()`):

```typescript
// lib/delaykit.ts
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PollingScheduler } from "delaykit/polling";
import { sql } from "./db"; // from the Postgres snippet above

export async function dk() {
  const store = await PostgresStore.connect(sql);
  const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

  dk.handle("send-reminder", async ({ key }) => {
    // your handler logic
  });

  return dk;
}
```

Add a poll route:

```typescript
// app/api/delaykit/poll/route.ts
import { dk } from "@/lib/delaykit";

export async function GET(req: Request) {
  // Verify the request is from Vercel Cron or an authorized caller
  const auth = req.headers.get("authorization");
  if (auth !== `Bearer ${process.env.CRON_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }

  const d = await dk();
  await d.poll({
    batchSize: 10, // jobs per batch, run concurrently (default: 10)
    timeout: "8s", // hard deadline — leave headroom under Vercel's 10s function limit
  });
  return Response.json({ ok: true });
}
```

Set `CRON_SECRET` in your Vercel environment variables. Vercel automatically sends it with cron requests. For external cron services, include it as `Authorization: Bearer <secret>`.

`poll()` processes due jobs in batches of `batchSize`, running each batch concurrently. It keeps processing batches until there are no more due jobs or `timeout` is reached. If a handler is still running when the deadline hits, it stays in `running` state and is automatically recovered on the next poll cycle.

Schedule the cron — for Vercel:

```json
// vercel.json (Pro plan — runs every minute)
{ "crons": [{ "path": "/api/delaykit/poll", "schedule": "* * * * *" }] }
```

Vercel Hobby only allows daily cron — use an external service for more frequent polling:

- **Vercel Cron (Pro)** — every minute, built-in
- **[cron-job.org](https://cron-job.org)** — free, calls any URL on a schedule
- **[Posthook Sequences](https://posthook.io)** — hourly on the free tier
- **Any server with cron** — `curl https://your-app.vercel.app/api/delaykit/poll`

### Option 3: Managed delivery with Posthook

Works with any store. Postgres is the common pairing — Posthook is most useful in multi-instance or serverless deployments, where SQLite's single-process constraint doesn't fit. Single-instance Posthook + SQLite is also valid when you want event-driven wake-ups without running a polling loop, or when the container suspends between requests.

```bash
npm install delaykit postgres @posthook/node
```

[Posthook](https://posthook.io) delivers each scheduled job to your app as a webhook at the right time. No cron, no long-running process:

```typescript
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PosthookScheduler } from "delaykit/posthook";
import { sql } from "./db"; // from the Postgres snippet above

const store = await PostgresStore.connect(sql);
const dk = new DelayKit({
  store,
  scheduler: new PosthookScheduler({
    apiKey: process.env.POSTHOOK_API_KEY!,
    signingKey: process.env.POSTHOOK_SIGNING_KEY!,
    basePath: "/api/delaykit",
  }),
});
```

Mount a catch-all route to receive deliveries:

```typescript
// app/api/delaykit/[handler]/route.ts
import { dk } from "@/lib/delaykit";

export async function POST(req: Request) {
  const d = await dk();
  const handler = d.createHandler();
  return handler(req);
}
```

### Running Postgres migrations at deploy time

By default, `PostgresStore.connect()` applies any pending migrations on first connect. That's fine for development and single-instance deployments. For production with rolling updates or serverless cold starts — Vercel, Kubernetes, Fly, anywhere concurrent instances spin up — apply migrations once at build time and skip request-time migration. `connect()` still runs a cheap version check so a mis-wired deploy fails loudly instead of silently at the first query.

Add a `postbuild` script that runs migrations before the app starts serving:

```json
// package.json
{
  "scripts": {
    "build": "next build",
    "postbuild": "node scripts/delaykit-migrate.js"
  }
}
```

```js
// scripts/delaykit-migrate.js
import { runPostgresMigrations } from "delaykit/postgres";

await runPostgresMigrations(process.env.DATABASE_URL);
console.log("[delaykit] migrations applied");
```

Then disable request-time migration in your app:

```ts
const store = await PostgresStore.connect(sql, { runMigrations: false });
```

If the schema is behind what the installed library requires, `connect({ runMigrations: false })` throws a clear error naming both versions. That's the safety net if you wire `runMigrations: false` but forget `postbuild`.

**Preview deployments.** Vercel Preview builds run `postbuild` too. Scope `DATABASE_URL` to both Production and Preview (pointing at separate databases), or scope the migration script to Production only.

**Schema compatibility.** Every DelayKit release ships migrations that are backwards-compatible with the previous release's code. Old pods continue to run during rolling deploys. See [CONTRIBUTING.md → Schema changes](./CONTRIBUTING.md#schema-changes) for the full contract.

## Design

**Keys, not payloads.** Jobs carry a key (`"user_123"`) — not a payload snapshot. Handlers fetch current state when they run. This keeps handlers simple and means they always act on fresh data rather than a snapshot from scheduling time. If you need an immutable value from scheduling time (the price at the time of an order, for example), store it in your own tables and look it up by key in the handler.

**Crash recovery.** If a process dies mid-execution, the job is still durable in Postgres or SQLite. DelayKit's stalled-job recovery re-runs the handler after the lease expires. Fetching current state at execution time makes many handlers naturally safe to re-run — a reminder handler that checks `if (user.onboarded) return` is correct however many times it executes. For handlers with external side effects (sending an email, charging a card), use an idempotency key when the service supports it, or check whether the action already completed before executing it.

**What DelayKit doesn't track.** DelayKit wakes a handler with a key at a scheduled time — that's it. It doesn't store workflow state, branch on outcomes, or pass payloads. Multi-step flows (do X, wait, do Y, wait, do Z) live in your own tables; DelayKit provides the timing primitive between steps. See [How it compares](#how-it-compares) below for what handles the broader cases.

**When DelayKit (and when not).** DelayKit's value is durability — timers that survive process death, deploys, and restarts — at the cost of a database roundtrip per wake-up. Reach for DelayKit when the timer must outlast the request that scheduled it: reminders, expirations, agent run timeouts, debounces across replicas. Use plain `setTimeout` or SDK polling helpers when the work fits within a single request's lifetime, or when losing the timer on restart is acceptable. Rule of thumb: would you be OK with this not happening if the process dies? If yes, ephemeral; if no, DelayKit.

For the full correctness model — the invariants that hold across stores and schedulers — see [`docs/INVARIANTS.md`](docs/INVARIANTS.md).

## How it compares

| Tool | Best for | How DelayKit fits |
|------|----------|-------------------|
| **Cron** | Genuinely recurring tasks (`generate-monthly-report`, `sync-exchange-rates`) | Replaces cron-table-scans with per-entity timers — schedule when the event happens, cancel if the condition resolves |
| **Queues** (BullMQ) | Short-lived, high-throughput jobs (Redis-backed) | Long-future durable timers — rows in Postgres/SQLite, not Redis memory. Compose cleanly: DelayKit fires at time T, handler enqueues a BullMQ job |
| **Workflow engines** (Inngest, Temporal) | Multi-step pipelines with branching, waiting, orchestration | DelayKit does one thing: run your handler at the right time |

## Observability

DelayKit has no built-in dashboard. Instead it emits structured lifecycle events and exposes `dk.stats()` — wire those into whatever you already use for monitoring and alerting.

```typescript
dk.on("job:failed", ({ job, error, reason }) => {
  logger.error("job failed", { handler: job.handler, key: job.key, reason });
  metrics.increment("delaykit.job.failed", { handler: job.handler });
});

dk.on("job:completed", ({ job, durationMs }) => {
  metrics.histogram("delaykit.job.duration", durationMs, { handler: job.handler });
});
```

| Event | Fires when |
|-------|-----------|
| `job:scheduled` | Job created (schedule, debounce, throttle) |
| `job:started` | Handler begins executing |
| `job:completed` | Handler succeeded |
| `job:failed` | Retries exhausted |
| `job:retrying` | Handler failed, will retry |
| `job:cancelled` | Job cancelled |
| `job:stalled` | Stalled job detected and recovered |
| `job:deferred` | Handler not registered on any live process; delivery postponed |

Listeners run inline during job execution — keep them fast (logging, metrics). Listener errors are caught and won't break your handlers.

For counts and backlog monitoring, `dk.stats()` returns a snapshot of job counts by status with per-handler breakdown. For operational intervention, `dk.retryJob(id)` reactivates a failed job with a fresh attempt budget.

For connection-pool sizing, retention, and other production concerns, see [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## API reference

| Method | Description |
|--------|-------------|
| `dk.handle(name, handler)` | Register a handler (before start/poll/createHandler) |
| `dk.schedule(handler, opts)` | Schedule a one-time job |
| `dk.debounce(handler, opts)` | Debounce rapid events into one handler call |
| `dk.throttle(handler, opts)` | Throttle to one handler call per time window |
| `dk.cancel(id)` | Cancel a pending job by ID |
| `dk.unschedule(handler, key)` | Cancel by handler and key |
| `dk.getJob(id)` | Look up a job by ID |
| `dk.getJobByKey(handler, key)` | Look up the active job for a handler + key |
| `dk.stats()` | Snapshot of job counts by status, with per-handler breakdown |
| `dk.retryJob(id)` | Reactivate a failed job with a fresh attempt budget |
| `dk.poll(opts?)` | Run one poll cycle (for cron routes) |
| `dk.createHandler()` | Create a webhook route handler (for external schedulers) |
| `dk.on(event, listener)` | Subscribe to lifecycle events |

### Duration format

Delays and timeouts use human-readable strings: `"5s"`, `"30m"`, `"24h"`, `"14d"`, `"500ms"`. Compound durations work too: `"1h30m"`.

| Unit | Example |
|------|---------|
| `ms` | `"500ms"` |
| `s` | `"30s"` |
| `m` | `"5m"` |
| `h` | `"24h"` |
| `d` | `"14d"` |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project layout, test commands, and conventions.

## License

MIT

Built by the team behind [Posthook](https://posthook.io).

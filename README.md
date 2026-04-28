# DelayKit

[![CI](https://github.com/delaykit/delaykit/actions/workflows/ci.yml/badge.svg)](https://github.com/delaykit/delaykit/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/delaykit.svg)](https://www.npmjs.com/package/delaykit)
[![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](./package.json)
[![types](https://img.shields.io/npm/types/delaykit.svg)](https://www.npmjs.com/package/delaykit)
[![license](https://img.shields.io/npm/l/delaykit.svg)](./LICENSE)

**Durable wake-ups for TypeScript apps and agents.**
Reminders, expirations, retries, debounces, and agent resumes. Backed by Postgres or SQLite.

DelayKit is intentionally narrow. It is not a workflow engine or a job queue. It runs your handler at the scheduled time and lets you cancel.

> **Status:** pre-1.0. Minor releases may include breaking changes. See the [changelog](CHANGELOG.md).

## Contents

- [Quick start](#quick-start)
- [What DelayKit handles](#what-delaykit-handles)
- [What you can build](#what-you-can-build)
- [Deploy to production](#deploy-to-production)
- [Design](#design)
- [Observability](#observability)
- [How it compares](#how-it-compares)
- [API](#api)

## Quick start

```bash
npm install delaykit   # Node
bun add delaykit       # Bun
```

Try it locally with MemoryStore. No database needed:

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

await dk.start(); // for serverless (Vercel), use poll() instead. See Deploy to production below.

// Send a reminder if the user hasn't onboarded after 24 hours
await dk.schedule("send-reminder", {
  key: "user_123",
  delay: "24h",
});

// User completed onboarding. Cancel the reminder.
await dk.unschedule("send-reminder", "user_123");
```

MemoryStore is for local development. For jobs that survive restarts, swap in PostgresStore or SQLiteStore. See [Pick a store](#pick-a-store).

Want to run something end-to-end? See [`examples/basic.ts`](examples/basic.ts) (MemoryStore), [`examples/sqlite.ts`](examples/sqlite.ts), and [`examples/postgres.ts`](examples/postgres.ts).

## What DelayKit handles

- **Jobs survive restarts and deploys.** Durable in Postgres or SQLite, not in memory.
- **No duplicate jobs.** Same handler and key won't create a second pending job.
- **Fresh state at execution time.** Handlers receive the key and fetch current data.
- **Automatic retries.** Failed handlers retry with configurable backoff.
- **Stalled job recovery.** Crashed processes don't leave stuck jobs.
- **Bounded concurrency.** `PollingScheduler` runs at most `maxConcurrent` handlers at once (default 10).
- **Zero runtime dependencies.** `postgres`, `better-sqlite3`, and `@posthook/node` are optional peers.

## What you can build

### Wake an agent after a human-in-the-loop timeout

When a long-running agent waits for human input, schedule a timeout so the run doesn't hang.

```typescript
await dk.schedule("approval-timeout", { key: "run_789", delay: "24h" });

// When approval arrives:
await dk.unschedule("approval-timeout", "run_789");
```

If approval doesn't come in time, the handler resumes the agent run with a "timed out" outcome. No polling loop, no idle worker.

### Expire a trial or reservation

```typescript
await dk.schedule("expire-trial", { key: "acct_456", delay: "14d" });

// Or use an absolute time
await dk.schedule("expire-trial", { key: "acct_456", at: trialEndsAt });

// User upgraded. Cancel the expiration.
await dk.unschedule("expire-trial", "acct_456");
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

### Reindex after a burst of edits

User updates several fields. Reindex once after they stop, not on every change.

```typescript
await dk.debounce("reindex", { key: "project_789", wait: "5s" });
```

## Deploy to production

DelayKit has two moving parts. Pick each independently based on your infrastructure.

**Store.** The durable source of truth. Owns the job rows. Every execution decision reads from the row, so duplicate or stale wake-ups don't cause incorrect runs. Implementations: `PostgresStore`, `SQLiteStore`, `MemoryStore` (dev only).

**Scheduler.** The wake-up signal. Decides *when* to claim due jobs: `PollingScheduler` polls the store; `PosthookScheduler` registers a webhook with [Posthook](https://posthook.io). Wake-ups are disposable. The store row has the final say on what actually runs.

The interface between them is small (claim, complete, fail, defer), so you can pair them freely.

### Pick a store

**SQLite. Local-first, zero infra.** The simplest path for single-process apps: a Bun server, a Node backend on one VPS, a desktop or CLI tool. No database service to run, no credentials to manage.

```bash
bun add delaykit                       # Bun: bun:sqlite is built in
npm install delaykit better-sqlite3    # Node: better-sqlite3 is the optional peer
```

```typescript
import { SQLiteStore } from "delaykit/sqlite";

const store = await SQLiteStore.connect("./delaykit.db");
```

Auto-migrates on first connect. Single-process: one `PollingScheduler` per file. For horizontal-scale polling, use Postgres.

**Postgres. Multi-replica.** For multi-instance apps and serverless. Share an existing `postgres` (postgres.js) pool, or pass a connection string:

```typescript
import postgres from "postgres";
import { PostgresStore } from "delaykit/postgres";

const sql = postgres(process.env.DATABASE_URL!);
const store = await PostgresStore.connect(sql);
// Or: await PostgresStore.connect(process.env.DATABASE_URL!);
```

Auto-migrates on first connect. Works with Neon, Supabase, Railway, or any Postgres.

### Three runtime shapes

Pick by where your code lives:

- **Long-running process** (Node, Bun, Docker, VPS, Fly). Call `dk.start()` to poll continuously. Works with SQLite or Postgres.
- **Serverless and cron** (Vercel, Lambda). Call `dk.poll()` from a cron route. Postgres only.
- **Managed delivery with Posthook.** Webhook-driven. No cron, no long-running process.

For walkthroughs of each option, plus tuning `maxConcurrent`, cooperative timeouts, and Postgres migrations at deploy time, see [`docs/deploy.md`](docs/deploy.md).

## Design

**Keys, not payloads.** Jobs carry a key (`"user_123"`), not a payload snapshot. Handlers fetch current state when they run, so they always act on fresh data. If you need an immutable value from scheduling time (the price at the time of an order, for example), store it in your own tables and look it up by key in the handler.

**Crash recovery.** If a process dies mid-execution, the job is still durable in Postgres or SQLite. DelayKit's stalled-job recovery re-runs the handler after the lease expires. Fetching current state at execution time makes many handlers naturally safe to re-run. A reminder handler that checks `if (user.onboarded) return` is correct however many times it executes. For handlers with external side effects (sending an email, charging a card), use an idempotency key when the service supports it, or check whether the action already completed before executing it.

**What DelayKit doesn't track.** DelayKit wakes a handler with a key at a scheduled time. That's it. It doesn't store workflow state, branch on outcomes, or pass payloads. Multi-step flows (do X, wait, do Y, wait, do Z) live in your own tables; DelayKit provides the timing primitive between steps. See [How it compares](#how-it-compares) below for what handles the broader cases.

**When DelayKit (and when not).** Reach for DelayKit when the timer must outlast the request that scheduled it: reminders, expirations, agent run timeouts, debounces across replicas. Use `setTimeout` or SDK polling helpers when the work fits in one request, or when losing the timer on restart is acceptable. Rule of thumb: would you be OK with this not happening if the process dies? If yes, ephemeral. If no, DelayKit.

For the full correctness model (the invariants that hold across stores and schedulers), see [`docs/INVARIANTS.md`](docs/INVARIANTS.md).

## Observability

DelayKit has no built-in dashboard. Instead it emits structured lifecycle events and exposes `dk.stats()`. Wire those into whatever you already use for monitoring and alerting.

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

Listeners run inline. Keep them fast (logging, metrics). Listener errors are caught and won't break your handlers.

For backlog stats and retrying failed jobs, see `dk.stats()` and `dk.retryJob(id)` in [`docs/api.md`](docs/api.md). Connection-pool sizing, retention, and other production concerns are covered in [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## How it compares

| Tool | Best for | How DelayKit fits |
|------|----------|-------------------|
| **Cron** | Genuinely recurring tasks (`generate-monthly-report`, `sync-exchange-rates`) | Replaces cron-table-scans with per-entity timers. Schedule when the event happens, cancel if the condition resolves |
| **Queues** (BullMQ) | Short-lived, high-throughput jobs (Redis-backed) | Long-future durable timers as rows in Postgres/SQLite, not Redis memory. Compose cleanly: DelayKit fires at time T, handler enqueues a BullMQ job |
| **Workflow engines** (Inngest, Temporal) | Multi-step pipelines with branching, waiting, orchestration | DelayKit does one thing: run your handler at the right time |

## API

DelayKit's TypeScript types are the canonical reference. Hover any method in your editor for full signatures and inline docstrings.

For an at-a-glance lookup table of every method plus the [duration format](docs/api.md#duration-format), see [`docs/api.md`](docs/api.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project layout, test commands, and conventions.

## License

MIT

Built alongside [Posthook](https://posthook.io).

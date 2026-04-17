# DelayKit

The timing layer for Next.js.
Reminders, expirations, follow-ups — backed by Postgres.

## Quick start

```bash
npm install delaykit
```

Try it locally with MemoryStore — no database needed:

```typescript
import { DelayKit } from "delaykit";
import { MemoryStore } from "delaykit/memory";
import { PollingScheduler } from "delaykit/polling";

const dk = new DelayKit({
  store: new MemoryStore(), // swap to PostgresStore for production
  scheduler: new PollingScheduler(),
});

dk.handle("send-reminder", async ({ key }) => {
  const user = await db.users.find(key);
  if (user.onboarded) return; // already acted, skip
  await sendEmail(user.email, "Complete your profile");
});

await dk.start(); // for serverless (Vercel), use poll() instead — see Deploy to production

// Send a reminder if the user hasn't onboarded after 24 hours
await dk.schedule("send-reminder", {
  key: "user_123",
  delay: "24h",
});

// User completed onboarding — cancel the reminder
await dk.unschedule("send-reminder", "user_123");
```

MemoryStore is for local development. For jobs that survive restarts, use PostgresStore — see [Deploy to production](#deploy-to-production).

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

### Safe to call from repeated requests

Same handler + same key won't create duplicate jobs. Call schedule from every request — only one pending job exists at a time.

```typescript
await dk.schedule("welcome-email", { key: "user_123", delay: "10m" });
```

## What DelayKit handles for you

- **Jobs survive restarts and deploys** — they're in Postgres, not memory
- **No duplicate jobs** — same handler + key won't create a second pending job
- **Fresh state at execution time** — handlers receive the key and fetch current data, no stale payloads
- **Automatic retries** — failed handlers retry with configurable backoff
- **Stalled job recovery** — crashed processes don't leave stuck jobs
- **Bounded concurrency** — `PollingScheduler` runs at most `maxConcurrent` handlers at once (default 10); the rest stay `pending` in the store and are claimed on subsequent polls

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

DelayKit has two moving parts: the **store** (Postgres) and the **scheduler** (how jobs get picked up at their scheduled time). Pick the scheduler that matches your infrastructure.

### Connect to your Postgres

If your app already has a `postgres` (postgres.js) pool, pass it to DelayKit directly so both share one connection pool against the database:

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

Either form auto-migrates on first connect. Works with Neon, Supabase, Railway — any Postgres.

### Option 1: Vercel + Posthook (managed delivery)

```bash
npm install delaykit postgres @posthook/node
```

[Posthook](https://posthook.io) delivers each scheduled job to your app as a webhook at the right time. No cron, no long-running process:

```typescript
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PosthookScheduler } from "delaykit/posthook";
import { sql } from "./db"; // from the snippet above

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

### Option 2: Vercel + cron (self-hosted polling)

```bash
npm install delaykit postgres
```

Set up DelayKit with `PollingScheduler`:

```typescript
// lib/delaykit.ts
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PollingScheduler } from "delaykit/polling";
import { sql } from "./db"; // from the snippet above

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

Schedule the cron:

```json
// vercel.json (Pro plan — runs every minute)
{ "crons": [{ "path": "/api/delaykit/poll", "schedule": "* * * * *" }] }
```

Vercel Hobby only allows daily cron — use an external service for more frequent polling:

- **Vercel Cron (Pro)** — every minute, built-in
- **[cron-job.org](https://cron-job.org)** — free, calls any URL on a schedule
- **[Posthook Sequences](https://posthook.io)** — hourly on the free tier
- **Any server with cron** — `curl https://your-app.vercel.app/api/delaykit/poll`

### Running migrations at deploy time

By default, `PostgresStore.connect()` applies any pending migrations on first connect. That's fine for development and small deployments. For production — especially on Vercel, where cold starts can stack up and function timeouts can cut off a long migration — apply migrations once at build time and skip request-time migration. `connect()` still runs a cheap version check so a mis-wired deploy fails loudly instead of silently at the first query.

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
import { runMigrations } from "delaykit/postgres";

await runMigrations(process.env.DATABASE_URL);
console.log("[delaykit] migrations applied");
```

Then disable request-time migration in your app:

```ts
const store = await PostgresStore.connect(sql, { runMigrations: false });
```

If the schema is behind what the installed library requires, `connect({ runMigrations: false })` throws a clear error naming both versions. That's the safety net if you wire `runMigrations: false` but forget `postbuild`.

**Preview deployments.** Vercel Preview builds run `postbuild` too. Scope `DATABASE_URL` to both Production and Preview (pointing at separate databases), or scope the migration script to Production only.

**Schema compatibility.** Every DelayKit release ships migrations that are backwards-compatible with the previous release's code. Old pods continue to run during Vercel's rollover. See [CONTRIBUTING.md → Schema changes](./CONTRIBUTING.md#schema-changes) for the full contract.

### Option 3: Long-running server (VPS, Docker, Fly)

For any host that runs a long-lived Node process, call `dk.start()` to begin continuous polling:

```typescript
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PollingScheduler } from "delaykit/polling";
import { sql } from "./db"; // from the snippet above

const store = await PostgresStore.connect(sql);
const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

dk.handle("send-reminder", async ({ key }) => { /* ... */ });

await dk.start();
```

**Multi-instance polling.** Concurrent `PollingScheduler` instances sharing one store claim disjoint job sets via `FOR UPDATE SKIP LOCKED`, so throughput scales with replicas. `maxConcurrent` is per-instance — the cluster ceiling is `N × maxConcurrent`. For a strict global cap, run one instance.

**Graceful shutdown.** On SIGTERM, call `dk.stop({ drainMs })` to wait for in-flight handlers to finish before the process exits:

```typescript
process.on("SIGTERM", async () => {
  await dk.stop({ drainMs: 30_000 });
  process.exit(0);
});
```

## Design

**Keys, not payloads.** Jobs carry a key (`"user_123"`) — not a payload snapshot. Handlers fetch current state when they run. This keeps handlers simple and means they always act on fresh data rather than a snapshot from scheduling time. If you need an immutable value from scheduling time (the price at the time of an order, for example), store it in your own tables and look it up by key in the handler.

**Crash recovery.** If a process dies mid-execution, the job is still in Postgres. DelayKit's stalled-job recovery re-runs the handler after the lease expires. Fetching current state at execution time makes many handlers naturally safe to re-run — a reminder handler that checks `if (user.onboarded) return` is correct however many times it executes. For handlers with external side effects (sending an email, charging a card), use an idempotency key when the service supports it, or check whether the action already completed before executing it.

## How it compares

**Cron** — cron runs a task on a schedule; DelayKit schedules an action per entity when an event happens. The common pattern of scanning a table on a timer to find records that need action is a natural fit for DelayKit: schedule the job when the event occurs, cancel it if the condition resolves. Cron remains the right tool for genuinely recurring tasks (`generate-monthly-report`, `sync-exchange-rates`).

**Queues** (BullMQ, QStash) — process background jobs as soon as possible. DelayKit schedules actions for a specific future time.

**Workflow engines** (Inngest, Temporal) — orchestrate multi-step pipelines with branching and waiting. DelayKit does one thing: run your handler at the right time.

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

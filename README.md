# DelayKit

Run code later in Next.js. Reminders, expirations, follow-ups — backed by Postgres.

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

await dk.start(); // for serverless (Vercel), use poll() instead — see Deploy to Vercel

// Send a reminder if the user hasn't onboarded after 24 hours
await dk.schedule("send-reminder", {
  key: "user_123",
  delay: "24h",
});

// User completed onboarding — cancel the reminder
await dk.unschedule("send-reminder", "user_123");
```

MemoryStore is for local development. For jobs that survive restarts, use PostgresStore — see [Deploy to Vercel](#deploy-to-vercel).

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
- **Handlers should be idempotent** — DelayKit prevents duplicate scheduling, but handlers may re-execute after a crash recovery

### Tuning concurrency

`PollingScheduler` runs at most `maxConcurrent` handlers at a time. Default is `10`. Raise it for I/O-bound handlers, lower it for CPU-heavy ones:

```typescript
new PollingScheduler({ maxConcurrent: 25 });
```

Excess due jobs stay `pending` in the store and are claimed on subsequent polls.

**Cooperative timeouts.** When a handler hits its `timeout`, DelayKit aborts `ctx.signal` and then waits for the handler to return before releasing its concurrency slot. Pass `signal` through to whatever the handler is calling (most modern Node APIs — `fetch`, `pg`, etc. — accept one) so the handler exits on abort. Handlers that ignore the signal hold their slot until they return on their own:

```typescript
dk.handle("send-email", {
  handler: async ({ key, signal }) => {
    await fetch(`https://api.example.com/send/${key}`, { signal });
  },
  timeout: "10s",
});
```

## Deploy to Vercel

```bash
npm install delaykit postgres
```

### 1. Set up DelayKit

```typescript
// lib/delaykit.ts
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PollingScheduler } from "delaykit/polling";

export async function dk() {
  const store = await PostgresStore.connect(process.env.DATABASE_URL!);
  const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

  dk.handle("send-reminder", async ({ key }) => {
    // your handler logic
  });

  return dk;
}
```

### 2. Add a poll route

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

### 3. Schedule the cron

```json
// vercel.json (Pro plan — runs every minute)
{ "crons": [{ "path": "/api/delaykit/poll", "schedule": "* * * * *" }] }
```

Auto-migrates on first connect. Works with Neon, Supabase, Railway — any Postgres.

### Calling your poll route

`poll()` needs something to call it on a schedule. Vercel Hobby only allows daily cron — use an external service for more frequent polling:

- **Vercel Cron (Pro)** — every minute, built-in
- **[cron-job.org](https://cron-job.org)** — free, calls any URL on a schedule
- **[Posthook Sequences](https://posthook.io)** — hourly on the free tier
- **Any server with cron** — `curl https://your-app.vercel.app/api/delaykit/poll`

## How it works

Jobs live in Postgres. A cron route calls `dk.poll()` on a schedule to find due jobs and run your handlers. If the process crashes mid-execution, the job is still in Postgres — it recovers on the next poll cycle.

DelayKit stores keys, not payloads. Handlers receive the key (`user_123`) and fetch current state when they run. This means handlers always act on fresh data, not stale snapshots from scheduling time. If you need an immutable snapshot (e.g., the price at the time of an order), store that in your app's tables and schedule the job with a reference to it.

## Not cron, not a queue, not a workflow engine

- **Cron** is for recurring tasks on a fixed schedule. DelayKit is for one-time actions tied to a specific user or entity.
- **Queues** (BullMQ, QStash) process background jobs as soon as possible. DelayKit schedules actions for a specific time in the future.
- **Workflow engines** (Inngest, Temporal) orchestrate multi-step pipelines. DelayKit does one thing: run your handler at the right time.

## Lifecycle events

```typescript
dk.on("job:completed", ({ job, durationMs }) => {
  console.log(`${job.key} completed in ${durationMs}ms`);
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

Listeners run inline during job execution — keep them fast (logging, metrics). Listener errors are caught and won't break your handlers.

## External schedulers

DelayKit's `Scheduler` interface is pluggable. Instead of polling, an external scheduler can call your app directly when each job is due. [Posthook](https://posthook.io) is the first supported external scheduler:

```bash
npm install @posthook/node
```

```typescript
import { PosthookScheduler } from "delaykit/posthook";

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

With an external scheduler, you don't need a cron route or poll cycle.

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

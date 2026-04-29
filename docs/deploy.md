# Deploying DelayKit

DelayKit fits three runtime shapes. This page covers all three, plus tuning and Postgres migrations.

- [Long-running process (Node, Bun, Docker, VPS, Fly)](#long-running-process)
- [Serverless and cron (Vercel, Lambda)](#serverless-and-cron)
- [Managed delivery with Posthook](#managed-delivery-with-posthook)
- [Tuning concurrency and timeouts](#tuning-concurrency-and-timeouts)
- [Running Postgres migrations at deploy time](#running-postgres-migrations-at-deploy-time)

## Long-running process

The simplest path. Works with both SQLite and Postgres. Call `dk.start()` to begin continuous polling:

```typescript
import { DelayKit } from "delaykit";
import { PollingScheduler } from "delaykit/polling";

const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

dk.handle("send-reminder", async ({ key }) => { /* ... */ });

await dk.start();
```

**Multi-instance polling (Postgres only).** Concurrent `PollingScheduler` instances sharing one Postgres store claim disjoint job sets via `FOR UPDATE SKIP LOCKED`, so throughput scales with replicas. `maxConcurrent` is per-instance, so the cluster ceiling is `N × maxConcurrent`. For a strict global cap, or for SQLite, run one instance.

**Graceful shutdown.** On SIGTERM, call `dk.stop({ drainMs, closeStore: true })` to drain in-flight handlers and close the store before the process exits:

```typescript
process.on("SIGTERM", async () => {
  await dk.stop({ drainMs: 30_000, closeStore: true });
});
```

`dk.stop()` is idempotent, so wiring SIGTERM and SIGINT to the same handler is safe. Pass `closeStore: false` (the default) when the store or its connection pool is shared with other parts of your app — closing it from `dk.stop()` would break other consumers. If your app also runs an HTTP server, call its `stop()` first so it stops accepting requests before the scheduler drains.

### Bun server example

A single-file Bun server with SQLite:

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

Run: `bun run server.ts`. No native compilation, no separate database service. See [`examples/bun-sqlite-server/`](../examples/bun-sqlite-server/) for a complete runnable version with HTTP routes and shutdown handling.

## Serverless and cron

Requires Postgres. SQLite is single-process and can't serve concurrent cold starts.

```bash
npm install delaykit postgres
```

Set up DelayKit with `PollingScheduler`. You won't call `.start()`. Use `.poll()` instead:

```typescript
// lib/delaykit.ts
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PollingScheduler } from "delaykit/polling";
import { sql } from "./db"; // your postgres.js client

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
    timeout: "8s", // hard deadline. Leave headroom under Vercel's 10s function limit.
  });
  return Response.json({ ok: true });
}
```

Set `CRON_SECRET` in your Vercel environment variables. Vercel automatically sends it with cron requests. For external cron services, include it as `Authorization: Bearer <secret>`.

`poll()` processes due jobs in batches of `batchSize`, running each batch concurrently. It keeps processing batches until there are no more due jobs or `timeout` is reached. If a handler is still running when the deadline hits, it stays in `running` state and is automatically recovered on the next poll cycle.

Schedule the cron. For Vercel:

```json
// vercel.json (Pro plan, runs every minute)
{ "crons": [{ "path": "/api/delaykit/poll", "schedule": "* * * * *" }] }
```

Vercel Hobby only allows daily cron. Use an external service for more frequent polling:

- **Vercel Cron (Pro).** Every minute, built-in.
- **[cron-job.org](https://cron-job.org).** Free, calls any URL on a schedule.
- **[Posthook Sequences](https://posthook.io).** Hourly on the free tier.
- **Any server with cron.** `curl https://your-app.vercel.app/api/delaykit/poll`

## Managed delivery with Posthook

Works with any store. Postgres is the common pairing. Posthook is most useful in multi-instance or serverless deployments, where SQLite's single-process constraint doesn't fit. Single-instance Posthook + SQLite is also valid when you want event-driven wake-ups without running a polling loop, or when the container suspends between requests.

```bash
npm install delaykit postgres @posthook/node
```

[Posthook](https://posthook.io) delivers each scheduled job to your app as a webhook at the right time. No cron, no long-running process:

```typescript
import { DelayKit } from "delaykit";
import { PostgresStore } from "delaykit/postgres";
import { PosthookScheduler } from "delaykit/posthook";
import { sql } from "./db"; // your postgres.js client

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

## Tuning concurrency and timeouts

`PollingScheduler` runs at most `maxConcurrent` handlers at a time. Default is `10`. Raise it for I/O-bound handlers, lower it for CPU-heavy ones:

```typescript
new PollingScheduler({ maxConcurrent: 25 });
```

Excess due jobs stay `pending` in the store and are claimed on subsequent polls.

**Cooperative timeouts.** Every handler has a timeout. The default is `30s`, or whatever you set via `timeout:`. When the timer fires, DelayKit aborts `ctx.signal` and then waits for the handler to return before releasing its concurrency slot. Pass `signal` through to whatever the handler is calling so it exits on abort. Most modern Node APIs (`fetch`, `pg`, etc.) accept one. Handlers that ignore the signal hold their slot until they return on their own:

```typescript
dk.handle("send-email", {
  handler: async ({ key, signal }) => {
    await fetch(`https://api.example.com/send/${key}`, { signal });
  },
  timeout: "10s",
});
```

## Running Postgres migrations at deploy time

By default, `PostgresStore.connect()` applies any pending migrations on first connect. That's fine for development and single-instance deployments. For production with rolling updates or serverless cold starts (Vercel, Kubernetes, Fly, anywhere concurrent instances spin up), apply migrations once at build time and skip request-time migration. `connect()` still runs a cheap version check so a mis-wired deploy fails loudly instead of silently at the first query.

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

**Schema compatibility.** Every DelayKit release ships migrations that are backwards-compatible with the previous release's code. Old pods continue to run during rolling deploys. See [CONTRIBUTING.md → Schema changes](../CONTRIBUTING.md#schema-changes) for the full contract.

# Operations

Reference for running DelayKit in production.

## Connection pool sizing

When handlers query the same Postgres instance that backs DelayKit, size your connection pool above `maxConcurrent`. Each running handler that hits the DB needs a connection — if the pool is smaller than the number of concurrent handlers, handlers queue waiting for a connection while holding their concurrency slot.

```typescript
import postgres from "postgres";

const sql = postgres(process.env.DATABASE_URL!, {
  max: 20, // pool size — should exceed PollingScheduler maxConcurrent
});

new PollingScheduler({ maxConcurrent: 15 });
```

A safe rule of thumb: pool size ≥ `maxConcurrent` + a few connections for your app's own queries.

## DB user permissions

For production, run migrations with a privileged user and application queries with a least-privilege user that cannot alter the schema.

**Migration user** — needs full DDL access:

```sql
GRANT ALL ON SCHEMA delaykit TO migrator;
GRANT ALL ON ALL TABLES IN SCHEMA delaykit TO migrator;
```

**Application user** — needs DML only:

```sql
GRANT USAGE ON SCHEMA delaykit TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA delaykit TO app_user;
```

Wire them separately:

```typescript
// scripts/delaykit-migrate.js — runs at build time with DATABASE_MIGRATION_URL
import { runMigrations } from "delaykit/postgres";
await runMigrations(process.env.DATABASE_MIGRATION_URL);

// lib/delaykit.ts — runtime uses the restricted app user
const store = await PostgresStore.connect(process.env.DATABASE_URL, {
  runMigrations: false,
});
```

## Useful queries

**Job counts by status:**

```sql
SELECT status, COUNT(*) FROM delaykit.jobs GROUP BY status;
```

**Due but unclaimed jobs** (should stay near zero — high counts indicate the poller is behind or stopped):

```sql
SELECT handler, COUNT(*)
FROM delaykit.jobs
WHERE status = 'pending' AND scheduled_for <= now()
GROUP BY handler
ORDER BY COUNT(*) DESC;
```

**Long-running jobs** (potential stallers):

```sql
SELECT id, handler, key, started_at, now() - started_at AS age
FROM delaykit.jobs
WHERE status = 'running'
ORDER BY started_at ASC;
```

**Recent failures:**

```sql
SELECT id, handler, key, attempt, last_error, completed_at
FROM delaykit.jobs
WHERE status = 'failed'
ORDER BY completed_at DESC
LIMIT 50;
```

**Inspect a specific entity:**

```sql
SELECT * FROM delaykit.jobs
WHERE handler = 'send-reminder' AND key = 'user_123'
ORDER BY created_at DESC;
```

## Failure modes

### PollingScheduler

**DB unreachable.** The poller backs off exponentially (up to 30s between attempts) and logs each failure. Jobs stay `pending` in the store — nothing is lost. When the DB recovers, the poller resumes and due jobs are claimed on the next cycle.

**Process crash mid-execution.** The job stays `running` with a stale `started_at`. The stalled-job sweep (runs every 30s by default) detects the expired lease and transitions the job back to `pending` for retry, or to `failed` if retries are exhausted.

**Handler timeout.** DelayKit aborts `ctx.signal` at the configured timeout and waits for the handler to return. The slot is not released until the handler exits. If the handler ignores the signal and the process crashes, stalled recovery handles it as above.

**Handler not registered.** `PollingScheduler` only claims jobs whose handlers are registered on the current replica — unregistered jobs stay `pending` and remain claimable by any replica that can run them. Each sweep cycle (and each `dk.poll()` call, after its claim loop) records the missing-handler horizon clock for orphan rows via `Store.noteMissingHandler`. The row's `scheduled_for` is not moved, so capable replicas in mixed-handler deployments still see it as due and claim it normally. Only when no replica registers the handler for the full horizon (default 24h) does the row flip to `failed` with `reason: "defer_horizon"`. `sweepStalled` also logs a warning each cycle that finds orphan rows, surfacing the misconfiguration ahead of horizon termination.

### PosthookScheduler

**DB unreachable or handler throws.** The handler returns 500 and Posthook retries delivery according to its retry schedule. Jobs stay `pending` in the store between attempts. No stalled sweep is needed — Posthook drives the retry externally.

**Process crash mid-execution.** The delivery times out on Posthook's side and is retried. If the retry arrives before the job's lease expires (`timeout + 5s` grace, 35s by default), the row is still `running` and the delivery is skipped — Posthook will retry again later. Once the lease expires, DelayKit's inline stalled reclaim transitions the row back to `pending` and the next delivery can claim it.

**Handler timeout.** DelayKit aborts `ctx.signal` and waits for the handler to return. If the handler exceeds the hard timeout configured on `createHandler`, DelayKit returns 500 so Posthook retries. The job transitions back to `pending` between attempts.

**Handler not registered.** If no live process has the handler registered, the job is deferred with exponential backoff and a `job:awaiting_handler` event is emitted. After the defer horizon (default 24h), the job transitions to `failed`.

## Poll-until-done

For workflows that wait on an external async job — Replicate predictions, OpenAI batch jobs, Mux transcodes — the handler can call `ctx.reschedule({ delay, at })` to come back later instead of completing. The row transitions `running → pending` with the new `scheduledFor` and a fresh attempt budget; `dk.schedule(...)` from inside the handler does *not* work for this (it's idempotent and skips active rows).

```ts
dk.handle("check-replicate", async ({ key, reschedule }) => {
  const prediction = await replicate.predictions.get(key);
  if (["succeeded", "failed", "canceled"].includes(prediction.status)) {
    await onDone(prediction);
    return; // row goes to `completed`
  }
  reschedule({ delay: "2m" }); // row goes back to `pending` for the next check
});
```

Semantics:

- Last `reschedule(...)` call within a run wins. The handler returns normally; intent is honored after return.
- Throwing from the handler discards the reschedule intent and falls through to normal retry/failure logic.
- `attempt` resets to 0 on each rescheduled delivery — the just-finished run is a checkpoint, not a consumed retry. Cap your own iteration count if needed (e.g., based on `prediction.created_at`).
- Currently scoped to `kind="once"` jobs. Pattern handlers (debounce, throttle) requeue automatically via their wait window — calling `ctx.reschedule` from a pattern handler throws.
- Each rescheduled cycle emits `job:rescheduled` with `scheduledFor` and `durationMs` for observability.

## Operating failed jobs

Every terminal failure carries a `failureReason` discriminator (`handler_error`, `timeout`, `stalled`, `defer_horizon`, `materialization_error`) on both the row and the `job:failed` event. Use it to filter triage and redrive workflows.

**Inspect a single job:**

```typescript
const job = await dk.getJob(id);
// job.failureReason === "timeout" / "handler_error" / etc.
```

**List failed jobs (paginated, newest-first):**

```typescript
let cursor: string | null = null;
do {
  const page = await dk.listFailed({
    handler: "send-reminder",     // optional
    reason: "timeout",            // optional
    since: new Date(Date.now() - 60 * 60 * 1000),  // optional
    limit: 100,                   // required, hard cap 1000
    cursor: cursor ?? undefined,
  });
  for (const job of page.jobs) {
    console.log(job.id, job.handler, job.failureReason, job.lastError);
  }
  cursor = page.cursor;
} while (cursor);
```

**Retry a single job:**

```typescript
const job = await dk.retryJob(id);
// returns null if the job doesn't exist or isn't failed
```

**Bulk redrive by filter:**

```typescript
const result = await dk.retryFailed({
  handler: "send-reminder",
  reason: "timeout",
  since: new Date(Date.now() - 60 * 60 * 1000),
  limit: 1000,
  // spreadMs defaults to min(N * 100, 60_000); pass 0 for immediate.
});
// result: { retried, skipped, spreadMs, hasMore }
```

`retryFailed` requires at least one of `handler`, `reason`, or `since` — bare calls would otherwise retry unbounded history. Returned `hasMore: true` signals more matching rows; iterate with `since`/`until`.

**Bulk redrive by IDs (after listing):**

```typescript
const page = await dk.listFailed({ handler: "charge-card", limit: 100 });
const ids = page.jobs.filter(j => j.lastError?.includes("ECONNRESET")).map(j => j.id);
await dk.retryFailed({ ids, spreadMs: 30_000 });
```

**Why staggering matters.** Bulk redrives spread `scheduledFor` linearly across the spread window so 1000 rows don't all become due at once — protects the user's handler endpoint from a thundering herd, and Postgres from a claim spike. Default formula is `min(count * 100, 60_000)`. Override via `spreadMs`. Pass `0` only when immediate redrive is the explicit intent.

`retryFailed` is sequential per row — each row goes through the same CAS + scheduler wake as a single-job retry. Concurrent rows from a separate manual `retryJob` are counted as `skipped`.

## Pruning old jobs

DelayKit does not prune completed or failed jobs automatically. Add a periodic cleanup based on your retention needs:

```sql
DELETE FROM delaykit.jobs
WHERE status IN ('completed', 'cancelled')
  AND completed_at < now() - interval '30 days';
```

Keep `failed` rows until you've triaged them or your alerting has processed them.

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

**Handler not registered.** `PollingScheduler` only claims jobs whose handlers are registered on the current replica — unregistered jobs stay `pending` and are available to any replica that can run them. If no replica in the cluster has the handler registered, `sweepStalled` logs a warning so the gap doesn't go silent.

### PosthookScheduler

**DB unreachable or handler throws.** The handler returns 500 and Posthook retries delivery according to its retry schedule. Jobs stay `pending` in the store between attempts. No stalled sweep is needed — Posthook drives the retry externally.

**Process crash mid-execution.** The delivery times out on Posthook's side and is retried. If the retry arrives before the job's lease expires (`timeout + 5s` grace, 35s by default), the row is still `running` and the delivery is skipped — Posthook will retry again later. Once the lease expires, DelayKit's inline stalled reclaim transitions the row back to `pending` and the next delivery can claim it.

**Handler timeout.** DelayKit aborts `ctx.signal` and waits for the handler to return. If the handler exceeds the hard timeout configured on `createHandler`, DelayKit returns 500 so Posthook retries. The job transitions back to `pending` between attempts.

**Handler not registered.** If no live process has the handler registered, the job is deferred with exponential backoff and a `job:deferred` event is emitted. After the defer horizon (default 24h), the job transitions to `failed`.

## Operating failed jobs

**Retry a single job:**

```typescript
const job = await dk.retryJob(id);
// returns null if the job doesn't exist or isn't failed
```

**Bulk retry by handler:**

```typescript
const { byHandler } = await dk.stats();

for (const entry of byHandler) {
  if (entry.failed24h === 0) continue;

  // query your own store for failed job IDs by handler
  const { rows } = await sql`
    SELECT id FROM delaykit.jobs
    WHERE status = 'failed' AND handler = ${entry.handler}
  `;
  for (const row of rows) {
    await dk.retryJob(row.id);
  }
}
```

Or directly in SQL if you need to retry a large batch without waking the scheduler:

```sql
UPDATE delaykit.jobs
SET
  status = 'pending',
  attempt = 0,
  version = version + 1,
  scheduled_for = now(),
  started_at = NULL,
  completed_at = NULL,
  claimed_version = NULL,
  last_error = NULL,
  scheduler_ref = NULL
WHERE status = 'failed'
  AND handler = 'send-reminder'
  AND completed_at > now() - interval '1 hour';
```

Note: the SQL approach bypasses scheduler wake materialization. For `PollingScheduler` deployments this is fine — the poller will claim the rows on its next cycle. For Posthook deployments, use `dk.retryJob()` so a webhook delivery is scheduled.

## Pruning old jobs

DelayKit does not prune completed or failed jobs automatically. Add a periodic cleanup based on your retention needs:

```sql
DELETE FROM delaykit.jobs
WHERE status IN ('completed', 'cancelled')
  AND completed_at < now() - interval '30 days';
```

Keep `failed` rows until you've triaged them or your alerting has processed them.

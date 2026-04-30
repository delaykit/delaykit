# Changelog

All notable changes to DelayKit are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Until v1.0,
minor releases may include breaking changes.

## 0.9.0 - 2026-04-30

### Fixed

- `SQLiteStore.connect(db)` (caller-owned `Database` instance) now
  honors ownership on shutdown: `store.close()` and
  `dk.stop({ closeStore: true })` leave the caller's connection open.
  Previously `close()` always called `db.close()` regardless of who
  opened it, which closed the app's own connection out from under it.
  The path-mode (`connect("/path/to.db")`) lifecycle is unchanged —
  delaykit owns and closes the database it opens.

### Added

- `SQLiteStore.connect(pathOrDb)` JSDoc documenting both modes
  explicitly (path-mode vs. caller-owned), the PRAGMAs delaykit
  applies (`journal_mode = WAL`, `busy_timeout = 5000`,
  `foreign_keys = ON`), and the table-name prefix
  (`delaykit_jobs`, `delaykit_migrations`) that lets app tables
  co-tenant on the same connection or file.

  This brings the `SQLiteStore` connect API to feature parity with
  `PostgresStore.connect(stringOrClient)`. Apps using the standard
  Bun + SQLite single-file shape can now open one `Database`, set
  PRAGMAs once, and share it between delaykit and their own domain
  tables — surfaced by the `bun-reminders` example build.

### Changed

- `SQLiteLikeTransactionFn<T>` no longer requires a `default` member.
  delaykit only ever invoked `.immediate()` or the bare call form;
  the `default` requirement was dead weight that blocked
  `bun:sqlite` Database instances from satisfying the interface.
  No runtime change. Custom `SQLiteLike` driver authors targeting
  the better-sqlite3 shape are unaffected (their `default` is still
  allowed, just no longer required).

## 0.8.0 - 2026-04-29

### Changed

- **BREAKING:** `dk.getJobByKey()` renamed to `dk.getActiveJobByKey()`.
  Behavior unchanged — terminal (fired, failed, cancelled) jobs return
  null, since the key may have been reused by a fresh schedule. The old
  name elided the active-only filter and surprised callers building HTTP
  read endpoints. The new name matches the `Store` contract method
  (`store.getActiveJobByKey`). Migration: rename the call site.

### Added

- JSDoc on `dk.getActiveJobByKey` clarifying the active-only filter and
  pointing to `getJob(id)` for status-agnostic lookup.
- JSDoc on the `PollingScheduler` constructor summarizing option
  defaults (`interval: 1000ms`, `stalledCheckInterval: 30000ms`,
  `maxConcurrent: 10`) so `new PollingScheduler()` is self-documenting
  on hover.
- `StopOptions.closeStore?: boolean` — when `true`, `dk.stop()` closes
  the store after the scheduler drains. Default `false` preserves the
  existing pattern (consumer manages store lifecycle; post-stop
  cleanup ops like `cancel`/`unschedule` remain available). Opt-in is
  convenient for long-running apps that own a dedicated store, where
  the canonical shutdown is now `await dk.stop({ closeStore: true })`
  instead of `await dk.stop(); await store.close();`.
- `dk.stop()` JSDoc clarifies that it is idempotent — concurrent or
  repeated calls await the same in-flight shutdown promise.
- `SQLiteStore.close()` is now idempotent — a second call is a no-op
  instead of throwing. (`MemoryStore.close()` and
  `PostgresStore.close()` were already idempotent.)
- `examples/bun-sqlite-server/` — minimal in-repo Bun + SQLite
  single-file server demonstrating schedule, lookup, cancel over HTTP.
  Uses `bun:sqlite` (no peer dep on Bun) and a one-call shutdown via
  `dk.stop({ closeStore: true })`.

## 0.7.1 - 2026-04-28

### Added

- `ConcurrentInsertError` exported from the package root. Stores throw
  this typed error from `createJob` when a concurrent insert wins the
  `(handler, key)` race; `dk.schedule`, `dk.debounce`, and `dk.throttle`
  use `instanceof` instead of string-matching. Custom store
  implementations should throw `ConcurrentInsertError` from `createJob`
  on unique-violation.
- `ClaimBatch` exported from the package root. Already the return type
  of `Store.claimDueJobs`; it now has a name on the public surface for
  custom-store authors.
- `PosthookSchedulerOptions.client?` — pre-constructed Posthook client.
  When provided, `apiKey` and `baseURL` are ignored. Useful for sharing
  one client across schedulers, or for injecting a stub in tests.
  Mirrors the `PostgresStore.connect(stringOrClient)` shape.

### Changed

- `dk.handle()` rejects `retry.attempts < 1` (and non-integer values)
  at registration. Previously a `retry.attempts: 0` config silently
  registered as "never run, always fail."

### Fixed

- `delaykit/posthook` no longer fails at module load with
  `ERR_MODULE_NOT_FOUND` when `@posthook/node` is not installed.
  The peer dependency is now lazy-loaded via `createRequire`; the
  constructor throws a clear "install @posthook/node" message
  instead. The Posthook client type is now declared structurally
  (`PosthookClient`) so the optional peer no longer leaks into
  `dist/schedulers/posthook.d.ts` — TypeScript consumers without
  `skipLibCheck` no longer see a stray `Cannot find module
  '@posthook/node'`.
- `createHandler()` validates that the verified delivery payload
  contains a string `jobId` before calling `store.getJob()`. A
  malformed payload (Posthook bug, replay across deploys with a
  schema mismatch) returns 401 instead of `TypeError`-ing.
- `MemoryStore`'s eviction interval is `unref()`'d so a forgotten
  `close()` in tests, REPLs, and CLI scripts no longer pins the
  event loop.
- `JobEventEmitter.emit` snapshots its listener `Set` before
  iterating. A listener that calls `unsubscribe()` on itself or
  another listener mid-dispatch no longer mutates the iteration.
- `runRaceMode` and `dk.poll()`'s deadline race attach defensive
  `.catch(() => {})` to orphan promise tails so a future regression
  to inner error-swallowing can't surface as an unhandled rejection.

## 0.7.0 - 2026-04-28

### Added

- `SQLiteStore`, exported at `delaykit/sqlite`. Implements the full
  `Store` contract with timestamps stored as INTEGER ms, partial
  unique index on `(handler, key)`, and `claimDueJobs` running inside
  a `BEGIN IMMEDIATE` transaction. Auto-migrates on first connect.
  Single-process — one `PollingScheduler` per file.
- Runtime-detected SQLite driver: `bun:sqlite` under Bun, `better-sqlite3`
  under Node (optional peer dependency). Users who want a specific
  driver can construct a `Database` themselves and pass it to
  `SQLiteStore.connect`.
- `runSQLiteMigrations(path | Database)` helper for deploy-time or
  one-shot migration runs, mirroring `runPostgresMigrations`.
- Bun runtime support confirmed end-to-end — all existing suites pass
  under Bun with no code changes. New `test:bun` script runs the SQLite
  contract under Bun's native test runner.
- Positioning update: "Durable wake-ups for TypeScript apps and agents."
  README reorganized around store choice (SQLite or Postgres) and
  scheduler choice, with long-running processes as the default
  deployment path.

### Changed (breaking)

- Postgres exports renamed for symmetry with the new SQLite exports:
  `runMigrations` → `runPostgresMigrations`, `LATEST_MIGRATION_VERSION`
  → `LATEST_POSTGRES_MIGRATION_VERSION`. The `runMigrations: false`
  option key on `PostgresStore.connect()` and `SQLiteStore.connect()`
  is unchanged.

## 0.6.0 - 2026-04-17

### Added

- `dk.stats()` returns a `DelayKitStats` snapshot: counts for
  `pending`, `duePending`, `running`, and `deferred` jobs; `failed24h`
  for the last 24 hours; `oldestDuePending` and `oldestRunning` with
  the id, handler, and timestamp of the oldest row in each bucket; and
  `byHandler` — the same counters broken out per handler, sorted
  alphabetically, omitting handlers with all-zero counts.
  `duePending` (and `byHandler[].duePending`) excludes unsettled
  debounce rows whose wait window hasn't closed yet, matching the
  backlog that `claimDueJobs` would actually pick up.
  `Store` gains a `stats()` method; custom store implementations must
  add it.
- `job:deferred` event — emitted each time a job is deferred because
  its handler is not registered on any live process. Carries
  `deferAttempts` (total defer steps so far) and `nextAttemptAt`.
  Complements `job:failed` with `reason: "defer_horizon"` for the
  terminal case.
- `JobFailedEvent.reason` discriminant: `"handler_error"` (normal
  exhaustion), `"timeout"` (handler exceeded its timeout), or
  `"defer_horizon"` (handler was never registered within the defer
  horizon). Allows alerting rules to distinguish silent timeouts from
  code errors.
- `dk.retryJob(id)` resets a `failed` job to `pending` with a fresh
  attempt budget (`attempt=0`, `scheduledFor=now()`, `version`
  bumped). Pattern fields and `retryConfig` are preserved so debounce
  and throttle rows retain their window shape and Posthook retry
  configuration. Returns the updated `Job`, or `null` if the job
  doesn't exist or isn't `failed`. If scheduler wake materialization
  fails after the DB mutation, the row is flipped back to `failed`.
  `Store` gains a `resetJob(id)` method; custom store implementations
  must add it.
- `PollingScheduler` now adds ±25% jitter to its exponential poll-error
  backoff so concurrent replicas stagger their retry bursts after a DB
  outage. The 30-second ceiling is enforced after jitter.

### Fixed

- `schedule()`, `debounce()`, and `throttle()` now reject keys that are
  empty or contain only whitespace, preventing silent no-ops from strings
  like `"   "` that pass an empty-check but produce a meaningless row.

## 0.5.0 - 2026-04-16

### Added

- Multi-instance polling. `PollingScheduler` instances sharing one
  store claim disjoint job sets via `FOR UPDATE SKIP LOCKED`, so
  throughput scales with replicas. `maxConcurrent` is per-instance;
  the cluster ceiling is `N × maxConcurrent`. For a strict global
  cap, run one instance.
- `Store.unknownDueHandlers(knownHandlers)` — returns distinct
  handler names of due rows this replica can't process.
  `PollingScheduler`'s stall sweep and `dk.poll()` call it and log
  a warning, so operators notice when rows are due for handlers no
  replica in the cluster has registered.

### Changed

- **Breaking (custom Stores only):** `Store.getDueJobs` is replaced
  by `Store.claimDueJobs(budget, handlerNames)`, which returns
  `ClaimBatch = { toRun, rescheduled }` in one round-trip. Settled
  rows are flipped to `running` and land in `toRun`; un-settled
  debounce rows have their `scheduled_for` atomically advanced and
  land in `rescheduled` (caller materializes a fresh wake for each).
  Rows whose handler is not in `handlerNames` are never claimed —
  handler availability is replica-local, so those rows stay pending
  and remain available for any replica that can run them.

## 0.4.0 - 2026-04-14

### Changed

- A delivery whose handler isn't registered on the current process is
  now deferred with exponential backoff (5s → 5min cap) instead of
  marked `failed`. After the defer horizon (default 24h) the row
  transitions directly to `failed` with a `job:failed` event.
- `Store` gains a `deferJob` method. Custom store implementations must
  add it.
- `Job` gains `deferAttempts`, `deferredSince`, and `retryConfig`
  fields. Postgres migrations 3 and 4 add the corresponding columns;
  both run automatically on `PostgresStore.connect()` unless
  `runMigrations: false` is set.
- `stop()` is terminal. After `stop()` begins, `schedule`, `debounce`,
  `throttle`, `poll`, `createHandler`, and `start` throw; `cancel`
  and `unschedule` remain allowed for cleanup. Recovery from a
  shutdown error is instantiating a new `DelayKit`.
- `stop()` without `drainMs` now waits up to
  `max(handler timeouts) + STALLED_GRACE_MS` for in-flight handlers
  instead of returning immediately. Pass `drainMs: 0` to opt out.
  Platform grace periods tighter than the handler bound require an
  explicit `drainMs`.
- The webhook handler returned by `createHandler()` returns HTTP 500
  after `stop()` so the external scheduler redelivers to a healthy
  instance.
- Concurrent `stop()` calls share one in-flight shutdown.
- Exponential-backoff `retry.maxDelay` now defaults to `1h` when
  unset. Prevents `initialDelay * 2^attempts` from scheduling
  retries hours or days apart at high attempt counts. Fixed and
  linear backoff have no runaway case and receive no implicit cap.
  Explicit `maxDelay` overrides are still honored.
- `Job.lastError` is truncated to 2048 characters on every store
  write path (`createJob`, `markFailed`, `retryJob`, `deferJob`).
  Handlers that throw errors carrying huge serialized payloads no
  longer bloat DB rows.
- `Store` gains a `pruneTerminal(olderThan, limit?)` method that
  deletes terminal rows (`completed`, `failed`, `cancelled`) whose
  `completedAt < olderThan`. Returns the number of rows deleted.
  When `limit` is provided, deletes oldest-first in batches — use
  this for scheduled retention jobs that shouldn't lock the table.
  Throws when `limit <= 0`. Custom store implementations must add
  it.
- Postgres migration 5 adds `idx_jobs_completed_at` — a partial
  index on `completed_at` scoped to terminal statuses — so retention
  queries don't seq-scan the table. Runs automatically on
  `PostgresStore.connect()` unless `runMigrations: false`.
- `dk.schedule({ at })` rejects invalid Date values: `NaN` Dates
  (e.g. `new Date("not a date")`) and Dates more than 10 years in
  the future (almost always a unit mistake — seconds passed as ms,
  wrong year). Past Dates remain valid and fire on the next poll.
- Concurrent migrators now serialize via a Postgres advisory lock
  held on a reserved connection. On Vercel, a traffic spike right
  after deploy no longer produces dozens of cold starts racing on
  the same DDL.
- `PostgresStore.connect(sql, { runMigrations: false })` now
  verifies the schema is caught up and throws with a clear error
  if not, instead of producing opaque "column does not exist"
  errors at the first query. Safety net for the deploy-time
  migration pattern (see below).
- Every DelayKit release now ships migrations backwards-compatible
  with the previous release's code, so rolling deploys (Vercel and
  similar) don't break old pods during rollover. Removals span two
  releases. See `CONTRIBUTING.md` → Schema changes.

### Added

- `DelayKitOptions.deferHorizon` — duration string controlling the
  missing-handler defer horizon. Default `"24h"`.
- `runMigrations(urlOrClient)` exported from `delaykit/postgres`.
  Intended for deploy-time use (e.g. a `postbuild` script) so the
  app can set `runMigrations: false` on `PostgresStore.connect()`
  and skip request-time migration (a cheap version check still
  runs, so a mis-wired deploy fails loudly). Accepts either a
  connection string (short-lived client, closed after migrations
  apply) or an existing `postgres.js` client (caller owns the
  lifecycle).
- `LATEST_MIGRATION_VERSION` exported from `delaykit/postgres` for
  tools that want to check the schema version out-of-band.

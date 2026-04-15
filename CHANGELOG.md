# Changelog

All notable changes to DelayKit are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Until v1.0,
minor releases may include breaking changes.

## Unreleased

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

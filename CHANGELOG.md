# Changelog

All notable changes to DelayKit are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Until v1.0,
minor releases may include breaking changes.

## Unreleased

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

### Added

- `DelayKitOptions.deferHorizon` — duration string controlling the
  missing-handler defer horizon. Default `"24h"`.

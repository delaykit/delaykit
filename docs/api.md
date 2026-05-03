# API reference

DelayKit's TypeScript types are the canonical reference. Hover any method in your editor for full signatures and inline docstrings. The table below is for at-a-glance lookup.

| Method | Description |
|--------|-------------|
| `dk.handle(name, handler)` | Register a handler (before start/poll/createHandler) |
| `dk.schedule(handler, opts)` | Schedule a one-time job |
| `dk.debounce(handler, opts)` | Debounce rapid events into one handler call |
| `dk.throttle(handler, opts)` | Throttle to one handler call per time window |
| `dk.cancel(id)` | Cancel a pending job by ID |
| `dk.unschedule(handler, key)` | Cancel by handler and key |
| `dk.getJob(id)` | Look up a job by ID |
| `dk.getActiveJobByKey(handler, key)` | Look up the active job for a handler + key. Returns null for terminal jobs (fired, failed, cancelled). |
| `dk.stats()` | Snapshot of job counts by status, with per-handler breakdown |
| `dk.retryJob(id)` | Reactivate a failed job with a fresh attempt budget |
| `dk.listFailed(opts)` | Page through failed jobs for triage |
| `dk.retryFailed(opts)` | Bulk retry failed jobs by filter or IDs, with staggered scheduling |
| `dk.start()` | Begin continuous polling (long-running process) |
| `dk.stop(options?)` | Drain in-flight handlers, then stop |
| `dk.poll(opts?)` | Run one poll cycle (for cron routes) |
| `dk.createHandler()` | Create a webhook route handler (for external schedulers) |
| `dk.on(event, listener)` | Subscribe to lifecycle events |

## Duration format

Delays and timeouts use human-readable strings: `"5s"`, `"30m"`, `"24h"`, `"14d"`, `"500ms"`. Compound durations work too: `"1h30m"`.

| Unit | Example |
|------|---------|
| `ms` | `"500ms"` |
| `s` | `"30s"` |
| `m` | `"5m"` |
| `h` | `"24h"` |
| `d` | `"14d"` |

## Behavior

### `dk.handle(name, config)`

Beyond `dk.handle(name, fn)`, you can pass a config object to set per-handler timeout, retry policy, and a failure callback:

```typescript
dk.handle("send-email", {
  handler: async ({ key, signal }) => { /* ... */ },
  timeout: "10s",
  retry: { attempts: 5, backoff: "exponential", initialDelay: "1s", maxDelay: "1m", jitter: true },
  onFailure: async ({ key, error, attempts }) => alerts.notify(`${key}: ${error.message}`),
});
```

- **`timeout`** — handler deadline. When it fires, `ctx.signal` is aborted; pass it through to whatever the handler calls (`fetch`, `pg`, etc.) so it can exit cleanly. Default `30s`.
- **`retry.attempts`** — total attempts including the first run (`1` = no retry; `5` = up to five tries).
- **`retry.backoff`** — `"exponential"` (capped at `30s` by default), `"linear"`, or `"fixed"`. Linear and fixed have no default cap.
- **`retry.initialDelay`** / **`retry.maxDelay`** — duration strings bounding the wait between retries.
- **`retry.jitter`** — adds ±25% randomness to each delay. Default `false`.
- **`onFailure`** — fires once per job after retries are exhausted. Errors thrown from `onFailure` are swallowed so they don't mask the original failure.

### `dk.schedule(handler, { key, delay | at, onDuplicate? })`

Default `onDuplicate` is `"skip"`. If a pending or running row exists for the same `(handler, key)`, returns the existing row with `created: false`.

With `onDuplicate: "replace"`:
- If the existing row is **pending** — atomically bumps version, updates `scheduledFor`, materializes a new wake, and best-effort cancels the old artifact. Returns the replaced row with `created: true`.
- If the existing row is **running** — returns `created: false` with `skippedReason: "running"`. The handler owns the row until terminal; use `ctx.reschedule({ delay, at })` from inside the handler to reschedule the current run.

A `kind` mismatch (e.g., scheduling `once` over an active `debounce`) throws.

### `ctx.reschedule({ delay | at })`

Reschedule the current run from inside the handler — for "poll until done" patterns where you don't yet know how long the work will take. The row transitions `running → pending` with the supplied `scheduledFor` after the handler returns successfully, instead of `running → completed`. Throwing from the handler discards the intent and falls through to normal retry/failure logic. `attempt` resets to `0` on the next delivery — a rescheduled run is treated as a completed checkpoint, not a consumed retry.

Currently scoped to `kind: "once"` jobs. Calling on a debounce or throttle pattern handler throws synchronously — those flows have their own requeue semantics via the pattern wait/maxWait window. See [`examples/posthook-poll-until-done.ts`](../examples/posthook-poll-until-done.ts) for a working example.

### `dk.debounce(handler, { key, wait, maxWait? })`

Trailing-edge debounce. Each call extends the window by `wait` from now. If no further calls arrive within the window, the handler runs once.

Returns `{ settlesAt }` — when the debounce will fire if no further calls are made on this key. With `maxWait`, the window is clamped: bursts longer than `maxWait` from the first event settle at `firstAt + maxWaitMs` regardless of further calls.

If a call arrives while the previous handler is still running, version bumps and `lastAt` updates. The terminal transition automatically requeues a fresh window — no event is lost.

### `dk.throttle(handler, { key, wait })`

Fixed-window throttle. The first call schedules a wake at `now + wait`. Further calls within the window are coalesced — `lastAt` updates but no new wake is scheduled. The handler runs once at the end of the window.

### `dk.cancel(id)` and `dk.unschedule(handler, key)`

Cancels a pending row. Best-effort cancels the scheduler artifact; if delivery still arrives, it is rejected by the artifact-identity guard. Running rows cannot be cancelled — they own their lifecycle until terminal.

For the correctness model that backs these behaviors (atomic claim, version semantics, stale-wake rejection), see [`docs/invariants.md`](invariants.md).

## Lifecycle events

DelayKit emits structured lifecycle events. See the events table and subscription examples in the [Observability section of the README](../README.md#observability).

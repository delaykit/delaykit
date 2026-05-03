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

### `dk.schedule(handler, { key, delay | at, onDuplicate? })`

Default `onDuplicate` is `"skip"`. If a pending or running row exists for the same `(handler, key)`, returns the existing row with `created: false`.

With `onDuplicate: "replace"`:
- If the existing row is **pending** — atomically bumps version, updates `scheduledFor`, materializes a new wake, and best-effort cancels the old artifact. Returns the replaced row with `created: true`.
- If the existing row is **running** — returns `created: false` with `skippedReason: "running"`. The handler owns the row until terminal; use `ctx.reschedule({ delay, at })` from inside the handler to reschedule the current run.

A `kind` mismatch (e.g., scheduling `once` over an active `debounce`) throws.

### `dk.debounce(handler, { key, wait, maxWait? })`

Trailing-edge debounce. Each call extends the window by `wait` from now. If no further calls arrive within the window, the handler runs once.

Returns `{ settlesAt }` — when the debounce will fire if no further calls are made on this key. With `maxWait`, the window is clamped: bursts longer than `maxWait` from the first event settle at `firstAt + maxWaitMs` regardless of further calls.

If a call arrives while the previous handler is still running, version bumps and `lastAt` updates. The terminal transition automatically requeues a fresh window — no event is lost.

### `dk.throttle(handler, { key, wait })`

Fixed-window throttle. The first call schedules a wake at `now + wait`. Further calls within the window are coalesced — `lastAt` updates but no new wake is scheduled. The handler runs once at the end of the window.

### `dk.cancel(id)` and `dk.unschedule(handler, key)`

Cancels a pending row. Best-effort cancels the scheduler artifact; if delivery still arrives, it is rejected by the artifact-identity guard. Running rows cannot be cancelled — they own their lifecycle until terminal.

For the correctness model that backs these behaviors (atomic claim, version semantics, stale-wake rejection), see [`docs/INVARIANTS.md`](INVARIANTS.md).

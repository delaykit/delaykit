# Contributing to DelayKit

Thanks for your interest in contributing. This document covers project layout, how to run the tests, and the conventions to follow when modifying the code.

## Running tests

```bash
npm test                  # Unit tests (no Postgres needed)
npm run test:postgres     # Postgres tests (requires `docker compose up`)
npm run test:packaging    # npm pack + bare import smoke test
npm run test:all          # Everything
npm run typecheck         # tsc --noEmit
```

## Project layout

- `src/delaykit.ts` — main class: `handle`, `schedule`, `debounce`, `throttle`, `cancel`, `unschedule`, `poll`, `createHandler`, `on`
- `src/types.ts` — `Job`, `Store`, `Scheduler`, event, and retry interfaces
- `src/executor.ts` + `src/result-handler.ts` — claim a job, run the handler, decide complete/retry/fail
- `src/emitter.ts` — lifecycle event emitter
- `src/duration.ts` — duration string parser (`"5s"`, `"1h30m"`)
- `src/stores/` — `MemoryStore` (dev) and `PostgresStore` (production) + migrations
- `src/schedulers/` — `PollingScheduler` and `PosthookScheduler`
- `test/` — Vitest suites. `store-contract.ts` and `scheduler-contract.ts` are shared contract suites run against every store/scheduler implementation.
- `examples/` — runnable demos for each store
- `docs/INVARIANTS.md` — correctness model: read this before changing execution, delivery, or store logic

## Key design decisions

- **Uniqueness is (handler, key)** — different handlers can use the same key. The key is an entity ID, not a namespaced string.
- **Store is the source of truth** — scheduler wakes are disposable signals. The row decides whether to execute.
- **No generic `updateJob`** — all store mutations are purpose-built with CAS guards. No `Partial<Job>` escape hatch.
- **Handlers check fresh state** — no payloads stored in jobs. Handlers receive the key and fetch current data.
- **`schedulerRef` identity guard** — delivery validation checks `hookId` against the row's `schedulerRef` before executing.

## When modifying

- Read `docs/INVARIANTS.md` before changing execution, delivery, or store logic.
- If a test fails, assume the code is wrong first. Only weaken a test after verifying the invariant itself is incorrect.
- Handler names must be `[a-zA-Z0-9_-]` — they become URL path segments for `PosthookScheduler`.
- All `as any` casts have been eliminated from `src/`. Keep it that way.

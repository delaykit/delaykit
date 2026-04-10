# DelayKit — Agent Context

## What this project is

DelayKit is a TypeScript library for scheduling delayed actions in Next.js apps — reminders, expirations, follow-ups, debounced saves. Jobs are stored in Postgres and survive restarts.

## Project structure

```
src/
  delaykit.ts          — Main class: handle, schedule, debounce, throttle, cancel, unschedule, poll, createHandler, on
  types.ts             — All interfaces: Job, Store, Scheduler, events, retry config
  executor.ts          — executeJob: claims a job and runs the handler
  result-handler.ts    — Post-execution logic: complete, retry, requeue, fail
  emitter.ts           — Lifecycle event emitter (7 event types)
  duration.ts          — Duration string parser ("5s", "24h", "1h30m")
  stores/
    memory.ts          — In-memory store (dev/testing only)
    postgres.ts        — PostgreSQL store (production)
    postgres-migrations.ts — Schema migrations
  schedulers/
    polling.ts         — PollingScheduler: polls store on interval or via poll()
    posthook.ts        — PosthookScheduler: webhook delivery via Posthook
test/
  store-contract.ts    — Shared store tests run against both MemoryStore and PostgresStore
  scheduler-contract.ts — Shared scheduler tests run against both transports
  race-conditions.test.ts — Interleaving and transport artifact tests
  events.test.ts       — Lifecycle event integration tests
  helpers/             — Job factory, invariants, external scheduler harness, barriers
examples/
  basic.ts             — Local dev with MemoryStore
  postgres.ts          — Local dev with PostgresStore
  e2e-memory.ts        — End-to-end smoke test (MemoryStore)
  e2e-postgres.ts      — End-to-end smoke test (PostgresStore)
docs/
  INVARIANTS.md        — Correctness model: what the system must guarantee
```

## Key design decisions

- **Uniqueness is (handler, key)** — different handlers can use the same key. The key is an entity ID, not a namespaced string.
- **Store is the source of truth** — scheduler wakes are disposable signals. The row decides whether to execute.
- **No generic updateJob** — all store mutations are purpose-built with CAS guards. No `Partial<Job>` escape hatch.
- **Handlers check fresh state** — no payloads stored in jobs. Handlers receive the key and fetch current data.
- **schedulerRef identity guard** — delivery validation checks hookId against row's schedulerRef before executing.

## Running tests

```bash
npm test                  # Unit tests (no Postgres needed)
npm run test:postgres     # Postgres tests (requires docker compose up)
npm run test:packaging    # npm pack + bare import smoke test
npm run test:all          # Everything
npm run typecheck         # tsc --noEmit
```

## When modifying

- Read `docs/INVARIANTS.md` before changing execution, delivery, or store logic.
- If a test fails, assume the code is wrong first. Only weaken a test after verifying the invariant itself is incorrect.
- Handler names must be `[a-zA-Z0-9_-]` — they become URL path segments for PosthookScheduler.
- All `as any` casts have been eliminated from src/. Keep it that way.

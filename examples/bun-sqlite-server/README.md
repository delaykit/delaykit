# bun-sqlite-server

Minimal DelayKit example. A Bun server backed by SQLite. Schedule a reminder, check its status, cancel it, watch it fire.

## Run

This example imports `delaykit` from the parent repo via a `file:../..` dependency, so a fresh clone needs the parent built first. From this directory:

```bash
bun run setup        # one-time: build delaykit's dist/, install deps
bun run server.ts
```

After the first setup, iterate with `bun run server.ts`. The server listens on `http://localhost:3000` and writes to `./delaykit.db`.

## Try it

```bash
# Schedule a reminder for 10 seconds from now
curl -X POST http://localhost:3000/reminders \
  -H "content-type: application/json" \
  -d '{"key":"user_123","delay":"10s"}'

# Check it's still pending (returns 404 once it fires or is cancelled)
curl http://localhost:3000/reminders/user_123

# Cancel it before it fires
curl -X DELETE http://localhost:3000/reminders/user_123
```

If you don't cancel, you'll see `reminder fired for user_123` logged when the delay elapses. The job survives restarts — kill the server with Ctrl-C, restart it, and pending reminders are still there.

## What it shows

- `SQLiteStore` with `bun:sqlite` (no peer dependency on Bun).
- `PollingScheduler` driving handler execution from a long-running process.
- `dk.schedule` / `dk.getActiveJobByKey` / `dk.unschedule` over HTTP.
- Graceful shutdown on SIGTERM/SIGINT.

## Configure

| Env var             | Default          | Purpose                                                                  |
|---------------------|------------------|--------------------------------------------------------------------------|
| `DELAYKIT_DB_PATH`  | `./delaykit.db`  | SQLite file path. Set to a persistent volume mount in cloud deployments. |
| `PORT`              | `3000`           | HTTP port.                                                               |

## See also

- The polished, UI-bearing version: [`delaykit/bun-reminders`](https://github.com/delaykit/bun-reminders) (separate repo).
- Patterns reference: [delaykit.dev/patterns](https://delaykit.dev/patterns).

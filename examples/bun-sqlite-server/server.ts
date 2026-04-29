/**
 * Minimal DelayKit example: a Bun server backed by SQLite.
 * Schedule a reminder, check its status, cancel it, watch it fire.
 *
 * Run: `bun run setup` once, then `bun run server.ts`. See README.md.
 */

import { DelayKit } from "delaykit";
import type { Job } from "delaykit";
import { SQLiteStore } from "delaykit/sqlite";
import { PollingScheduler } from "delaykit/polling";

const HANDLER = "send-reminder";
const dbPath = process.env.DELAYKIT_DB_PATH ?? "./delaykit.db";
const port = Number(process.env.PORT ?? 3000);

const store = await SQLiteStore.connect(dbPath);
const dk = new DelayKit({
  store,
  scheduler: new PollingScheduler(),
});

dk.handle(HANDLER, async ({ key }) => {
  console.log(`[${new Date().toISOString()}] reminder fired for ${key}`);
  // In a real app: fetch user state, send email/notification, etc.
});

await dk.start();

const toDTO = (job: Job) => ({
  id: job.id,
  key: job.key,
  status: job.status,
  scheduledFor: job.scheduledFor,
  attempt: job.attempt,
});

const server = Bun.serve({
  port,
  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/reminders") {
      const body = (await req.json()) as { key?: string; delay?: string };
      if (!body.key || !body.delay) {
        return Response.json({ error: "key and delay required" }, { status: 400 });
      }
      const { job, created } = await dk.schedule(HANDLER, {
        key: body.key,
        delay: body.delay,
      });
      return Response.json({ ...toDTO(job), created }, { status: created ? 201 : 200 });
    }

    const match = url.pathname.match(/^\/reminders\/(.+)$/);
    if (match) {
      const key = decodeURIComponent(match[1]!);

      if (req.method === "GET") {
        const job = await dk.getActiveJobByKey(HANDLER, key);
        if (!job) return Response.json({ error: "not found" }, { status: 404 });
        return Response.json(toDTO(job));
      }

      if (req.method === "DELETE") {
        const cancelled = await dk.unschedule(HANDLER, key);
        return Response.json({ cancelled });
      }
    }

    return Response.json({ error: "not found" }, { status: 404 });
  },
});

console.log(`Listening on http://localhost:${port} (db: ${dbPath})`);

const shutdown = async () => {
  console.log("\nShutting down...");
  await server.stop();
  await dk.stop({ closeStore: true }); // idempotent; closes the store too
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

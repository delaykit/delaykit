/**
 * Smoke test: `ctx.reschedule` with PosthookScheduler.
 *
 * Wires a single "check-job" handler that pretends to poll an external
 * async job (Replicate prediction, OpenAI batch, Mux asset, etc.). The
 * fake API returns "pending" on the first two deliveries, then
 * "succeeded". Each pending response calls
 * `ctx.reschedule({ delay: "10s" })`; the succeeded response returns
 * and marks the row complete.
 *
 * Requirements:
 * - A Posthook account (https://posthook.io)
 * - A publicly-reachable URL — easiest path: ngrok or cloudflared
 *     `ngrok http 3030`
 *     `cloudflared tunnel --url http://localhost:3030`
 * - In your Posthook project settings, configure the webhook destination
 *   URL to point at your tunnel (e.g., `https://abc123.ngrok-free.app`).
 *   Posthook delivers to `<destination>${basePath}/<handler>`; this
 *   example uses `basePath = "/api/delaykit"`.
 *
 * Run:
 *   POSTHOOK_API_KEY=ph_xxx \
 *   POSTHOOK_SIGNING_KEY=phk_xxx \
 *   PORT=3030 \
 *   npx tsx examples/posthook-poll-until-done.ts
 *
 * Expected output (rough timing):
 *   t=0    [main] scheduled check-job
 *   t=5s   [event] job:started ... [fake-api] check #1 ... [event] job:rescheduled
 *   t=15s  [event] job:started ... [fake-api] check #2 ... [event] job:rescheduled
 *   t=25s  [event] job:started ... [fake-api] check #3 ... [event] job:completed
 */

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { DelayKit } from "../src/index.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PosthookScheduler } from "../src/schedulers/posthook.js";

const apiKey = process.env.POSTHOOK_API_KEY;
const signingKey = process.env.POSTHOOK_SIGNING_KEY;
const port = Number(process.env.PORT ?? 3030);
const basePath = "/api/delaykit";

if (!apiKey || !signingKey) {
  console.error(
    "Missing required env vars: POSTHOOK_API_KEY, POSTHOOK_SIGNING_KEY",
  );
  process.exit(1);
}

// Fake external job: 2 polls return "pending", then "succeeded".
let pollCount = 0;
function fetchExternalStatus(): "pending" | "succeeded" {
  pollCount++;
  console.log(`[fake-api] check #${pollCount}`);
  return pollCount >= 3 ? "succeeded" : "pending";
}

const store = new MemoryStore();
const scheduler = new PosthookScheduler({
  apiKey,
  signingKey,
  basePath,
});
const dk = new DelayKit({ store, scheduler });

dk.handle("check-job", async ({ key, reschedule }) => {
  const status = fetchExternalStatus();
  console.log(`[handler] key=${key} status=${status}`);
  if (status === "succeeded") {
    console.log(`[handler] done — returning normally, row will mark completed`);
    return;
  }
  reschedule({ delay: "10s" });
  console.log(`[handler] not done — rescheduled +10s`);
});

dk.on("job:scheduled", (e) =>
  console.log(`[event] job:scheduled key=${e.job.key} at=${e.job.scheduledFor.toISOString()}`),
);
dk.on("job:started", (e) =>
  console.log(`[event] job:started key=${e.job.key} attempt=${e.attempt}`),
);
dk.on("job:rescheduled", (e) =>
  console.log(
    `[event] job:rescheduled key=${e.job.key} next=${e.scheduledFor.toISOString()} duration=${e.durationMs}ms`,
  ),
);
dk.on("job:completed", (e) =>
  console.log(`[event] job:completed key=${e.job.key} duration=${e.durationMs}ms`),
);
dk.on("job:failed", (e) =>
  console.log(`[event] job:failed key=${e.job.key} reason=${e.reason} error=${e.error.message}`),
);

const dkHandler = dk.createHandler();

const server = createServer(async (req, res) => {
  if (req.method !== "POST" || !req.url?.startsWith(basePath)) {
    res.writeHead(404).end();
    return;
  }
  try {
    const webReq = await toWebRequest(req);
    const webRes = await dkHandler(webReq);
    await writeWebResponse(webRes, res);
  } catch (err) {
    console.error("[server] handler error:", err);
    res.writeHead(500).end();
  }
});

async function toWebRequest(req: IncomingMessage): Promise<Request> {
  const host = req.headers.host ?? "localhost";
  const url = new URL(req.url!, `http://${host}`);
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const body = Buffer.concat(chunks).toString("utf8");
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === "string") headers.set(k, v);
    else if (Array.isArray(v)) headers.set(k, v.join(", "));
  }
  return new Request(url, {
    method: req.method,
    headers,
    body: body.length ? body : undefined,
  });
}

async function writeWebResponse(webRes: Response, res: ServerResponse): Promise<void> {
  const headers: Record<string, string> = {};
  webRes.headers.forEach((v, k) => { headers[k] = v; });
  res.writeHead(webRes.status, headers);
  res.end(await webRes.text());
}

async function main(): Promise<void> {
  await new Promise<void>((resolve) => server.listen(port, () => resolve()));
  console.log(`[server] listening on http://localhost:${port}${basePath}`);
  console.log(`[server] expecting Posthook to deliver to <project-webhook-url>${basePath}/<handler>`);

  const key = `prediction:${Date.now()}`;
  await dk.schedule("check-job", { key, delay: "5s" });
  console.log(`[main] scheduled check-job key=${key} (first delivery in ~5s)`);
  console.log(`[main] Ctrl-C to stop\n`);

  process.on("SIGINT", async () => {
    console.log("\n[main] shutting down...");
    await dk.stop({ drainMs: 5_000 });
    server.close();
    await store.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("[main] fatal:", err);
  process.exit(1);
});

/**
 * DelayKit soak test — schedules a sustained job rate against
 * `PostgresStore` + `PollingScheduler` and asserts memory, connection
 * count, backlog, and tail latency stay within bounds.
 *
 * Run before tagging a release:
 *
 *   docker compose up -d
 *   DATABASE_URL=postgres://delaykit:delaykit@localhost:5444/delaykit_test \
 *     npm run soak
 *
 * Env knobs:
 *   DATABASE_URL          Postgres URL (required for non-default targets)
 *   SOAK_DURATION_MS      total run length (default 1_800_000 = 30min)
 *   SOAK_RATE_PER_SEC     schedules per second (default 17 = ~1000/min)
 *   SOAK_SAMPLE_INTERVAL  sample interval in ms (default 30_000)
 */

import process from "node:process";
import postgres from "postgres";
import { DelayKit } from "../src/index.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

const DATABASE_URL =
  process.env.DATABASE_URL ??
  "postgres://delaykit:delaykit@localhost:5444/delaykit_test";
const SOAK_DURATION_MS = Number(process.env.SOAK_DURATION_MS ?? 30 * 60_000);
const RATE_PER_SEC = Number(process.env.SOAK_RATE_PER_SEC ?? 17);
const SAMPLE_INTERVAL_MS = Number(process.env.SOAK_SAMPLE_INTERVAL ?? 30_000);
const POLL_INTERVAL_MS = 250;
const POLL_MAX_CONCURRENT = 25;

interface Sample {
  idx: number;
  tSec: number;
  rssMb: number;
  connections: number;
  pending: number;
  fired: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

function pad(n: number | string, w: number): string {
  return String(n).padStart(w);
}

function fmtRow(s: Sample): string {
  return (
    `[${pad(s.idx, 2)}] t=${pad(s.tSec, 4)}s ` +
    `rss=${pad(s.rssMb, 3)}MB connections=${pad(s.connections, 2)} ` +
    `pending=${pad(s.pending, 4)} fired=${pad(s.fired, 6)} ` +
    `p50=${pad(s.p50Ms, 4)}ms p95=${pad(s.p95Ms, 4)}ms p99=${pad(s.p99Ms, 4)}ms`
  );
}

async function main() {
  console.log(
    `DelayKit soak: ${SOAK_DURATION_MS / 1000}s @ ${RATE_PER_SEC}/sec, ` +
      `sample every ${SAMPLE_INTERVAL_MS / 1000}s`,
  );
  console.log(`DATABASE_URL=${DATABASE_URL}\n`);

  const store = await PostgresStore.connect(DATABASE_URL);
  const monitor = postgres(DATABASE_URL, { max: 1 });
  const dk = new DelayKit({
    store,
    scheduler: new PollingScheduler({
      interval: POLL_INTERVAL_MS,
      maxConcurrent: POLL_MAX_CONCURRENT,
    }),
  });

  let firedCount = 0;
  let scheduledCount = 0;
  let scheduleErrors = 0;
  let windowLatencies: number[] = [];

  dk.handle("soak", async () => {
    firedCount++;
  });

  dk.on("job:started", (e) => {
    windowLatencies.push(Date.now() - e.job.scheduledFor.getTime());
  });

  await dk.start();

  const startMs = Date.now();
  const startRssMb = Math.round(process.memoryUsage().rss / 1_048_576);
  const samples: Sample[] = [];

  // Schedule loop: dispatch RATE_PER_SEC schedules per 1s tick.
  // Errors are counted, not thrown — this loop must keep cadence.
  const scheduleTimer = setInterval(() => {
    for (let i = 0; i < RATE_PER_SEC; i++) {
      const id = ++scheduledCount;
      dk.schedule("soak", { key: `soak-${id}`, delay: "0s" }).catch(() => {
        scheduleErrors++;
      });
    }
  }, 1_000);

  const sampleTimer = setInterval(async () => {
    const tSec = Math.round((Date.now() - startMs) / 1000);
    const rssMb = Math.round(process.memoryUsage().rss / 1_048_576);
    const sorted = [...windowLatencies].sort((a, b) => a - b);
    windowLatencies = [];

    let connections = 0;
    try {
      const [{ count }] = await monitor`
        SELECT count(*)::int AS count FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
      `;
      connections = count;
    } catch {
      /* monitor blip — leave connections=0 for this sample */
    }

    let pending = 0;
    try {
      pending = (await dk.stats()).pending;
    } catch {
      /* stats blip — leave pending=0 for this sample */
    }

    const sample: Sample = {
      idx: samples.length + 1,
      tSec,
      rssMb,
      connections,
      pending,
      fired: firedCount,
      p50Ms: pct(sorted, 0.5),
      p95Ms: pct(sorted, 0.95),
      p99Ms: pct(sorted, 0.99),
    };
    samples.push(sample);
    console.log(fmtRow(sample));
  }, SAMPLE_INTERVAL_MS);

  await new Promise((r) => setTimeout(r, SOAK_DURATION_MS));

  clearInterval(scheduleTimer);
  console.log("\n[shutdown] scheduling stopped, draining...");
  await new Promise((r) => setTimeout(r, 2_000));
  clearInterval(sampleTimer);

  await dk.stop();
  await store.close();
  await monitor.end();

  // Report + assertions
  const finalRssMb = samples.length
    ? samples[samples.length - 1].rssMb
    : startRssMb;
  const rssGrowthPct = ((finalRssMb - startRssMb) / startRssMb) * 100;
  const conns = samples.map((s) => s.connections);
  const minConn = conns.length ? Math.min(...conns) : 0;
  const maxConn = conns.length ? Math.max(...conns) : 0;
  const maxPending = samples.length
    ? Math.max(...samples.map((s) => s.pending))
    : 0;

  let p99Ratio: number | null = null;
  if (samples.length >= 10) {
    const avg = (a: number[]) => a.reduce((x, y) => x + y, 0) / a.length;
    const first5 = samples.slice(0, 5).map((s) => s.p99Ms);
    const last5 = samples.slice(-5).map((s) => s.p99Ms);
    const f = avg(first5);
    if (f > 0) p99Ratio = avg(last5) / f;
  }

  const failures: string[] = [];
  if (rssGrowthPct > 50)
    failures.push(
      `RSS grew ${rssGrowthPct.toFixed(1)}% (>50%) [${startRssMb}→${finalRssMb}MB]`,
    );
  if (maxConn - minConn > 2)
    failures.push(`Connection count drifted: min=${minConn} max=${maxConn}`);
  if (maxPending > 100)
    failures.push(`Pending peaked at ${maxPending} (>100)`);
  if (p99Ratio != null && p99Ratio > 1.5)
    failures.push(
      `p99 latency drift: last5/first5 = ${p99Ratio.toFixed(2)}× (>1.5×)`,
    );
  if (scheduleErrors > 0) failures.push(`schedule errors: ${scheduleErrors}`);

  console.log("\n## Soak result\n");
  console.log(`| metric | value |`);
  console.log(`| --- | --- |`);
  console.log(`| scheduled | ${scheduledCount} |`);
  console.log(`| fired | ${firedCount} |`);
  console.log(`| pending peak | ${maxPending} |`);
  console.log(
    `| RSS | ${startRssMb}MB → ${finalRssMb}MB (${
      rssGrowthPct >= 0 ? "+" : ""
    }${rssGrowthPct.toFixed(1)}%) |`,
  );
  console.log(`| connections | min=${minConn} max=${maxConn} |`);
  console.log(
    `| p99 last5/first5 | ${p99Ratio == null ? "n/a" : p99Ratio.toFixed(2) + "×"} |`,
  );
  console.log(`| schedule errors | ${scheduleErrors} |`);

  if (failures.length === 0) {
    console.log("\nPASS");
    process.exit(0);
  }
  console.log("\nFAIL");
  for (const f of failures) console.log(` - ${f}`);
  process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

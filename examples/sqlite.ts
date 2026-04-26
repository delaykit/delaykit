/**
 * DelayKit with SQLiteStore — local-first, zero-infra setup.
 *
 * No database service needed; the library writes to a file on disk.
 * Run: npx tsx examples/sqlite.ts
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DelayKit } from "../src/index.js";
import { SQLiteStore } from "../src/stores/sqlite.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

async function main() {
  const dir = mkdtempSync(join(tmpdir(), "delaykit-sqlite-"));
  const dbPath = join(dir, "delaykit.db");

  try {
    const store = await SQLiteStore.connect(dbPath);
    console.log(`SQLite opened at ${dbPath}, migrations applied.`);

    const dk = new DelayKit({
      store,
      scheduler: new PollingScheduler({ interval: 200 }),
    });

    dk.handle("send-reminder", async ({ key }) => {
      console.log(`[handler] Sending reminder to user ${key}`);
    });

    await dk.start();

    const { job, created } = await dk.schedule("send-reminder", {
      key: "user_1",
      delay: "1s",
    });
    console.log(`Scheduled: ${job.key} (id: ${job.id}, created: ${created})`);
    console.log("Waiting for job to fire...\n");

    await new Promise((r) => setTimeout(r, 3_000));

    const completed = await store.getJob(job.id);
    console.log(`\nJob status: ${completed?.status}`);
    console.log(`Job started at: ${completed?.startedAt}`);
    console.log(`Job completed at: ${completed?.completedAt}`);

    await dk.stop();
    await store.close();
    console.log("\nDone.");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

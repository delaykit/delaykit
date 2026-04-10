/**
 * DelayKit with PostgresStore — simulates a production-like setup.
 *
 * Requires: docker compose up -d (in delaykit/)
 * Run: npx tsx examples/postgres.ts
 */

import { DelayKit } from "../src/index.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

const DATABASE_URL = "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

async function main() {
  const store = await PostgresStore.connect(DATABASE_URL);
  console.log("Connected to Postgres, migrations applied.");

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

  // Verify job completed in the database
  const completed = await store.getJob(job.id);
  console.log(`\nJob status: ${completed?.status}`);
  console.log(`Job started at: ${completed?.startedAt}`);
  console.log(`Job completed at: ${completed?.completedAt}`);

  await dk.stop();
  await store.close();
  console.log("\nDone.");
}

main().catch(console.error);

/**
 * Basic DelayKit usage example.
 *
 * This simulates how a developer would use DelayKit in a Next.js app
 * during local development (MemoryStore + PollingScheduler).
 *
 * Run: npx tsx examples/basic.ts
 */

import { DelayKit } from "../src/index.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

const dk = new DelayKit({
  store: new MemoryStore(),
  scheduler: new PollingScheduler({ interval: 100 }),
});

// Handlers fetch fresh state using the key
dk.handle("send-reminder", async ({ key }) => {
  console.log(`[handler] Checking if user ${key} still needs a reminder...`);

  // In a real app: const user = await db.users.findOne({ id: key });
  // if (user.onboarded) return; // already acted, skip
  console.log(`[handler] Sending reminder to user ${key}`);
});

dk.handle("expire-trial", async ({ key }) => {
  console.log(`[handler] Expiring trial for account ${key}`);
});

async function main() {
  await dk.start();
  console.log("DelayKit started.\n");

  // Schedule a reminder — key is the entity ID
  const reminder = await dk.schedule("send-reminder", {
    key: "user_42",
    delay: "1s",
  });
  console.log(`Scheduled reminder: ${reminder.job.key} (created: ${reminder.created})`);

  // Duplicate scheduling — returns existing job, created: false
  const duplicate = await dk.schedule("send-reminder", {
    key: "user_42",
    delay: "1s",
  });
  console.log(`Duplicate attempt: ${duplicate.job.key} (created: ${duplicate.created})`);

  // Schedule a trial expiration
  const trial = await dk.schedule("expire-trial", {
    key: "acct_99",
    delay: "2s",
  });
  console.log(`Scheduled trial expiration: ${trial.job.key}`);

  // Cancel the trial — user extended it
  const cancelled = await dk.unschedule("expire-trial", "acct_99");
  console.log(`\nCancelled trial expiration: ${cancelled}`);

  // Wait for jobs to fire
  console.log("\nWaiting for jobs...\n");
  await new Promise((r) => setTimeout(r, 3_000));

  await dk.stop();
  console.log("\nDelayKit stopped.");
}

main().catch(console.error);

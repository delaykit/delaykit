/**
 * End-to-end test: PollingScheduler + PostgresStore
 *
 * Same tests as e2e-memory.ts but against a real Postgres database.
 * Requires: docker compose up -d
 *
 * Run: npx tsx examples/e2e-postgres.ts
 */

import { DelayKit } from "../src/index.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

const DATABASE_URL = "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

const log: string[] = [];

async function main() {
  console.log("DelayKit E2E: PollingScheduler + PostgresStore\n");

  const store = await PostgresStore.connect(DATABASE_URL);
  console.log("Connected to Postgres, migrations applied.");

  await (store as any).sql`DELETE FROM delaykit.jobs`;

  const dk = new DelayKit({
    store,
    scheduler: new PollingScheduler({ interval: 200 }),
  });

  dk.handle("send-reminder", async ({ key }) => {
    log.push(`send-reminder: ${key}`);
  });

  dk.handle("auto-save", async ({ key }) => {
    log.push(`auto-save: ${key}`);
  });

  dk.handle("flaky-task", {
    handler: async ({ key }) => {
      if (Math.random() < 0.5) throw new Error("random failure");
      log.push(`flaky-task: ${key}`);
    },
    retry: { attempts: 5, backoff: "fixed", initialDelay: "200ms" },
    onFailure: async ({ key, attempts }) => {
      log.push(`flaky-task EXHAUSTED: ${key} after ${attempts}`);
    },
  });

  dk.on("job:scheduled", (e) => console.log(`  [scheduled] ${e.job.key} (${e.job.kind})`));
  dk.on("job:started", (e) => console.log(`  [started]   ${e.job.key} attempt=${e.attempt}`));
  dk.on("job:completed", (e) => console.log(`  [completed] ${e.job.key} (${e.durationMs}ms)`));
  dk.on("job:failed", (e) => console.log(`  [failed]    ${e.job.key} after ${e.attempts} attempts`));
  dk.on("job:retrying", (e) => console.log(`  [retrying]  ${e.job.key} attempt ${e.attempt}→${e.nextAttempt}`));
  dk.on("job:cancelled", (e) => console.log(`  [cancelled] ${e.job.key}`));

  async function poll() {
    await dk.poll({ batchSize: 10 });
  }
  function sleep(ms: number) {
    return new Promise((r) => setTimeout(r, ms));
  }

  // 1. Schedule + poll
  console.log("\n=== Test: schedule + poll ===");
  await dk.schedule("send-reminder", { key: "user_1", delay: "0s" });
  await dk.schedule("send-reminder", { key: "user_2", delay: "0s" });
  await sleep(50);
  await poll();
  assert(log.includes("send-reminder: user_1"), "user_1 should fire");
  assert(log.includes("send-reminder: user_2"), "user_2 should fire");
  console.log(`  handlers fired: ${log.length}`);
  log.length = 0;

  // 2. Idempotent
  console.log("\n=== Test: idempotent (skip) ===");
  const first = await dk.schedule("send-reminder", { key: "user_3", delay: "10s" });
  const second = await dk.schedule("send-reminder", { key: "user_3", delay: "10s" });
  assert(first.created === true, "first should create");
  assert(second.created === false, "second should skip");
  console.log(`  first.created=${first.created}, second.created=${second.created}`);
  await dk.unschedule("send-reminder", "user_3");

  // 3. Replace
  console.log("\n=== Test: replace ===");
  await dk.schedule("send-reminder", { key: "user_4", delay: "10s" });
  const replaced = await dk.schedule("send-reminder", {
    key: "user_4", delay: "0s", onDuplicate: "replace",
  });
  assert(replaced.created === true, "replace should return created=true");
  assert(replaced.job.version === 2, "version should be 2");
  await sleep(50);
  await poll();
  assert(log.includes("send-reminder: user_4"), "replaced job should fire");
  log.length = 0;

  // 4. Cancel
  console.log("\n=== Test: cancel ===");
  await dk.schedule("send-reminder", { key: "user_5", delay: "0s" });
  const cancelled = await dk.unschedule("send-reminder", "user_5");
  assert(cancelled === true, "cancel should succeed");
  await sleep(50);
  await poll();
  assert(!log.some((l) => l.includes("user_5")), "cancelled should not fire");
  console.log(`  cancelled=${cancelled}`);
  log.length = 0;

  // 5. Debounce
  console.log("\n=== Test: debounce ===");
  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });
  await sleep(100);
  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });
  await sleep(100);
  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });
  await poll();
  assert(log.length === 0, "should not fire before settlement");
  await sleep(350);
  await poll();
  assert(log.length === 1, `should fire once, got ${log.length}`);
  console.log(`  fired ${log.length} time(s)`);
  log.length = 0;

  // 6. Key reuse after completion
  console.log("\n=== Test: key reuse ===");
  await dk.schedule("send-reminder", { key: "user_6", delay: "0s" });
  await sleep(50);
  await poll();
  assert(log.length === 1, "first should fire");
  log.length = 0;
  const reused = await dk.schedule("send-reminder", { key: "user_6", delay: "0s" });
  assert(reused.created === true, "should create after completion");
  await sleep(50);
  await poll();
  assert(log.length === 1, "second should fire");
  console.log(`  reused.created=${reused.created}`);
  log.length = 0;

  // 7. Retry
  console.log("\n=== Test: retry ===");
  await dk.schedule("flaky-task", { key: "flaky_1", delay: "0s" });
  for (let i = 0; i < 8; i++) {
    await sleep(250);
    await poll();
  }
  const succeeded = log.some((l) => l === "flaky-task: flaky_1");
  const exhausted = log.some((l) => l.includes("flaky-task EXHAUSTED"));
  assert(succeeded || exhausted, "should either succeed or exhaust");
  console.log(`  succeeded=${succeeded}, exhausted=${exhausted}`);
  log.length = 0;

  // 8. Key collision (same handler, different kind)
  console.log("\n=== Test: key collision ===");
  await dk.schedule("send-reminder", { key: "shared_1", delay: "10s" });
  let threw = false;
  try {
    await dk.debounce("send-reminder", { key: "shared_1", wait: "500ms" });
  } catch {
    threw = true;
  }
  assert(threw, "debounce on scheduled key (same handler) should throw");
  console.log(`  correctly rejected`);
  await dk.unschedule("send-reminder", "shared_1");

  // 9. Verify data persists in Postgres
  console.log("\n=== Test: data in Postgres ===");
  const rows = await (store as any).sql`
    SELECT status, count(*)::int as count FROM delaykit.jobs GROUP BY status ORDER BY status
  `;
  for (const row of rows) {
    console.log(`  ${row.status}: ${row.count}`);
  }

  await store.close();
  console.log("\n=== ALL TESTS PASSED ===\n");
  process.exit(0);
}

function assert(condition: boolean, msg: string) {
  if (!condition) throw new Error(`ASSERTION FAILED: ${msg}`);
}

main().catch((err) => {
  console.error("\n=== TEST FAILED ===");
  console.error(err);
  process.exit(1);
});

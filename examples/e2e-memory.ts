/**
 * End-to-end test: PollingScheduler + MemoryStore
 *
 * Exercises the full DelayKit flow as a user would in a Next.js app
 * using poll() from a cron route. No external deps needed.
 *
 * Run: npx tsx examples/e2e-memory.ts
 */

import { DelayKit } from "../src/index.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

const dk = new DelayKit({
  store: new MemoryStore(),
  scheduler: new PollingScheduler({ interval: 200 }),
});

// Track handler invocations
const log: string[] = [];

// --- Register handlers ---

dk.handle("send-reminder", async ({ key }) => {
  log.push(`send-reminder: ${key}`);
});

dk.handle("auto-save", async ({ key }) => {
  log.push(`auto-save: ${key}`);
});

dk.handle("flaky-task", {
  handler: async ({ key }) => {
    if (Math.random() < 0.5) {
      throw new Error("random failure");
    }
    log.push(`flaky-task: ${key}`);
  },
  retry: { attempts: 5, backoff: "fixed", initialDelay: "200ms" },
  onFailure: async ({ key, attempts }) => {
    log.push(`flaky-task EXHAUSTED: ${key} after ${attempts}`);
  },
});

// --- Event logging ---

dk.on("job:scheduled", (e) => console.log(`  [scheduled] ${e.job.key} (${e.job.kind})`));
dk.on("job:started", (e) => console.log(`  [started]   ${e.job.key} attempt=${e.attempt}`));
dk.on("job:completed", (e) => console.log(`  [completed] ${e.job.key} (${e.durationMs}ms)`));
dk.on("job:failed", (e) => console.log(`  [failed]    ${e.job.key} after ${e.attempts} attempts`));
dk.on("job:retrying", (e) => console.log(`  [retrying]  ${e.job.key} attempt ${e.attempt}→${e.nextAttempt}`));
dk.on("job:cancelled", (e) => console.log(`  [cancelled] ${e.job.key}`));

// --- Test helpers ---

async function poll() {
  await dk.poll({ batchSize: 10 });
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// --- Tests ---

async function testScheduleAndPoll() {
  console.log("\n=== Test: schedule + poll ===");

  await dk.schedule("send-reminder", { key: "user_1", delay: "0s" });
  await dk.schedule("send-reminder", { key: "user_2", delay: "0s" });

  await sleep(50);
  await poll();

  console.log(`  handlers fired: ${log.length}`);
  assert(log.includes("send-reminder: user_1"), "user_1 reminder should fire");
  assert(log.includes("send-reminder: user_2"), "user_2 reminder should fire");
  log.length = 0;
}

async function testIdempotent() {
  console.log("\n=== Test: schedule is idempotent (skip) ===");

  const first = await dk.schedule("send-reminder", { key: "user_3", delay: "10s" });
  const second = await dk.schedule("send-reminder", { key: "user_3", delay: "10s" });

  assert(first.created === true, "first should create");
  assert(second.created === false, "second should skip");
  assert(second.job.id === first.job.id, "should return same job");
  console.log(`  first.created=${first.created}, second.created=${second.created}`);

  await dk.unschedule("send-reminder", "user_3");
}

async function testReplace() {
  console.log("\n=== Test: schedule with replace ===");

  const first = await dk.schedule("send-reminder", { key: "user_4", delay: "10s" });
  const replaced = await dk.schedule("send-reminder", {
    key: "user_4",
    delay: "0s",
    onDuplicate: "replace",
  });

  assert(replaced.created === true, "replace should return created=true");
  assert(replaced.job.id === first.job.id, "same job id");
  assert(replaced.job.version === 2, "version should increment");

  await sleep(50);
  await poll();

  assert(log.includes("send-reminder: user_4"), "replaced job should fire");
  log.length = 0;
}

async function testCancel() {
  console.log("\n=== Test: cancel prevents execution ===");

  await dk.schedule("send-reminder", { key: "user_5", delay: "0s" });
  const cancelled = await dk.unschedule("send-reminder", "user_5");
  assert(cancelled === true, "cancel should succeed");

  await sleep(50);
  await poll();

  assert(!log.some((l) => l.includes("user_5")), "cancelled job should not fire");
  console.log(`  cancelled=${cancelled}, handlers fired: ${log.length}`);
  log.length = 0;
}

async function testDebounce() {
  console.log("\n=== Test: debounce fires once after settling ===");

  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });
  await sleep(100);
  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });
  await sleep(100);
  await dk.debounce("auto-save", { key: "doc_1", wait: "300ms" });

  await poll();
  assert(log.length === 0, "should not fire before settlement");

  await sleep(350);
  await poll();

  assert(log.length === 1, `should fire exactly once, got ${log.length}`);
  assert(log[0] === "auto-save: doc_1", "should fire auto-save for doc_1");
  console.log(`  fired ${log.length} time(s)`);
  log.length = 0;
}

async function testDebounceCancel() {
  console.log("\n=== Test: cancel a debounce ===");

  await dk.debounce("auto-save", { key: "doc_2", wait: "300ms" });
  const cancelled = await dk.unschedule("auto-save", "doc_2");
  assert(cancelled === true, "cancel should succeed");

  await sleep(400);
  await poll();

  assert(log.length === 0, "cancelled debounce should not fire");
  console.log(`  cancelled=${cancelled}`);
  log.length = 0;
}

async function testKeyReuse() {
  console.log("\n=== Test: key reusable after completion ===");

  await dk.schedule("send-reminder", { key: "user_6", delay: "0s" });
  await sleep(50);
  await poll();
  assert(log.length === 1, "first should fire");
  log.length = 0;

  const second = await dk.schedule("send-reminder", { key: "user_6", delay: "0s" });
  assert(second.created === true, "should create a new job after completion");
  await sleep(50);
  await poll();
  assert(log.length === 1, "second should fire");
  console.log(`  second.created=${second.created}`);
  log.length = 0;
}

async function testRetry() {
  console.log("\n=== Test: retry with exponential backoff ===");

  await dk.schedule("flaky-task", { key: "flaky_1", delay: "0s" });

  for (let i = 0; i < 8; i++) {
    await sleep(250);
    await poll();
  }

  const succeeded = log.some((l) => l === "flaky-task: flaky_1");
  const exhausted = log.some((l) => l.includes("flaky-task EXHAUSTED"));
  assert(succeeded || exhausted, "should either succeed or exhaust retries");
  console.log(`  succeeded=${succeeded}, exhausted=${exhausted}`);
  log.length = 0;
}

async function testKeyCollision() {
  console.log("\n=== Test: key collision between once and pattern (same handler) ===");

  await dk.schedule("send-reminder", { key: "shared_1", delay: "10s" });

  let threw = false;
  try {
    await dk.debounce("send-reminder", { key: "shared_1", wait: "500ms" });
  } catch (e: any) {
    threw = true;
    console.log(`  correctly rejected: ${e.message}`);
  }
  assert(threw, "debounce on a scheduled key (same handler) should throw");

  await dk.unschedule("send-reminder", "shared_1");
}

async function testHandlerAfterPoll() {
  console.log("\n=== Test: handle() after poll() throws ===");

  let threw = false;
  try {
    dk.handle("late-handler", async () => {});
  } catch (e: any) {
    threw = true;
    console.log(`  correctly rejected: ${e.message}`);
  }
  assert(threw, "handle() after poll() should throw");
}

// --- Run ---

function assert(condition: boolean, msg: string) {
  if (!condition) throw new Error(`ASSERTION FAILED: ${msg}`);
}

async function main() {
  console.log("DelayKit E2E: PollingScheduler + MemoryStore\n");

  await testScheduleAndPoll();
  await testIdempotent();
  await testReplace();
  await testCancel();
  await testDebounce();
  await testDebounceCancel();
  await testKeyReuse();
  await testRetry();
  await testKeyCollision();
  await testHandlerAfterPoll();

  console.log("\n=== ALL TESTS PASSED ===\n");
  process.exit(0);
}

main().catch((err) => {
  console.error("\n=== TEST FAILED ===");
  console.error(err);
  process.exit(1);
});

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { makeJob } from "./helpers/job-factory.js";
import { TEST_URL, truncatePostgresJobs } from "./helpers/postgres-fixture.js";

let store: PostgresStore;

beforeAll(async () => {
  store = await PostgresStore.connect(TEST_URL);
});

afterAll(async () => {
  await store.close();
});

beforeEach(async () => {
  await truncatePostgresJobs(store);
});

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("PostgresStore concurrency", () => {
  it("exactly-once: concurrent markRunning on same job", async () => {
    const job = await store.createJob(makeJob({ key: "race:1" }));

    const results = await Promise.all([
      store.markRunning(job.id, 1),
      store.markRunning(job.id, 1),
    ]);

    const claimed = results.filter(Boolean);
    expect(claimed).toHaveLength(1);
  });

  it("concurrent claimDueJobs across pollers produce disjoint sets", async () => {
    // Seed N due-now rows; race many pollers; assert total claims == N
    // and no row appears twice. SKIP LOCKED is the line of defense here.
    const N = 50;
    const past = new Date(Date.now() - 1_000);
    for (let i = 0; i < N; i++) {
      await store.createJob(makeJob({
        key: `cluster:${i}`,
        scheduledFor: past,
      }));
    }

    const POLLERS = 5;
    const BUDGET = 20;
    const batches = await Promise.all(
      Array.from({ length: POLLERS }, () => store.claimDueJobs(BUDGET, ["test"])),
    );

    const claimed = batches.flatMap((b) => b.toRun);
    const ids = claimed.map((j) => j.id);
    expect(ids.length).toBe(N);
    expect(new Set(ids).size).toBe(N);

    for (const j of claimed) {
      expect(j.status).toBe("running");
      expect(j.claimedVersion).toBe(j.version);
      expect(j.startedAt).not.toBeNull();
    }
  });

  it("two PollingScheduler instances against one store: no double execution", async () => {
    const N = 30;
    const past = new Date(Date.now() - 200);
    for (let i = 0; i < N; i++) {
      await store.createJob(makeJob({
        handler: "shared",
        key: `multi:${i}`,
        scheduledFor: past,
      }));
    }

    const seen: string[] = [];
    const handler = async ({ key }: { key: string }) => {
      seen.push(key);
    };

    const dk1 = new DelayKit({ store, scheduler: new PollingScheduler({ interval: 50 }) });
    const dk2 = new DelayKit({ store, scheduler: new PollingScheduler({ interval: 50 }) });
    dk1.handle("shared", handler);
    dk2.handle("shared", handler);

    await Promise.all([dk1.start(), dk2.start()]);
    await wait(800);
    await Promise.all([dk1.stop({ drainMs: 500 }), dk2.stop({ drainMs: 500 })]);

    expect(seen.length).toBe(N);
    expect(new Set(seen).size).toBe(N);
  });
});

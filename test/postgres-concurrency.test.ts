import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { makeJob } from "./helpers/job-factory.js";
import { assertJobInvariants } from "./helpers/invariants.js";
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

  it("noteMissingHandler does not clobber a concurrently claimed row", async () => {
    // Race noteMissingHandler against markRunning on the same row +
    // version. Exactly one CAS must win, and the loser must leave the
    // row's invariants intact. Without the WHERE-clause CAS guard on
    // the UPDATE, EvalPlanQual would re-evaluate only `j.id = t.id`
    // after markRunning's commit, allowing the note to overwrite a
    // running row's pending fields.
    const N = 50;
    for (let i = 0; i < N; i++) {
      const job = await store.createJob(makeJob({ key: `nmh-race:${i}` }));
      const [noteResult, runResult] = await Promise.all([
        store.noteMissingHandler(job.id, 1, "deferred", "terminal", 60_000),
        store.markRunning(job.id, 1),
      ]);

      const final = (await store.getJob(job.id))!;
      assertJobInvariants(final);

      // Exactly one path won.
      const noteWon = noteResult !== null;
      expect(noteWon).not.toBe(runResult);

      if (runResult) {
        expect(final.status).toBe("running");
        expect(final.deferAttempts).toBe(0);
        expect(final.deferredSince).toBeNull();
      } else {
        expect(final.status).toBe("pending");
        expect(final.deferAttempts).toBe(1);
        expect(final.deferredSince).not.toBeNull();
        expect(final.claimedVersion).toBeNull();
        expect(final.startedAt).toBeNull();
      }
    }
  });

  it("deferJob does not clobber a concurrently claimed row", async () => {
    // Same race as noteMissingHandler. Latent under the same
    // missing-WHERE-predicate bug; same fix.
    const N = 50;
    for (let i = 0; i < N; i++) {
      const job = await store.createJob(makeJob({ key: `dj-race:${i}` }));
      const next = new Date(Date.now() + 30_000);
      const [deferResult, runResult] = await Promise.all([
        store.deferJob(job.id, 1, next, "deferred", "terminal", 60_000),
        store.markRunning(job.id, 1),
      ]);

      const final = (await store.getJob(job.id))!;
      assertJobInvariants(final);

      const deferWon = deferResult !== null;
      expect(deferWon).not.toBe(runResult);

      if (runResult) {
        expect(final.status).toBe("running");
        expect(final.deferAttempts).toBe(0);
        expect(final.deferredSince).toBeNull();
      } else {
        expect(final.status).toBe("pending");
        expect(final.deferAttempts).toBe(1);
        expect(final.deferredSince).not.toBeNull();
        expect(final.claimedVersion).toBeNull();
        expect(final.startedAt).toBeNull();
      }
    }
  });

  it("rescheduleJob does not clobber a concurrently completed row", async () => {
    // Race rescheduleJob against markCompleted. Exactly one CAS must
    // win, and the loser must leave the row's invariants intact.
    const N = 50;
    for (let i = 0; i < N; i++) {
      const job = await store.createJob(makeJob({ key: `rs-race:${i}` }));
      await store.markRunning(job.id, 1);

      const [reschedResult, completeResult] = await Promise.all([
        store.rescheduleJob(job.id, 1, new Date(Date.now() + 30_000)),
        store.markCompleted(job.id, 1),
      ]);

      const final = (await store.getJob(job.id))!;
      assertJobInvariants(final);

      const reschedWon = reschedResult !== null;
      expect(reschedWon).not.toBe(completeResult);

      if (completeResult) {
        expect(final.status).toBe("completed");
        expect(final.attempt).toBe(0);
      } else {
        expect(final.status).toBe("pending");
        expect(final.attempt).toBe(0);
        expect(final.startedAt).toBeNull();
        expect(final.claimedVersion).toBeNull();
        expect(final.completedAt).toBeNull();
      }
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

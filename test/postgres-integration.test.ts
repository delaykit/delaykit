import { describe, it, expect, vi, beforeAll, afterAll, beforeEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { makeJob } from "./helpers/job-factory.js";

const TEST_URL = "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

let store: PostgresStore;

beforeAll(async () => {
  store = await PostgresStore.connect(TEST_URL);
});

afterAll(async () => {
  await store.close();
});

beforeEach(async () => {
  await (store as any).sql`DELETE FROM delaykit.jobs`;
});

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("PostgresStore + PollingScheduler integration", () => {
  it("fires a scheduled job", async () => {
    const scheduler = new PollingScheduler({ interval: 50 });
    const dk = new DelayKit({ store, scheduler });

    const received = vi.fn();
    dk.handle("test", async ({ key }) => {
      received(key);
    });

    await dk.start();
    await dk.schedule("test", { key: "pg:1", delay: "200ms" });

    await wait(500);

    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith("pg:1");

    await dk.stop();
  });

  it("job survives scheduler restart", async () => {
    // Phase 1: schedule a job, stop before it fires
    const scheduler1 = new PollingScheduler({ interval: 50 });
    const dk1 = new DelayKit({ store, scheduler: scheduler1 });

    dk1.handle("survive", async () => {});
    await dk1.start();

    const { job } = await dk1.schedule("survive", {
      key: "restart:1",
      at: new Date(Date.now() + 300),
    });

    await dk1.stop();

    // Verify job is still pending in the database
    const pending = await store.getJob(job.id);
    expect(pending!.status).toBe("pending");

    // Phase 2: new scheduler picks up the pending job
    const received = vi.fn();
    const scheduler2 = new PollingScheduler({ interval: 50 });
    const dk2 = new DelayKit({ store, scheduler: scheduler2 });

    dk2.handle("survive", async ({ key }) => {
      received(key);
    });

    await dk2.start();
    await wait(600);

    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith("restart:1");

    await dk2.stop();
  });

  it("dk.poll() runs a scheduled job", async () => {
    const scheduler = new PollingScheduler();
    const dk = new DelayKit({ store, scheduler });

    const received = vi.fn();
    dk.handle("test", async ({ key }) => {
      received(key);
    });

    // Backdate so the job is unambiguously due at SQL now(), regardless
    // of JS ↔ Postgres round-trip timing and precision.
    await dk.schedule("test", { key: "pg:poll:1", at: new Date(Date.now() - 100) });
    await dk.poll();

    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith("pg:poll:1");
  });

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

  it("idempotent scheduling: same key blocked while active", async () => {
    const scheduler = new PollingScheduler({ interval: 50 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("test", async () => {});

    const first = await dk.schedule("test", { key: "idem:1", delay: "1h" });
    const second = await dk.schedule("test", { key: "idem:1", delay: "1h" });

    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.job.id).toBe(first.job.id);
  });

  it("allows scheduling same key after completion", async () => {
    const scheduler = new PollingScheduler({ interval: 50 });
    const dk = new DelayKit({ store, scheduler });

    dk.handle("test", async () => {});
    await dk.start();

    await dk.schedule("test", { key: "reuse:1", delay: "100ms" });
    await wait(400);

    const second = await dk.schedule("test", { key: "reuse:1", delay: "1h" });
    expect(second.created).toBe(true);

    await dk.stop();
  });
});

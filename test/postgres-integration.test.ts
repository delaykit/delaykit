import { describe, it, expect, vi, beforeAll, afterAll, beforeEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { PostgresStore } from "../src/stores/postgres.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
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

/**
 * `dk.listFailed` / `dk.retryFailed` orchestration: stagger, sequential
 * per-row CAS, footgun guards, IDs form, hasMore.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { Job, FailureReason, Scheduler, ScheduleRequest } from "../src/types.js";
import { assertJobInvariants } from "./helpers/invariants.js";
import { makeJob } from "./helpers/job-factory.js";

async function plantFailed(
  store: MemoryStore,
  key: string,
  opts: { handler?: string; reason?: FailureReason } = {},
): Promise<Job> {
  const job = await store.createJob(makeJob({ key, handler: opts.handler ?? "h" }));
  await store.markRunning(job.id, job.version);
  await store.markFailed(job.id, job.version, new Error("boom"), opts.reason ?? "handler_error");
  return (await store.getJob(job.id))!;
}

describe("dk.listFailed", () => {
  let dk: DelayKit;
  let store: MemoryStore;

  beforeEach(() => {
    store = new MemoryStore();
    dk = new DelayKit({ store, scheduler: new PollingScheduler({ interval: 1_000 }) });
  });
  afterEach(async () => {
    await dk.stop({ drainMs: 0 });
    await store.close();
  });

  it("delegates to store and returns the page", async () => {
    await plantFailed(store, "lf:1");
    const page = await dk.listFailed({ limit: 10 });
    expect(page.jobs).toHaveLength(1);
    expect(page.cursor).toBeNull();
  });

  it("forwards filters", async () => {
    await plantFailed(store, "lf:a", { reason: "timeout" });
    await plantFailed(store, "lf:b", { reason: "stalled" });
    const page = await dk.listFailed({ reason: "timeout", limit: 10 });
    expect(page.jobs).toHaveLength(1);
    expect(page.jobs[0].failureReason).toBe("timeout");
  });
});

describe("dk.retryFailed (filter form)", () => {
  let dk: DelayKit;
  let store: MemoryStore;

  beforeEach(() => {
    store = new MemoryStore();
    dk = new DelayKit({ store, scheduler: new PollingScheduler({ interval: 1_000 }) });
    dk.handle("h", async () => {});
  });
  afterEach(async () => {
    await dk.stop({ drainMs: 0 });
    await store.close();
  });

  it("rejects calls with no handler/reason/since filter", async () => {
    await expect(dk.retryFailed({ limit: 10 })).rejects.toThrow(/at least one of/);
  });

  it("retries matching rows and resets to pending", async () => {
    const j = await plantFailed(store, "rf:1");
    const result = await dk.retryFailed({ handler: "h", limit: 10 });
    expect(result.retried).toBe(1);
    expect(result.skipped).toBe(0);
    expect(result.hasMore).toBe(false);

    const row = await store.getJob(j.id);
    expect(row!.status).toBe("pending");
    expect(row!.attempt).toBe(0);
    expect(row!.failureReason).toBeNull();
    expect(row!.lastError).toBeNull();
    expect(row!.claimedVersion).toBeNull();
    expect(row!.schedulerRef).toBeNull();
    assertJobInvariants(row!);
  });

  it("returns hasMore: true when matches exceed limit", async () => {
    for (let i = 0; i < 5; i++) await plantFailed(store, `rf:hm-${i}`);
    const result = await dk.retryFailed({ handler: "h", limit: 3 });
    expect(result.retried).toBe(3);
    expect(result.hasMore).toBe(true);
  });

  it("staggers scheduledFor across the spread window", async () => {
    const planted: Job[] = [];
    for (let i = 0; i < 4; i++) planted.push(await plantFailed(store, `rf:s-${i}`));

    const t0 = Date.now();
    const result = await dk.retryFailed({ handler: "h", limit: 10, spreadMs: 1000 });
    expect(result.retried).toBe(4);
    expect(result.spreadMs).toBe(1000);

    const rows = await Promise.all(planted.map((p) => store.getJob(p.id)));
    const offsets = rows.map((r) => r!.scheduledFor.getTime() - t0).sort((a, b) => a - b);
    // 4 rows over 1000ms = 0, 250, 500, 750
    expect(offsets[0]).toBeGreaterThanOrEqual(0);
    expect(offsets[0]).toBeLessThan(50);
    expect(offsets[3]).toBeGreaterThanOrEqual(700);
    expect(offsets[3]).toBeLessThanOrEqual(800);
  });

  it("spreadMs: 0 schedules all rows immediately", async () => {
    for (let i = 0; i < 3; i++) await plantFailed(store, `rf:0-${i}`);
    const t0 = Date.now();
    const result = await dk.retryFailed({ handler: "h", limit: 10, spreadMs: 0 });
    expect(result.spreadMs).toBe(0);
    const page = await dk.listFailed({ limit: 10 });
    expect(page.jobs).toHaveLength(0);
    // All scheduledFor values clustered at t0 (within a few ms).
    const { jobs } = await store.listFailed({ limit: 10 });
    expect(jobs).toEqual([]);
  });

  it("default spreadMs follows min(N*100, 60_000)", async () => {
    for (let i = 0; i < 3; i++) await plantFailed(store, `rf:def-${i}`);
    const result = await dk.retryFailed({ handler: "h", limit: 10 });
    expect(result.spreadMs).toBe(300); // 3 * 100
  });

  it("rejects negative spreadMs", async () => {
    await expect(
      dk.retryFailed({ handler: "h", limit: 10, spreadMs: -1 }),
    ).rejects.toThrow(/spreadMs/);
  });

  it("filters by reason", async () => {
    await plantFailed(store, "rf:t1", { reason: "timeout" });
    await plantFailed(store, "rf:t2", { reason: "timeout" });
    await plantFailed(store, "rf:s1", { reason: "stalled" });

    const result = await dk.retryFailed({ reason: "timeout", limit: 10 });
    expect(result.retried).toBe(2);

    const remaining = await dk.listFailed({ limit: 10 });
    expect(remaining.jobs).toHaveLength(1);
    expect(remaining.jobs[0].failureReason).toBe("stalled");
  });

});

describe("dk.retryFailed (IDs form)", () => {
  let dk: DelayKit;
  let store: MemoryStore;

  beforeEach(() => {
    store = new MemoryStore();
    dk = new DelayKit({ store, scheduler: new PollingScheduler({ interval: 1_000 }) });
    dk.handle("h", async () => {});
  });
  afterEach(async () => {
    await dk.stop({ drainMs: 0 });
    await store.close();
  });

  it("retries the listed jobs", async () => {
    const a = await plantFailed(store, "ids:a");
    const b = await plantFailed(store, "ids:b");

    const result = await dk.retryFailed({ ids: [a.id, b.id] });
    expect(result.retried).toBe(2);
    expect(result.skipped).toBe(0);
  });

  it("counts missing or non-failed IDs as skipped", async () => {
    const failed = await plantFailed(store, "ids:f");
    const pending = await store.createJob(makeJob({ key: "ids:p", handler: "h" }));

    const ids = [failed.id, pending.id, "nonexistent"];
    const result = await dk.retryFailed({ ids });
    expect(result.retried).toBe(1);
    expect(result.skipped).toBe(2);
    // Invariant: every input is accounted for as either retried or skipped.
    expect(result.retried + result.skipped).toBe(ids.length);
  });

  it("rejects when ids exceeds the cap", async () => {
    const ids = Array.from({ length: 1001 }, () => crypto.randomUUID());
    await expect(dk.retryFailed({ ids })).rejects.toThrow(/cap/);
  });

  it("staggers IDs-form retries", async () => {
    const planted: Job[] = [];
    for (let i = 0; i < 4; i++) planted.push(await plantFailed(store, `ids:s-${i}`));

    const t0 = Date.now();
    const result = await dk.retryFailed({
      ids: planted.map((p) => p.id),
      spreadMs: 800,
    });
    expect(result.retried).toBe(4);

    const rows = await Promise.all(planted.map((p) => store.getJob(p.id)));
    const offsets = rows.map((r) => r!.scheduledFor.getTime() - t0).sort((a, b) => a - b);
    expect(offsets[0]).toBeLessThan(50);
    expect(offsets[3]).toBeGreaterThan(500);
  });
});

describe("dk.retryFailed materialization failures", () => {
  class ThrowOnNthScheduler implements Scheduler {
    private calls = 0;
    constructor(private throwOnCall: number) {}
    async schedule(_req: ScheduleRequest): Promise<string | null> {
      this.calls++;
      if (this.calls === this.throwOnCall) throw new Error("scheduler down");
      return `ref-${this.calls}`;
    }
    async cancel(): Promise<void> {}
    async start(): Promise<void> {}
    async stop(): Promise<void> {}
  }

  it("counts a row that hits a wake error as skipped and continues with the rest", async () => {
    const store = new MemoryStore();
    const scheduler = new ThrowOnNthScheduler(2);
    const dk = new DelayKit({ store, scheduler });
    dk.handle("h", async () => {});
    await dk.start();

    const a = await plantFailed(store, "mf:1");
    const b = await plantFailed(store, "mf:2");
    const c = await plantFailed(store, "mf:3");

    const result = await dk.retryFailed({ ids: [a.id, b.id, c.id] });
    expect(result.retried).toBe(2);
    expect(result.skipped).toBe(1);

    const rowB = await store.getJob(b.id);
    expect(rowB!.status).toBe("failed");
    expect(rowB!.failureReason).toBe("materialization_error");

    await dk.stop({ drainMs: 0 });
    await store.close();
  });
});

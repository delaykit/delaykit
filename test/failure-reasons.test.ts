/**
 * Every terminal-failure path emits `job:failed` with the right
 * `FailureReason` and persists it on the row.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type {
  JobFailedEvent,
  Scheduler,
  ScheduleRequest,
} from "../src/types.js";

function createKit(options?: { deferHorizonMs?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: 50, stalledCheckInterval: 100 });
  const dk = new DelayKit({
    store,
    scheduler,
    deferHorizonMs: options?.deferHorizonMs,
  });
  return { dk, store, scheduler };
}

describe("FailureReason: per-path coverage", () => {
  let dk: DelayKit;

  beforeEach(() => vi.useFakeTimers());
  afterEach(async () => {
    if (dk) await dk.stop({ drainMs: 0 });
    vi.useRealTimers();
  });

  it("handler_error: handler throws past max attempts", async () => {
    const { dk: kit, store } = createKit();
    dk = kit;
    dk.handle("doomed", {
      handler: async () => { throw new Error("boom"); },
      retry: { attempts: 1, backoff: "fixed", initialDelay: "1s" },
    });

    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    await dk.start();
    const { job } = await dk.schedule("doomed", { key: "h:1", delay: "1s" });
    await vi.advanceTimersByTimeAsync(1_100);

    expect(events).toHaveLength(1);
    expect(events[0].reason).toBe("handler_error");
    expect(events[0].job.failureReason).toBe("handler_error");

    const row = await store.getJob(job.id);
    expect(row!.status).toBe("failed");
    expect(row!.failureReason).toBe("handler_error");
    expect(row!.lastError).toBe("boom");
  });

  it("timeout: handler exceeds its timeout budget", async () => {
    const { dk: kit, store } = createKit();
    dk = kit;
    dk.handle("slow", {
      handler: async (ctx) => {
        // Hang until aborted; resolve on abort so handler exits cleanly.
        await new Promise<void>((resolve) => {
          ctx.signal.addEventListener("abort", () => resolve());
        });
        throw new Error("timed out"); // Re-throw on abort to mark as error
      },
      timeout: "1s",
      retry: { attempts: 1, backoff: "fixed", initialDelay: "1s" },
    });

    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    await dk.start();
    const { job } = await dk.schedule("slow", { key: "t:1", delay: "1s" });
    await vi.advanceTimersByTimeAsync(1_100); // dispatch
    await vi.advanceTimersByTimeAsync(1_100); // timeout fires
    await vi.advanceTimersByTimeAsync(50); // result handler settles

    expect(events).toHaveLength(1);
    expect(events[0].reason).toBe("timeout");
    expect(events[0].job.failureReason).toBe("timeout");

    const row = await store.getJob(job.id);
    expect(row!.failureReason).toBe("timeout");
  });

  it("stalled: process died, attempts exhausted via stalled-job sweep", async () => {
    const { dk: kit, store } = createKit();
    dk = kit;
    dk.handle("crash", {
      handler: async () => { /* never invoked — we simulate a crashed process */ },
      retry: { attempts: 1, backoff: "fixed", initialDelay: "1s" },
    });

    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    // Plant a "running" row, then jump fake time past the stall window.
    // reclaimStalledJobs cutoff = max(timeouts) + STALLED_GRACE_MS = 35s.
    // After reclaim, attempt becomes 1, which equals maxAttempts → terminal.
    const { job } = await dk.schedule("crash", { key: "s:1", delay: "1s" });
    await store.markRunning(job.id, job.version);

    await dk.start();
    await vi.advanceTimersByTimeAsync(40_000); // past stall window + sweep

    expect(events.length).toBeGreaterThanOrEqual(1);
    const failed = events.find((e) => e.reason === "stalled");
    expect(failed).toBeDefined();
    expect(failed!.job.failureReason).toBe("stalled");

    const row = await store.getJob(job.id);
    expect(row!.status).toBe("failed");
    expect(row!.failureReason).toBe("stalled");
  });

  it("defer_horizon: store.deferJob(horizon=0) flips to failed with failure_reason='defer_horizon'", async () => {
    // PollingScheduler filters unregistered handlers out of dispatch, so the
    // defer path only fires from the wake-driven flow. Exercise the store
    // contract directly here.
    const store = new MemoryStore();
    const id = crypto.randomUUID();
    await store.createJob({
      id,
      kind: "once",
      handler: "ghost",
      key: "g:1",
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 1,
      schedulerRef: null,
      lastError: null,
      failureReason: null,
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
      deferAttempts: 0,
      deferredSince: null,
      retryConfig: null,
    });

    await store.deferJob(id, 1, new Date(Date.now() + 1000), "deferred", "horizon", 1_000_000);
    const deferred = await store.getJob(id);
    expect(deferred!.status).toBe("pending");
    expect(deferred!.failureReason).toBeNull();

    await vi.advanceTimersByTimeAsync(10);
    await store.deferJob(id, deferred!.version, new Date(), "deferred", "horizon-exceeded", 0);

    const row = await store.getJob(id);
    expect(row!.status).toBe("failed");
    expect(row!.failureReason).toBe("defer_horizon");
    expect(row!.lastError).toBe("horizon-exceeded");

    await store.close();
  });
});

// =============================================================================
// materialization_error — needs a scheduler whose schedule() throws.
// =============================================================================

class ThrowingScheduler implements Scheduler {
  shouldThrow = false;
  scheduleCalls = 0;
  cancelCalls: string[] = [];

  async schedule(_req: ScheduleRequest): Promise<string | null> {
    this.scheduleCalls++;
    if (this.shouldThrow) throw new Error("scheduler down");
    return `ref-${this.scheduleCalls}`;
  }
  async cancel(ref: string): Promise<void> {
    this.cancelCalls.push(ref);
  }
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
}

describe("FailureReason: materialization_error", () => {
  it("emits when wake materialization throws during schedule()-replace", async () => {
    const store = new MemoryStore();
    const scheduler = new ThrowingScheduler();
    const dk = new DelayKit({ store, scheduler });
    dk.handle("h", async () => {});
    await dk.start();

    const { job: first } = await dk.schedule("h", { key: "m:1", delay: "10s" });
    expect(first.failureReason).toBeNull();

    scheduler.shouldThrow = true;
    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    await expect(
      dk.schedule("h", { key: "m:1", delay: "20s", onDuplicate: "replace" }),
    ).rejects.toThrow("scheduler down");

    expect(events).toHaveLength(1);
    expect(events[0].reason).toBe("materialization_error");
    expect(events[0].job.failureReason).toBe("materialization_error");

    const row = await store.getJob(first.id);
    expect(row!.status).toBe("failed");
    expect(row!.failureReason).toBe("materialization_error");

    await dk.stop({ drainMs: 0 });
    await store.close();
  });

  it("emits when wake materialization throws during retryJob()", async () => {
    const store = new MemoryStore();
    const scheduler = new ThrowingScheduler();
    const dk = new DelayKit({ store, scheduler });
    dk.handle("h", async () => {});
    await dk.start();

    const id = crypto.randomUUID();
    await store.createJob({
      id,
      kind: "once",
      handler: "h",
      key: "r:1",
      version: 1,
      claimedVersion: null,
      status: "failed",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: new Date(),
      attempt: 1,
      maxAttempts: 1,
      schedulerRef: null,
      lastError: "prior failure",
      failureReason: "handler_error",
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
      deferAttempts: 0,
      deferredSince: null,
      retryConfig: null,
    });

    scheduler.shouldThrow = true;
    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    await expect(dk.retryJob(id)).rejects.toThrow("scheduler down");

    expect(events).toHaveLength(1);
    expect(events[0].reason).toBe("materialization_error");
    expect(events[0].job.failureReason).toBe("materialization_error");

    const row = await store.getJob(id);
    expect(row!.status).toBe("failed");
    expect(row!.failureReason).toBe("materialization_error");

    await dk.stop({ drainMs: 0 });
    await store.close();
  });
});

// =============================================================================
// Negative coverage: CAS-loss paths must not emit reason-less events.
// =============================================================================

describe("FailureReason: CAS-loss paths", () => {
  it("markFailed CAS loss does not emit a reason-less job:failed", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 50 });
    const dk = new DelayKit({ store, scheduler });

    const events: JobFailedEvent[] = [];
    dk.on("job:failed", (e) => events.push(e));

    const ok = await store.markFailed("nonexistent", 1, new Error("boom"), "handler_error");
    expect(ok).toBe(false);
    expect(events).toHaveLength(0);

    await dk.stop({ drainMs: 0 });
    await store.close();
  });
});

// =============================================================================
// Backend-level: failure_reason persists and round-trips for all five reasons.
// =============================================================================

describe("FailureReason: persistence round-trip (MemoryStore)", () => {
  it.each([
    "handler_error",
    "timeout",
    "stalled",
    "defer_horizon",
    "materialization_error",
  ] as const)("markFailed persists %s and getJob reads it back", async (reason) => {
    const store = new MemoryStore();
    const id = crypto.randomUUID();
    await store.createJob({
      id,
      kind: "once",
      handler: "h",
      key: `p:${reason}`,
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 1,
      schedulerRef: null,
      lastError: null,
      failureReason: null,
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
      deferAttempts: 0,
      deferredSince: null,
      retryConfig: null,
    });
    await store.markRunning(id, 1);
    const ok = await store.markFailed(id, 1, new Error("e"), reason);
    expect(ok).toBe(true);

    const row = await store.getJob(id);
    expect(row!.failureReason).toBe(reason);

    const reset = await store.resetJob(id);
    expect(reset!.failureReason).toBeNull();

    await store.close();
  });
});

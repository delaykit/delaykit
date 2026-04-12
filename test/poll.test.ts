/**
 * dk.poll() coverage — the Vercel cron production path.
 *
 * dk.poll() is a single-cycle execution triggered externally
 * (Vercel cron, a manual endpoint), distinct from the long-running
 * dk.start() polling loop.
 *
 * Shared code paths — executor, handleResult, retries, patterns,
 * stalled recovery — are covered in other suites via dk.start().
 * These tests focus on what's genuinely poll-specific: the batching
 * loop, the hard timeout deadline, and what happens when the
 * deadline cuts in-flight work.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

function createKit() {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler();
  const dk = new DelayKit({ store, scheduler });
  return { dk, store };
}

describe("dk.poll()", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("runs a due job and marks it terminal", async () => {
    const { dk } = createKit();
    const received = vi.fn();
    dk.handle("task", async ({ key }) => {
      received(key);
    });

    await dk.schedule("task", { key: "k:1", at: new Date() });
    await dk.poll();

    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith("k:1");
    const after = await dk.getJobByKey("task", "k:1");
    expect(after).toBeNull();
  });

  it("processes multiple batches until the store is empty", async () => {
    const { dk } = createKit();
    const executed: string[] = [];
    dk.handle("task", async ({ key }) => {
      executed.push(key);
    });

    for (let i = 0; i < 5; i++) {
      await dk.schedule("task", { key: `k:${i}`, at: new Date() });
    }

    // batchSize: 2 forces three iterations to drain 5 jobs.
    await dk.poll({ batchSize: 2 });

    expect(executed).toHaveLength(5);
    expect(executed.sort()).toEqual(["k:0", "k:1", "k:2", "k:3", "k:4"]);
  });

  it("stops dispatching new batches when the timeout deadline hits", async () => {
    const { dk } = createKit();
    const invoked: string[] = [];
    dk.handle("slow", {
      handler: async ({ key, signal }) => {
        invoked.push(key);
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, 200);
          signal.addEventListener("abort", () => {
            clearTimeout(timer);
            resolve();
          });
        });
      },
      timeout: "5s",
    });

    for (let i = 0; i < 4; i++) {
      await dk.schedule("slow", { key: `k:${i}`, at: new Date() });
    }

    // batchSize: 1 + 100ms deadline. The first batch starts (invokes
    // handler for k:0) but the deadline fires before it completes;
    // subsequent batches never dispatch.
    const p = dk.poll({ batchSize: 1, timeout: "100ms" });
    await vi.advanceTimersByTimeAsync(150);
    await p;

    expect(invoked).toHaveLength(1);
    expect(invoked[0]).toBe("k:0");
    // Jobs 1-3 never got to dispatch — still pending for the next cycle.
    for (const k of ["k:1", "k:2", "k:3"]) {
      const job = await dk.getJobByKey("slow", k);
      expect(job?.status).toBe("pending");
    }

    // Drain the first handler's lingering timer so the suite doesn't
    // see pending fake timers.
    await vi.advanceTimersByTimeAsync(500);
  });

  it("recovers orphaned 'running' jobs on a subsequent poll cycle", async () => {
    // Models the Vercel cron scenario: a handler that hangs past the
    // deadline. poll() returns, the platform kills the function
    // (destroying pending timers), the job is left in 'running'. The
    // next cron invocation reclaims it via stalled recovery and runs
    // a fresh execution.
    const store = new MemoryStore();

    // First invocation — handler truly hangs (ignores signal,
    // never resolves). Deadline cuts before the executor timer fires.
    const dk1 = new DelayKit({ store, scheduler: new PollingScheduler() });
    dk1.handle("task", {
      handler: async () => {
        await new Promise<void>(() => { /* hangs forever */ });
      },
      timeout: "1s",
    });
    const { job } = await dk1.schedule("task", { key: "k:1", at: new Date() });
    const firstPoll = dk1.poll({ timeout: "50ms" });
    await vi.advanceTimersByTimeAsync(60);
    await firstPoll;

    // Simulate Vercel killing the function. Clears the pending
    // executor rejection timer so the job stays in 'running' instead
    // of being processed by handleResult in the background.
    vi.clearAllTimers();

    const orphaned = await store.getJob(job.id);
    expect(orphaned?.status).toBe("running");

    // Enough fake time has to pass for the stalled-grace window.
    // The second kit's handler timeout is the default (30s); reclaim
    // threshold is 30s + STALLED_GRACE_MS (5s) = 35s.
    await vi.advanceTimersByTimeAsync(40_000);

    // Second invocation — fresh kit on the same store.
    const received = vi.fn();
    const dk2 = new DelayKit({ store, scheduler: new PollingScheduler() });
    dk2.handle("task", async ({ key }) => {
      received(key);
    });
    await dk2.poll();

    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith("k:1");
  });

  it("hard-rejects an uncooperative handler at its timeout", async () => {
    // Default timeoutMode is "race" for dk.poll() so the caller
    // (Vercel cron) can return its response before the platform
    // deadline. An uncooperative handler doesn't delay poll() past
    // the configured handler timeout, even though it keeps running
    // in the background.
    const { dk } = createKit();
    let handlerReturned = false;
    dk.handle("uncooperative", {
      handler: async () => {
        await new Promise<void>((resolve) => setTimeout(resolve, 5000));
        handlerReturned = true;
      },
      timeout: "100ms",
    });

    await dk.schedule("uncooperative", { key: "u:1", at: new Date() });

    const p = dk.poll();
    await vi.advanceTimersByTimeAsync(150);
    await p;

    // Handler is still parked in the background — poll() didn't wait
    // for it.
    expect(handlerReturned).toBe(false);

    // Job was marked failed (no retry config → single attempt).
    const active = await dk.getJobByKey("uncooperative", "u:1");
    expect(active).toBeNull();

    // Drain the handler's lingering timer.
    await vi.advanceTimersByTimeAsync(5000);
  });
});

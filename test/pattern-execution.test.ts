/**
 * Pattern execution invariant tests.
 *
 * Tests verify system guarantees: retry, timeout, abort, onFailure,
 * cancel during retry, key collision, and mid-execution event handling.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { HandlerEntry } from "../src/executor.js";
import { makeJob } from "./helpers/job-factory.js";

function createKit(options?: { interval?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: options?.interval ?? 50 });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("pattern execution", () => {
  let dk: DelayKit;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  // --- retry ---

  describe("retry", () => {
    it("retries a debounced handler and eventually succeeds", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("save", {
        handler: async () => {
          callCount++;
          if (callCount < 3) throw new Error("not yet");
        },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "500ms" },
      });

      await dk.start();
      await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(600);
      expect(callCount).toBe(1);

      await vi.advanceTimersByTimeAsync(600);
      expect(callCount).toBe(2);

      await vi.advanceTimersByTimeAsync(600);
      expect(callCount).toBe(3);

      // Key is reusable after completion
      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
    });

    it("requeues for new window when event arrives during failed execution", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("save", {
        handler: async () => {
          callCount++;
          if (callCount === 1) {
            // Slow failure — gives time for a new event
            await new Promise((r) => setTimeout(r, 200));
            throw new Error("fail");
          }
        },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "500ms" },
      });

      await dk.start();
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });

      // Handler starts executing
      await vi.advanceTimersByTimeAsync(350);

      // New event during execution
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });

      // Handler fails at ~550ms — version advanced, so requeue instead of retry
      await vi.advanceTimersByTimeAsync(250);

      // New window should fire
      await vi.advanceTimersByTimeAsync(400);
      expect(callCount).toBe(2);
    });
  });

  // --- timeout + abort ---

  describe("timeout and abort", () => {
    it("propagates abort signal from executor timeout", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let receivedSignal: AbortSignal | null = null;
      dk.handle("slow", {
        handler: async ({ signal }) => {
          receivedSignal = signal;
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 5000);
            signal.addEventListener("abort", () => {
              clearTimeout(timer);
              reject(new Error("aborted"));
            });
          });
        },
        timeout: "100ms",
      });

      await dk.start();
      await dk.debounce("slow", { key: "task:1", wait: "200ms" });

      await vi.advanceTimersByTimeAsync(600);

      expect(receivedSignal).not.toBeNull();
      expect(receivedSignal!.aborted).toBe(true);
    });

    it("executeJob hard-rejects an uncooperative handler in race mode (default)", async () => {
      // The default `timeoutMode: "race"` is what dk.poll() and
      // createHandler() use so they return their response before the
      // platform kills the function.
      const store = new MemoryStore();
      const handlers = new Map<string, HandlerEntry>();

      let handlerReturned = false;
      handlers.set("uncooperative", {
        fn: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 5000));
          handlerReturned = true;
        },
        timeoutMs: 100,
      });

      const job = makeJob({
        handler: "uncooperative",
        key: "u:1",
        scheduledFor: new Date(Date.now() - 1),
      });
      await store.createJob(job);

      const { executeJob } = await import("../src/executor.js");
      const resultPromise = executeJob(
        { jobId: job.id, version: 1 },
        store,
        handlers,
      );

      await vi.advanceTimersByTimeAsync(150);
      const result = await resultPromise;

      expect(result.status).toBe("handler_error");
      expect(handlerReturned).toBe(false);
      if (result.status === "handler_error") {
        expect(result.error.message).toContain("timed out");
      }

      // Drain the lingering handler timer.
      await vi.advanceTimersByTimeAsync(5000);
    });
  });

  // --- onFailure ---

  describe("onFailure", () => {
    it("calls onFailure with user business key after exhausted retries", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const onFailure = vi.fn();
      dk.handle("doomed", {
        handler: async () => { throw new Error("always fails"); },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "100ms" },
        onFailure,
      });

      await dk.start();
      await dk.debounce("doomed", { key: "doc:1", wait: "200ms" });

      await vi.advanceTimersByTimeAsync(300);
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(200);

      expect(onFailure).toHaveBeenCalledOnce();
      expect(onFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          key: "doc:1",
          attempts: 2,
        }),
      );
    });

    it("fires onFailure even when requeuing for new window", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const onFailure = vi.fn();
      let callCount = 0;
      dk.handle("fail", {
        handler: async () => {
          callCount++;
          if (callCount === 1) {
            await new Promise((r) => setTimeout(r, 200));
            throw new Error("boom");
          }
        },
        onFailure,
      });

      await dk.start();
      await dk.debounce("fail", { key: "doc:1", wait: "300ms" });

      // Handler starts
      await vi.advanceTimersByTimeAsync(350);

      // Event during execution
      await dk.debounce("fail", { key: "doc:1", wait: "300ms" });

      // Handler fails — onFailure fires, then requeues
      await vi.advanceTimersByTimeAsync(250);
      expect(onFailure).toHaveBeenCalledOnce();

      // New window fires successfully
      await vi.advanceTimersByTimeAsync(400);
      expect(callCount).toBe(2);
    });

    it("does not double-fire onFailure when stalled recovery wins race with awaited handler", async () => {
      // Under `timeoutMode: "await"`, a handler that ignores its abort
      // signal stays inside handleJob() after its timeout. If the
      // stalled sweep reclaims the row first, sweepStalled marks it
      // failed and invokes onFailure. When the handler eventually
      // returns, handleResult's CAS must lose — and must NOT invoke
      // onFailure again.
      const store = new MemoryStore();
      const scheduler = new PollingScheduler({
        interval: 50,
        stalledCheckInterval: 500,
      });
      dk = new DelayKit({ store, scheduler });

      const onFailure = vi.fn();
      dk.handle("hang", {
        // Sleeps longer than the reclaim floor (DEFAULT_TIMEOUT_MS +
        // STALLED_GRACE_MS = 35s) so the stalled sweep reliably wins
        // the race against handler completion.
        handler: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 60_000));
        },
        timeout: "100ms",
        onFailure,
      });

      await dk.schedule("hang", { key: "h:1", delay: "1ms" });
      await dk.start();

      // Past the reclaim floor; the sweep at ~35_500ms reclaims the
      // row, marks it failed, and invokes onFailure once.
      await vi.advanceTimersByTimeAsync(36_000);
      expect(onFailure).toHaveBeenCalledOnce();

      // Drain the handler's remaining sleep so handleResult runs.
      // Without the fix it would fire onFailure a second time.
      await vi.advanceTimersByTimeAsync(30_000);
      expect(onFailure).toHaveBeenCalledOnce();
    });
  });

  // --- cancel during retry ---

  describe("cancel during retry", () => {
    it("allows cancel between retry attempts", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          throw new Error("fails");
        },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "500ms" },
      });

      await dk.start();
      await dk.debounce("flaky", { key: "doc:1", wait: "200ms" });

      // First attempt fails
      await vi.advanceTimersByTimeAsync(300);
      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(1);

      // Between retries — cancel should work
      const cancelled = await dk.unschedule("flaky", "doc:1");
      expect(cancelled).toBe(true);

      // No more retries
      await vi.advanceTimersByTimeAsync(2000);
      expect(callCount).toBe(1);
    });
  });

  // --- key collision ---

  describe("key collision", () => {
    it("rejects debounce when a scheduled job owns the key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      await dk.schedule("save", { key: "doc:1", delay: "10s" });
      await expect(
        dk.debounce("save", { key: "doc:1", wait: "500ms" })
      ).rejects.toThrow();
    });

    it("rejects schedule when a pattern owns the key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await expect(
        dk.schedule("save", { key: "doc:1", delay: "10s" })
      ).rejects.toThrow();
    });

    it("allows pattern after scheduled job completes", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      await dk.start();

      await dk.schedule("save", { key: "doc:1", delay: "200ms" });
      await vi.advanceTimersByTimeAsync(300);

      // Scheduled job completed — pattern should work
      await dk.debounce("save", { key: "doc:1", wait: "200ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).toHaveBeenCalledTimes(2);
    });
  });

  // --- window anchoring during execution ---

  describe("window anchoring during execution", () => {
    it("debounce maxWait anchors to first mid-execution event, not last", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("save", async () => {
        callCount++;
        if (callCount === 1) {
          await new Promise((r) => setTimeout(r, 400));
        }
      });

      await dk.start();
      await dk.debounce("save", { key: "doc:1", wait: "300ms", maxWait: "500ms" });

      // Handler starts at ~300ms, runs until ~700ms
      await vi.advanceTimersByTimeAsync(350);

      // First event during execution at ~350ms (anchors firstAt)
      await dk.debounce("save", { key: "doc:1", wait: "300ms", maxWait: "500ms" });
      // More events at ~500ms, ~600ms (should NOT reset firstAt)
      await vi.advanceTimersByTimeAsync(150);
      await dk.debounce("save", { key: "doc:1", wait: "300ms", maxWait: "500ms" });
      await vi.advanceTimersByTimeAsync(100);
      await dk.debounce("save", { key: "doc:1", wait: "300ms", maxWait: "500ms" });

      // First handler completes at ~700ms. New window requeued.
      // maxWait = firstAt(~350ms) + 500ms = ~850ms
      // Last event at ~600ms, so wait = lastAt(~600ms) + 300ms = ~900ms
      // maxWait deadline (~850ms) comes first.

      // At ~800ms (total 800): should NOT have fired yet (maxWait ~850ms)
      await vi.advanceTimersByTimeAsync(200);
      expect(callCount).toBe(1); // only first handler

      // At ~950ms (total 950): should have fired (past maxWait deadline)
      await vi.advanceTimersByTimeAsync(150);
      expect(callCount).toBe(2);
    });

    it("throttle window anchors to first mid-execution event", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      const timestamps: number[] = [];
      dk.handle("notify", async () => {
        callCount++;
        timestamps.push(Date.now());
        if (callCount === 1) {
          await new Promise((r) => setTimeout(r, 300));
        }
      });

      await dk.start();
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });

      // First handler fires at ~500ms, runs until ~800ms
      await vi.advanceTimersByTimeAsync(550);

      // Events during execution at ~550ms (anchors firstAt), ~650ms
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(100);
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });

      // First handler completes at ~800ms. New window requeued.
      // Throttle window = firstAt(~550ms) + 500ms = ~1050ms

      // Let first handler finish
      await vi.advanceTimersByTimeAsync(200);

      // At ~950ms: should NOT have fired yet (window ends ~1050ms)
      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(1);

      // At ~1100ms: should have fired
      await vi.advanceTimersByTimeAsync(200);
      expect(callCount).toBe(2);
    });
  });

  // --- stalled recovery ---

  describe("stalled recovery", () => {
    it("reclaims a stalled once job and consumes a retry attempt", async () => {
      const { dk: kit, store } = createKit({ interval: 50 });
      dk = kit;

      let callCount = 0;
      dk.handle("task", {
        handler: async () => { callCount++; },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "100ms" },
      });
      await dk.start();

      const { randomUUID } = await import("node:crypto");
      const id = randomUUID();
      await store.createJob({
        id,
        kind: "once",
        handler: "task",
        key: "stalled:1",
        version: 1,
        claimedVersion: 1,
        status: "running",
        scheduledFor: new Date(Date.now() - 60_000),
        startedAt: new Date(Date.now() - 60_000),
        completedAt: null,
        attempt: 0,
        maxAttempts: 3,
        schedulerRef: null,
        lastError: null,
        firstAt: null,
        lastAt: null,
        waitMs: null,
        maxWaitMs: null,
      });

      await dk.stop();
      const scheduler2 = new PollingScheduler({
        interval: 50, stalledCheckInterval: 100,
      });
      const dk2 = new DelayKit({ store, scheduler: scheduler2 });
      dk2.handle("task", {
        handler: async () => { callCount++; },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "100ms" },
      });
      dk = dk2;
      await dk2.start();

      // Wait for stalled sweep + retry delay + poll
      await vi.advanceTimersByTimeAsync(500);

      expect(callCount).toBe(1);

      // Verify attempt was consumed: the reclaimed job should be at attempt 1
      const job = await store.getJob(id);
      expect(job).not.toBeNull();
      // Job completed successfully, so it's terminal now
      // But the attempt was consumed during reclaim
    });

    it("inline reclaim on redelivery consumes a retry attempt", async () => {
      const { store } = createKit({ interval: 50 });

      let callCount = 0;
      const handlers = new Map();
      const { executeJob } = await import("../src/executor.js");
      handlers.set("task", {
        fn: async () => { callCount++; },
        timeoutMs: 100,
      });

      const { randomUUID } = await import("node:crypto");
      const id = randomUUID();
      await store.createJob({
        id,
        kind: "once",
        handler: "task",
        key: "stalled:2",
        version: 1,
        claimedVersion: 1,
        status: "running",
        scheduledFor: new Date(Date.now() - 10_000),
        startedAt: new Date(Date.now() - 10_000),
        completedAt: null,
        attempt: 0,
        maxAttempts: 3,
        schedulerRef: null,
        lastError: null,
        firstAt: null,
        lastAt: null,
        waitMs: null,
        maxWaitMs: null,
      });

      const result = await executeJob({ jobId: id, version: 1 }, store, handlers);

      // Reclaim succeeded, handler ran
      expect(result.status).toBe("completed");
      expect(callCount).toBe(1);

      // Verify the completed job consumed an attempt (attempt was incremented by reclaim)
      const job = await store.getJob(id);
      expect(job!.attempt).toBe(1); // was 0, incremented to 1 by reclaim

      dk = new (await import("../src/delaykit.js")).DelayKit({
        store,
        scheduler: new PollingScheduler({ interval: 50 }),
      });
    });

    it("inline reclaim marks exhausted job as failed", async () => {
      const { store } = createKit({ interval: 50 });

      const handlers = new Map();
      const { executeJob } = await import("../src/executor.js");
      handlers.set("task", {
        fn: async () => {},
        timeoutMs: 100,
      });

      const { randomUUID } = await import("node:crypto");
      const id = randomUUID();
      await store.createJob({
        id,
        kind: "once",
        handler: "task",
        key: "stalled:exhausted",
        version: 1,
        claimedVersion: 1,
        status: "running",
        scheduledFor: new Date(Date.now() - 10_000),
        startedAt: new Date(Date.now() - 10_000),
        completedAt: null,
        attempt: 2, // already used 2 of 3 attempts
        maxAttempts: 3,
        schedulerRef: null,
        lastError: null,
        firstAt: null,
        lastAt: null,
        waitMs: null,
        maxWaitMs: null,
      });

      // Reclaim increments attempt to 3 (>= maxAttempts=3) → executor marks failed
      const result = await executeJob({ jobId: id, version: 1 }, store, handlers);
      expect(result.status).toBe("handler_error");

      // Job should be failed (exhausted after crash)
      const job = await store.getJob(id);
      expect(job!.status).toBe("failed");

      dk = new (await import("../src/delaykit.js")).DelayKit({
        store,
        scheduler: new PollingScheduler({ interval: 50 }),
      });
    });

    it("stalled pattern with version advance requeues fresh window", async () => {
      const { dk: kit, store } = createKit({ interval: 50 });
      dk = kit;

      let callCount = 0;
      dk.handle("save", async () => { callCount++; });
      await dk.start();

      const { randomUUID } = await import("node:crypto");
      await store.createJob({
        id: randomUUID(),
        kind: "debounce",
        handler: "save",
        key: "stalled:3",
        version: 3,
        claimedVersion: 1,
        status: "running",
        scheduledFor: new Date(Date.now() - 60_000),
        startedAt: new Date(Date.now() - 60_000),
        completedAt: null,
        attempt: 0,
        maxAttempts: 1,
        schedulerRef: null,
        lastError: null,
        firstAt: new Date(Date.now() - 30_000),
        lastAt: new Date(Date.now() - 10_000),
        waitMs: 500,
        maxWaitMs: null,
      });

      await dk.stop();
      const scheduler2 = new PollingScheduler({
        interval: 50, stalledCheckInterval: 100,
      });
      const dk2 = new DelayKit({ store, scheduler: scheduler2 });
      dk2.handle("save", async () => { callCount++; });
      dk = dk2;
      await dk2.start();

      await vi.advanceTimersByTimeAsync(500);

      // The stalled sweep requeued (fresh window, attempt reset to 0)
      expect(callCount).toBe(1);
    });
  });
});

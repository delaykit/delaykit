/**
 * Shared scheduler contract suite.
 *
 * Every execution path (PollingScheduler, ExternalSchedulerHarness) must
 * satisfy these behavioral invariants from docs/invariants.md.
 *
 * When a test fails, check the invariant first — the code is likely wrong.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { Store, Job } from "../src/types.js";
import { DelayKit } from "../src/delaykit.js";
import { assertJobInvariants, assertKeyReusable } from "./helpers/invariants.js";

export interface SchedulerTestContext {
  dk: DelayKit;
  store: Store;
  /**
   * Advance time and deliver due jobs.
   * - PollingScheduler: vi.advanceTimersByTimeAsync(ms)
   * - ExternalScheduler: deliver hooks whose scheduledFor <= now
   */
  advance(ms: number): Promise<void>;
  /** Tear down the scheduler (stop timers, etc.) */
  teardown(): Promise<void>;
}

export interface SchedulerContractOptions {
  /**
   * Handler uses setTimeout internally — requires non-blocking delivery.
   * false for external harness (deliver() blocks until handler completes).
   */
  asyncHandlers?: boolean;
  /** Transport has a sweep/reclaim mechanism for stalled jobs. */
  stalledRecovery?: boolean;
}

export function schedulerContractSuite(
  name: string,
  setup: () => SchedulerTestContext,
  options: SchedulerContractOptions = {},
) {
  const { asyncHandlers = true, stalledRecovery = true } = options;

  describe(`Scheduler contract: ${name}`, () => {
    let ctx: SchedulerTestContext;

    beforeEach(() => {
      ctx = setup();
    });

    afterEach(async () => {
      await ctx.teardown();
    });

    // =========================================================================
    // Once jobs
    // =========================================================================

    describe("once jobs", () => {
      it("fires handler at scheduled time", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async ({ key }) => { received(key); });

        await ctx.dk.schedule("task", { key: "o:1", delay: "1s" });
        await ctx.advance(1_100);

        expect(received).toHaveBeenCalledOnce();
        expect(received).toHaveBeenCalledWith("o:1");
      });

      it("does NOT fire before scheduled time", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async () => { received(); });

        await ctx.dk.schedule("task", { key: "o:2", delay: "5s" });
        await ctx.advance(3_000);

        expect(received).not.toHaveBeenCalled();
      });

      it("cancel prevents execution", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async () => { received(); });

        const { job } = await ctx.dk.schedule("task", { key: "o:3", delay: "1s" });
        await ctx.dk.cancel(job.id);
        await ctx.advance(2_000);

        expect(received).not.toHaveBeenCalled();
      });

      it("replace: executes at new time, not old", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async ({ key }) => { received(key); });

        await ctx.dk.schedule("task", { key: "o:4", delay: "2s" });
        await ctx.dk.schedule("task", { key: "o:4", delay: "5s", onDuplicate: "replace" });

        // Past original time — should NOT have fired
        await ctx.advance(3_000);
        expect(received).not.toHaveBeenCalled();

        // Past replacement time
        await ctx.advance(3_000);
        expect(received).toHaveBeenCalledOnce();
      });

      it("replace: cancels old scheduler artifact", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async ({ key }) => { received(key); });

        await ctx.dk.schedule("task", { key: "o:5", delay: "1s" });
        const { job } = await ctx.dk.schedule("task", {
          key: "o:5", delay: "3s", onDuplicate: "replace",
        });

        // Old time passes — no fire
        await ctx.advance(1_500);
        expect(received).not.toHaveBeenCalled();

        // New time — fires
        await ctx.advance(2_000);
        expect(received).toHaveBeenCalledOnce();

        const final = await ctx.store.getJob(job.id);
        expect(final!.status).toBe("completed");
      });

      it("idempotent: same key returns existing (skip)", async () => {
        ctx.dk.handle("task", async () => {});

        const first = await ctx.dk.schedule("task", { key: "o:6", delay: "5s" });
        const second = await ctx.dk.schedule("task", { key: "o:6", delay: "10s" });

        expect(first.created).toBe(true);
        expect(second.created).toBe(false);
        expect(second.job.id).toBe(first.job.id);
      });

      it("key reusable after completion", async () => {
        const received = vi.fn();
        ctx.dk.handle("task", async ({ key }) => { received(key); });

        await ctx.dk.schedule("task", { key: "o:7", delay: "1s" });
        await ctx.advance(1_500);
        expect(received).toHaveBeenCalledOnce();

        // Key is now free
        await assertKeyReusable(ctx.store, "task", "o:7");
        const second = await ctx.dk.schedule("task", { key: "o:7", delay: "1s" });
        expect(second.created).toBe(true);

        await ctx.advance(1_500);
        expect(received).toHaveBeenCalledTimes(2);
      });

      it("marks job completed with valid invariants", async () => {
        ctx.dk.handle("task", async () => {});

        const { job } = await ctx.dk.schedule("task", { key: "o:8", delay: "1s" });
        await ctx.advance(1_500);

        const final = await ctx.store.getJob(job.id);
        expect(final!.status).toBe("completed");
        assertJobInvariants(final!);
      });
    });

    // =========================================================================
    // Debounce
    // =========================================================================

    describe("debounce", () => {
      it("fires once after activity settles", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async ({ key }) => { received(key); });

        await ctx.dk.debounce("save", { key: "d:1", wait: "500ms" });
        await ctx.advance(600);

        expect(received).toHaveBeenCalledOnce();
        expect(received).toHaveBeenCalledWith("d:1");
      });

      it("does NOT fire before settlement", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async () => { received(); });

        await ctx.dk.debounce("save", { key: "d:2", wait: "500ms" });
        await ctx.advance(400);

        expect(received).not.toHaveBeenCalled();
      });

      it("second event extends settlement window", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async () => { received(); });

        await ctx.dk.debounce("save", { key: "d:3", wait: "500ms" });

        // Event at 300ms resets the timer
        await ctx.advance(300);
        await ctx.dk.debounce("save", { key: "d:3", wait: "500ms" });

        // 500ms from first event — should NOT have fired (timer was reset)
        await ctx.advance(200);
        expect(received).not.toHaveBeenCalled();

        // 500ms from second event — should fire
        await ctx.advance(400);
        expect(received).toHaveBeenCalledOnce();
      });

      it("maxWait forces execution at deadline", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async () => { received(); });

        // Continuous activity with maxWait
        await ctx.dk.debounce("save", { key: "d:4", wait: "500ms", maxWait: "1s" });
        await ctx.advance(400);
        await ctx.dk.debounce("save", { key: "d:4", wait: "500ms", maxWait: "1s" });
        await ctx.advance(400);
        await ctx.dk.debounce("save", { key: "d:4", wait: "500ms", maxWait: "1s" });

        // Not settled, but maxWait (1s from first event) forces execution
        await ctx.advance(300);
        expect(received).toHaveBeenCalledOnce();
      });

      it("does NOT fire before maxWait deadline", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async () => { received(); });

        await ctx.dk.debounce("save", { key: "d:5", wait: "500ms", maxWait: "1s" });
        await ctx.advance(400);
        await ctx.dk.debounce("save", { key: "d:5", wait: "500ms", maxWait: "1s" });

        // At 800ms: not settled (last event at 400ms, wait=500ms → due at 900ms)
        // maxWait deadline = 1000ms. Neither met yet at 800ms.
        // But the debounce due time is lastAt+wait = 400+500 = 900ms
        // And maxWait = 0+1000 = 1000ms. Min of these is 900ms.
        await ctx.advance(400); // total 800ms
        expect(received).not.toHaveBeenCalled();

        // At 1000ms: should fire (settlement at 900ms or maxWait at 1000ms)
        await ctx.advance(200);
        expect(received).toHaveBeenCalledOnce();
      });

      // Requires non-blocking delivery so handler can run while we inject events.
      // Covered in race-conditions.test.ts for external scheduler.
      it.skipIf(!asyncHandlers)("event during execution triggers new window after completion", async () => {
        let callCount = 0;
        ctx.dk.handle("save", async () => {
          callCount++;
          if (callCount === 1) {
            await new Promise((r) => setTimeout(r, 200));
          }
        });

        await ctx.dk.debounce("save", { key: "d:6", wait: "300ms" });

        // Handler starts executing
        await ctx.advance(350);

        // New event while handler is running
        await ctx.dk.debounce("save", { key: "d:6", wait: "300ms" });

        // Let first execution complete + second window settle
        await ctx.advance(250);
        await ctx.advance(350);

        expect(callCount).toBe(2);
      });

      it("key reusable after completion", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async ({ key }) => { received(key); });

        await ctx.dk.debounce("save", { key: "d:7", wait: "300ms" });
        await ctx.advance(400);
        expect(received).toHaveBeenCalledOnce();

        await ctx.dk.debounce("save", { key: "d:7", wait: "300ms" });
        await ctx.advance(400);
        expect(received).toHaveBeenCalledTimes(2);
      });
    });

    // =========================================================================
    // Throttle
    // =========================================================================

    describe("throttle", () => {
      it("fires once at end of fixed window", async () => {
        const received = vi.fn();
        ctx.dk.handle("notify", async ({ key }) => { received(key); });

        await ctx.dk.throttle("notify", { key: "t:1", wait: "500ms" });
        await ctx.advance(100);
        await ctx.dk.throttle("notify", { key: "t:1", wait: "500ms" });
        await ctx.advance(100);
        await ctx.dk.throttle("notify", { key: "t:1", wait: "500ms" });

        await ctx.advance(400);

        expect(received).toHaveBeenCalledOnce();
        expect(received).toHaveBeenCalledWith("t:1");
      });

      it("does NOT fire before window end", async () => {
        const received = vi.fn();
        ctx.dk.handle("notify", async () => { received(); });

        await ctx.dk.throttle("notify", { key: "t:2", wait: "500ms" });
        await ctx.advance(400);

        expect(received).not.toHaveBeenCalled();
      });

      it("new events don't extend window", async () => {
        const received = vi.fn();
        ctx.dk.handle("notify", async () => { received(); });

        await ctx.dk.throttle("notify", { key: "t:3", wait: "500ms" });
        await ctx.advance(400);
        await ctx.dk.throttle("notify", { key: "t:3", wait: "500ms" });

        // Should still fire at ~500ms from first event
        await ctx.advance(200);
        expect(received).toHaveBeenCalledOnce();
      });

      // Requires non-blocking delivery so handler can run while we inject events.
      it.skipIf(!asyncHandlers)("event during execution triggers new window anchored to first event", async () => {
        let callCount = 0;
        ctx.dk.handle("notify", async () => {
          callCount++;
          if (callCount === 1) {
            await new Promise((r) => setTimeout(r, 200));
          }
        });

        await ctx.dk.throttle("notify", { key: "t:4", wait: "300ms" });

        // Handler starts executing
        await ctx.advance(350);

        // Events during execution
        await ctx.dk.throttle("notify", { key: "t:4", wait: "300ms" });

        // Let execution complete + second window
        await ctx.advance(250);
        await ctx.advance(350);

        expect(callCount).toBe(2);
      });

      it("key reusable after completion", async () => {
        const received = vi.fn();
        ctx.dk.handle("notify", async ({ key }) => { received(key); });

        await ctx.dk.throttle("notify", { key: "t:5", wait: "500ms" });
        await ctx.advance(600);
        expect(received).toHaveBeenCalledOnce();

        await ctx.dk.throttle("notify", { key: "t:5", wait: "500ms" });
        await ctx.advance(600);
        expect(received).toHaveBeenCalledTimes(2);
      });
    });

    // =========================================================================
    // Retry + failure
    // =========================================================================

    describe("retry + failure", () => {
      it("retries on failure, eventually succeeds", async () => {
        let callCount = 0;
        ctx.dk.handle("flaky", {
          handler: async () => {
            callCount++;
            if (callCount < 3) throw new Error("not yet");
          },
          retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
        });

        await ctx.dk.schedule("flaky", { key: "r:1", delay: "1s" });

        // First attempt
        await ctx.advance(1_100);
        expect(callCount).toBe(1);

        // Second attempt
        await ctx.advance(1_100);
        expect(callCount).toBe(2);

        // Third attempt — succeeds
        await ctx.advance(1_100);
        expect(callCount).toBe(3);

        // Key reusable after success
        await assertKeyReusable(ctx.store, "flaky", "r:1");
      });

      it("exhausted retries call onFailure with business key", async () => {
        const onFailure = vi.fn();
        ctx.dk.handle("doomed", {
          handler: async () => { throw new Error("always fails"); },
          retry: { attempts: 2, backoff: "fixed", initialDelay: "1s" },
          onFailure,
        });

        await ctx.dk.schedule("doomed", { key: "r:2", delay: "1s" });

        // First attempt
        await ctx.advance(1_100);
        // Second attempt — exhausted
        await ctx.advance(1_100);

        expect(onFailure).toHaveBeenCalledOnce();
        expect(onFailure).toHaveBeenCalledWith(
          expect.objectContaining({ key: "r:2", attempts: 2 }),
        );

        await assertKeyReusable(ctx.store, "doomed", "r:2");
      });

      it("cancel between retry attempts works", async () => {
        let callCount = 0;
        ctx.dk.handle("flaky", {
          handler: async () => {
            callCount++;
            throw new Error("fails");
          },
          retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
        });

        await ctx.dk.schedule("flaky", { key: "r:3", delay: "1s" });

        // First attempt fails
        await ctx.advance(1_100);
        expect(callCount).toBe(1);

        // Cancel between retries
        const cancelled = await ctx.dk.unschedule("flaky", "r:3");
        expect(cancelled).toBe(true);

        // No more retries
        await ctx.advance(5_000);
        expect(callCount).toBe(1);
      });
    });

    // =========================================================================
    // Recovery (stalled jobs)
    // =========================================================================

    // Recovery requires a stalled sweep mechanism (PollingScheduler has one,
    // ExternalSchedulerHarness does not — Posthook handles recovery via redelivery).
    describe.skipIf(!stalledRecovery)("recovery", () => {
      it("stalled job reclaimed on next cycle", async () => {
        let callCount = 0;
        ctx.dk.handle("task", {
          handler: async () => { callCount++; },
          timeout: "100ms",
          retry: { attempts: 3, backoff: "fixed", initialDelay: "100ms" },
        });

        // Create a stalled job (running with expired startedAt)
        const { randomUUID } = await import("node:crypto");
        const id = randomUUID();
        await ctx.store.createJob({
          id,
          kind: "once",
          handler: "task",
          key: "rec:1",
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

        // Advance enough for stalled sweep + poll
        await ctx.advance(1_000);

        expect(callCount).toBe(1);

        const job = await ctx.store.getJob(id);
        assertJobInvariants(job!);
      });

      it("stalled reclaim consumes attempt", async () => {
        ctx.dk.handle("task", {
          handler: async () => {},
          timeout: "100ms",
          retry: { attempts: 3, backoff: "fixed", initialDelay: "100ms" },
        });

        const { randomUUID } = await import("node:crypto");
        const id = randomUUID();
        await ctx.store.createJob({
          id,
          kind: "once",
          handler: "task",
          key: "rec:2",
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

        await ctx.advance(1_000);

        // Job completed after reclaim, but attempt was consumed by reclaim
        const job = await ctx.store.getJob(id);
        expect(job!.attempt).toBeGreaterThanOrEqual(1);
      });
    });

    // =========================================================================
    // Key collision
    // =========================================================================

    describe("key collision", () => {
      it("schedule rejects when pattern active", async () => {
        ctx.dk.handle("save", async () => {});

        await ctx.dk.debounce("save", { key: "col:1", wait: "500ms" });
        await expect(
          ctx.dk.schedule("save", { key: "col:1", delay: "10s" }),
        ).rejects.toThrow("pattern is active");
      });

      it("pattern rejects when scheduled job active", async () => {
        ctx.dk.handle("save", async () => {});

        await ctx.dk.schedule("save", { key: "col:2", delay: "10s" });
        await expect(
          ctx.dk.debounce("save", { key: "col:2", wait: "500ms" }),
        ).rejects.toThrow();
      });

      it("key reusable after completion", async () => {
        const received = vi.fn();
        ctx.dk.handle("save", async ({ key }) => { received(key); });

        await ctx.dk.schedule("save", { key: "col:3", delay: "1s" });
        await ctx.advance(1_500);
        expect(received).toHaveBeenCalledOnce();

        // Pattern now works on same key
        await ctx.dk.debounce("save", { key: "col:3", wait: "300ms" });
        await ctx.advance(400);
        expect(received).toHaveBeenCalledTimes(2);
      });
    });
  });
}

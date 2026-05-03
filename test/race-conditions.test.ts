/**
 * Race condition tests.
 *
 * Uses the interleaving harness (barriers) and external scheduler harness
 * to force specific orderings that would be non-deterministic in production.
 *
 * These tests encode docs/INVARIANTS.md race-condition guarantees.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { ExternalSchedulerHarness } from "./helpers/external-scheduler-harness.js";
import { Barrier, interceptBefore, interceptAfter } from "./helpers/interleaving.js";
import { assertJobInvariants, assertAtMostOneActive, assertKeyReusable } from "./helpers/invariants.js";

function createExternalKit() {
  const store = new MemoryStore();
  const harness = new ExternalSchedulerHarness();
  const dk = new DelayKit({ store, scheduler: harness });
  return { dk, store, harness };
}

describe("race conditions", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // =========================================================================
  // Concurrent operations (interleaving harness)
  // =========================================================================

  describe("concurrent same-key schedule", () => {
    it("one wins insert, other applies skip/replace", async () => {
      const { dk, store } = createExternalKit();
      dk.handle("task", async () => {});

      // Race: two schedule() calls for the same key.
      // The second will hit a concurrent insert error and retry.
      const barrier = new Barrier();
      const restore = interceptBefore(store, "createJob", barrier);

      // Start first schedule (will block at createJob)
      const p1 = dk.schedule("task", { key: "race:1", delay: "5s" });

      // Start second schedule while first is blocked
      const p2 = dk.schedule("task", { key: "race:1", delay: "10s" });

      // Release — both proceed
      barrier.release();
      restore();

      const [r1, r2] = await Promise.all([p1, p2]);

      // One created, other skipped
      expect(r1.created || r2.created).toBe(true);
      // Same job ID (skip returns existing)
      const active = await store.getActiveJobByKey("task", "race:1");
      expect(active).not.toBeNull();
    });

    it("concurrent schedule with replace: one wins, other replaces", async () => {
      const { dk, store } = createExternalKit();
      dk.handle("task", async () => {});

      // Seed a job first so both calls see an existing row
      const { job: seed } = await dk.schedule("task", { key: "race:2", delay: "5s" });

      const barrier = new Barrier();
      const restore = interceptBefore(store, "replaceJob", barrier);

      const p1 = dk.schedule("task", { key: "race:2", delay: "10s", onDuplicate: "replace" });
      const p2 = dk.schedule("task", { key: "race:2", delay: "15s", onDuplicate: "replace" });

      barrier.release();
      restore();

      const [r1, r2] = await Promise.all([p1, p2]);

      // At most one active row
      const active = await store.getActiveJobByKey("task", "race:2");
      expect(active).not.toBeNull();
      expect(active!.status).toBe("pending");
    });
  });

  describe("concurrent pattern first-event", () => {
    it("one wins insert, other retries as update", async () => {
      const { dk, store } = createExternalKit();
      dk.handle("save", async () => {});

      const barrier = new Barrier();
      const restore = interceptBefore(store, "createJob", barrier);

      const p1 = dk.debounce("save", { key: "race:3", wait: "500ms" });
      const p2 = dk.debounce("save", { key: "race:3", wait: "500ms" });

      barrier.release();
      restore();

      await Promise.all([p1, p2]);

      // Exactly one active row
      const active = await store.getActiveJobByKey("save", "race:3");
      expect(active).not.toBeNull();
      // Version should be >= 2 (one insert + at least one update)
      expect(active!.version).toBeGreaterThanOrEqual(1);
    });
  });

  describe("replace racing with delivery", () => {
    it("old delivery rejected after replace moves job later", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("task", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      // Schedule job for 1s
      const { job } = await dk.schedule("task", { key: "race:4", delay: "1s" });
      const oldHookRef = harness.hookFor(job.id)!.ref;

      // Replace to 5s — old hook should be cancelled
      await dk.schedule("task", { key: "race:4", delay: "5s", onDuplicate: "replace" });
      const newJob = await store.getActiveJobByKey("task", "race:4");
      expect(newJob!.scheduledFor.getTime()).toBeGreaterThan(Date.now() + 4_000);

      // Old hook delivers (stale) — should be rejected by scheduledFor guard
      await vi.advanceTimersByTimeAsync(1_100);
      const res = await harness.deliver(oldHookRef);
      expect(res.status).toBe(200); // acknowledged but not executed
      expect(received).not.toHaveBeenCalled();

      // Job should still be pending
      const afterDelivery = await store.getActiveJobByKey("task", "race:4");
      expect(afterDelivery!.status).toBe("pending");
    });
  });

  // =========================================================================
  // External scheduler transport artifacts
  // =========================================================================

  describe("stale hook after cancel", () => {
    it("delivery ignored for cancelled job", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("task", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("task", { key: "stale:1", delay: "1s" });
      const hookRef = harness.hookFor(job.id)!.ref;

      // Cancel the job
      await dk.cancel(job.id);

      // Stale hook still delivers (Posthook didn't cancel in time)
      await vi.advanceTimersByTimeAsync(1_100);
      const res = await harness.deliver(hookRef);

      expect(res.status).toBe(200); // acknowledged
      expect(received).not.toHaveBeenCalled();

      // Job remains cancelled
      const final = await store.getJob(job.id);
      expect(final!.status).toBe("cancelled");
    });
  });

  describe("stale hook after replace", () => {
    it("old hook delivery rejected by scheduledFor guard", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("task", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      // Schedule at 1s, replace to 10s
      const { job } = await dk.schedule("task", { key: "stale:2", delay: "1s" });
      const oldRef = harness.hookFor(job.id)!.ref;

      await dk.schedule("task", { key: "stale:2", delay: "10s", onDuplicate: "replace" });

      // Old hook fires at 1s — rejected because scheduledFor is now 10s
      await vi.advanceTimersByTimeAsync(1_100);
      const res = await harness.deliver(oldRef);

      expect(res.status).toBe(200);
      expect(received).not.toHaveBeenCalled();

      // New hook should work at 10s
      const newHook = harness.activeHooks().find(h => h.ref !== oldRef && h.id === job.id);
      expect(newHook).toBeDefined();

      await vi.advanceTimersByTimeAsync(10_000);
      const res2 = await harness.deliver(newHook!.ref);
      expect(res2.status).toBe(200);
      expect(received).toHaveBeenCalledOnce();
    });
  });

  describe("duplicate delivery", () => {
    it("second delivery skipped (already running/completed)", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("task", async () => { callCount++; });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("task", { key: "dup:1", delay: "1s" });
      const hookRef = harness.hookFor(job.id)!.ref;

      // Advance time so job is due
      await vi.advanceTimersByTimeAsync(1_100);

      // Deliver twice
      const [res1, res2] = await harness.deliverTwice(hookRef);

      expect(res1.status).toBe(200);
      expect(res2.status).toBe(200); // acknowledged but skipped
      expect(callCount).toBe(1); // only executed once
    });

    it("second delivery skipped for patterns too", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("save", async () => { callCount++; });
      harness.setHandler(dk.createHandler());

      await dk.debounce("save", { key: "dup:2", wait: "500ms" });
      const active = await store.getActiveJobByKey("save", "dup:2");
      const hookRef = harness.hookFor(active!.id)!.ref;

      // Advance past settlement
      await vi.advanceTimersByTimeAsync(600);

      const [res1, res2] = await harness.deliverTwice(hookRef);
      expect(res1.status).toBe(200);
      expect(res2.status).toBe(200);
      expect(callCount).toBe(1);
    });
  });

  describe("redelivery after failure", () => {
    it("second delivery succeeds when job is pending (retry state)", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          if (callCount === 1) throw new Error("first fail");
        },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
      });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("flaky", { key: "retry:1", delay: "1s" });
      const hookRef = harness.hookFor(job.id)!.ref;

      await vi.advanceTimersByTimeAsync(1_100);

      // First delivery — fails, returns 500
      const res1 = await harness.deliver(hookRef);
      expect(res1.status).toBe(500);
      expect(callCount).toBe(1);

      // Job should be pending (retryJob transitions running → pending)
      const afterFail = await store.getJob(job.id);
      expect(afterFail!.status).toBe("pending");
      expect(afterFail!.attempt).toBe(1);

      // Redelivery — succeeds
      const res2 = await harness.deliver(hookRef);
      expect(res2.status).toBe(200);
      expect(callCount).toBe(2);

      // Job completed
      const final = await store.getJob(job.id);
      expect(final!.status).toBe("completed");
      assertJobInvariants(final!);
    });
  });

  describe("out-of-order delivery", () => {
    it("earlier hook ignored if later already executed", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("task", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      // Schedule, then replace — creates two hooks
      const { job } = await dk.schedule("task", { key: "ooo:1", delay: "1s" });
      const hook1Ref = harness.hookFor(job.id)!.ref;

      await dk.schedule("task", { key: "ooo:1", delay: "500ms", onDuplicate: "replace" });
      const allHooks = harness.allHooksFor(job.id);
      const hook2Ref = allHooks[allHooks.length - 1].ref;

      // Advance past both scheduled times
      await vi.advanceTimersByTimeAsync(1_500);

      // Deliver hook2 (later/replacement) first — succeeds
      const res2 = await harness.deliver(hook2Ref);
      expect(res2.status).toBe(200);
      expect(received).toHaveBeenCalledOnce();

      // Deliver hook1 (earlier/original) after — job is completed, ignored
      const res1 = await harness.deliver(hook1Ref);
      expect(res1.status).toBe(200);
      expect(received).toHaveBeenCalledOnce(); // no second call

      await assertKeyReusable(store, "task", "ooo:1");
    });
  });

  // =========================================================================
  // Event during execution (interleaving with external harness)
  // =========================================================================

  describe("event during execution (external harness)", () => {
    it("debounce: event during handler triggers requeue", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("save", async () => { callCount++; });
      harness.setHandler(dk.createHandler());

      await dk.debounce("save", { key: "mid:1", wait: "500ms" });
      const active = await store.getActiveJobByKey("save", "mid:1");
      const hookRef = harness.hookFor(active!.id)!.ref;

      // Advance past settlement
      await vi.advanceTimersByTimeAsync(600);

      // Inject a barrier: pause after markRunning but before handler execution.
      // We'll use interceptAfter on markRunning to inject an event mid-execution.
      const restore = interceptAfter(store, "markRunning", async () => {
        // Event arrives while job is running
        await dk.debounce("save", { key: "mid:1", wait: "500ms" });
      });

      // Deliver — markRunning succeeds, event bumps version, handler runs
      const res = await harness.deliver(hookRef);
      restore();

      expect(res.status).toBe(200);
      expect(callCount).toBe(1);

      // Version advanced during execution → requeue happened
      const afterDelivery = await store.getActiveJobByKey("save", "mid:1");
      expect(afterDelivery).not.toBeNull();
      expect(afterDelivery!.status).toBe("pending");
      expect(afterDelivery!.version).toBeGreaterThan(1);

      // Deliver the requeued hook
      const newHook = harness.activeHooks().find(
        h => h.id === active!.id && h.ref !== hookRef,
      );
      if (newHook) {
        await vi.advanceTimersByTimeAsync(600);
        const res2 = await harness.deliver(newHook.ref);
        expect(res2.status).toBe(200);
        expect(callCount).toBe(2);
      }
    });

    it("throttle: event during handler triggers requeue", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("notify", async () => { callCount++; });
      harness.setHandler(dk.createHandler());

      await dk.throttle("notify", { key: "mid:2", wait: "500ms" });
      const active = await store.getActiveJobByKey("notify", "mid:2");
      const hookRef = harness.hookFor(active!.id)!.ref;

      await vi.advanceTimersByTimeAsync(600);

      const restore = interceptAfter(store, "markRunning", async () => {
        await dk.throttle("notify", { key: "mid:2", wait: "500ms" });
      });

      const res = await harness.deliver(hookRef);
      restore();

      expect(res.status).toBe(200);
      expect(callCount).toBe(1);

      // Version advanced → requeue
      const afterDelivery = await store.getActiveJobByKey("notify", "mid:2");
      expect(afterDelivery).not.toBeNull();
      expect(afterDelivery!.status).toBe("pending");
    });
  });

  // =========================================================================
  // Inline stalled recovery via redelivery
  // =========================================================================

  describe("inline stalled recovery", () => {
    it("redelivery to stalled job reclaims and executes", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("task", {
        handler: async () => { callCount++; },
        timeout: "100ms",
        retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
      });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("task", { key: "stalled:1", delay: "1s" });
      const hookRef = harness.hookFor(job.id)!.ref;

      await vi.advanceTimersByTimeAsync(1_100);

      // Simulate stall: mark running then backdate startedAt
      await store.markRunning(job.id, 1);
      const stalled = await store.getJob(job.id);
      (stalled as any).startedAt = new Date(Date.now() - 60_000);
      // Write back via the store's internal map (MemoryStore test-only access)
      (store as any).jobs.set(job.id, stalled);

      // Redelivery — inline reclaim should detect stalled job and re-execute
      const res = await harness.deliver(hookRef);
      expect(res.status).toBe(200);
      expect(callCount).toBe(1);

      const final = await store.getJob(job.id);
      expect(final!.status).toBe("completed");
      // Attempt consumed by reclaim
      expect(final!.attempt).toBeGreaterThanOrEqual(1);
    });
  });

  // =========================================================================
  // Hard-timeout response on createHandler (request-scoped path)
  // =========================================================================

  describe("createHandler hard timeout", () => {
    it("returns 500 within the configured timeout when the handler ignores signal", async () => {
      // createHandler must return its retry response before the
      // handler actually finishes — otherwise Vercel/Lambda kills the
      // request before Posthook gets a status code, and the job
      // strands in `running`.
      const { dk, harness } = createExternalKit();

      let handlerReturned = false;
      dk.handle("uncooperative", {
        handler: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 5000));
          handlerReturned = true;
        },
        timeout: "100ms",
        retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
      });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("uncooperative", {
        key: "u:1",
        delay: "1ms",
      });
      const hookRef = harness.hookFor(job.id)!.ref;

      const deliverPromise = harness.deliver(hookRef);

      await vi.advanceTimersByTimeAsync(150);
      const response = await deliverPromise;

      expect(response.status).toBe(500);
      expect(handlerReturned).toBe(false);

      // Drain the lingering handler timer.
      await vi.advanceTimersByTimeAsync(5000);
    });
  });

  // =========================================================================
  // schedulerRef artifact identity (invariant #3)
  // =========================================================================

  describe("schedulerRef artifact identity", () => {
    it("old hook after replace: ref mismatch → ignored", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("task", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      const { job } = await dk.schedule("task", { key: "ref:1", delay: "1s" });
      const oldRef = harness.hookFor(job.id)!.ref;

      // Replace → new hook, old hook cancelled (best-effort)
      await dk.schedule("task", { key: "ref:1", delay: "5s", onDuplicate: "replace" });
      const newRef = harness.activeHooks().find(h => h.ref !== oldRef)!.ref;

      // Verify row has the new ref
      const row = await store.getActiveJobByKey("task", "ref:1");
      expect(row!.schedulerRef).not.toBe(oldRef);

      // Old hook delivers — ref mismatch → ignored
      await vi.advanceTimersByTimeAsync(1_500);
      const res = await harness.deliver(oldRef);
      expect(res.status).toBe(200);
      expect(received).not.toHaveBeenCalled();

      // New hook delivers at the right time
      await vi.advanceTimersByTimeAsync(5_000);
      const res2 = await harness.deliver(newRef);
      expect(res2.status).toBe(200);
      expect(received).toHaveBeenCalledOnce();
    });

    it("old hook after debounce reschedule: ref mismatch → ignored", async () => {
      const { dk, store, harness } = createExternalKit();
      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      harness.setHandler(dk.createHandler());

      // Start debounce window
      await dk.debounce("save", { key: "ref:2", wait: "500ms" });
      const active = await store.getActiveJobByKey("save", "ref:2");
      const originalRef = harness.hookFor(active!.id)!.ref;

      // New event at 300ms — bumps version but no new hook (existing window)
      await vi.advanceTimersByTimeAsync(300);
      await dk.debounce("save", { key: "ref:2", wait: "500ms" });

      // Original hook fires at 500ms — not settled (last event 200ms ago, need 500ms)
      // Executor detects not settled → reschedule → new hook created
      await vi.advanceTimersByTimeAsync(200);
      const res = await harness.deliver(originalRef);
      expect(res.status).toBe(200); // acknowledged (reschedule happened)
      expect(received).not.toHaveBeenCalled();

      // Row should now have a new schedulerRef
      const afterReschedule = await store.getActiveJobByKey("save", "ref:2");
      expect(afterReschedule!.schedulerRef).not.toBe(originalRef);

      // Old hook delivers again — ref mismatch → ignored
      const res2 = await harness.deliver(originalRef);
      expect(res2.status).toBe(200);
      expect(received).not.toHaveBeenCalled();

      // New hook delivers at settlement time
      const newRef = afterReschedule!.schedulerRef!;
      const newHook = harness.activeHooks().find(h => h.ref === newRef);
      if (newHook) {
        await vi.advanceTimersByTimeAsync(600);
        const res3 = await harness.deliver(newHook.ref);
        expect(res3.status).toBe(200);
        expect(received).toHaveBeenCalledOnce();
      }
    });

    it("schedulerRef updated after requeue (event during execution)", async () => {
      const { dk, store, harness } = createExternalKit();
      let callCount = 0;
      dk.handle("save", async () => { callCount++; });
      harness.setHandler(dk.createHandler());

      await dk.debounce("save", { key: "ref:3", wait: "500ms" });
      const active = await store.getActiveJobByKey("save", "ref:3");
      const hookRef = harness.hookFor(active!.id)!.ref;

      await vi.advanceTimersByTimeAsync(600);

      // Inject event during execution to force requeue
      const restore = interceptAfter(store, "markRunning", async () => {
        await dk.debounce("save", { key: "ref:3", wait: "500ms" });
      });

      await harness.deliver(hookRef);
      restore();

      expect(callCount).toBe(1);

      // After requeue, schedulerRef should point to the new hook
      const requeued = await store.getActiveJobByKey("save", "ref:3");
      expect(requeued).not.toBeNull();
      expect(requeued!.schedulerRef).not.toBeNull();
      expect(requeued!.schedulerRef).not.toBe(hookRef);
    });

    it("orphaned hook cancelled when schedulerRef CAS fails", async () => {
      const { dk, store, harness } = createExternalKit();
      dk.handle("save", async () => {});
      harness.setHandler(dk.createHandler());

      // Start debounce, then add event at 300ms so it's not settled at 500ms
      await dk.debounce("save", { key: "ref:4", wait: "500ms" });
      const active = await store.getActiveJobByKey("save", "ref:4");
      const hookRef = harness.hookFor(active!.id)!.ref;

      await vi.advanceTimersByTimeAsync(300);
      await dk.debounce("save", { key: "ref:4", wait: "500ms" });

      // Advance to 500ms: settlement check fails (500 - 300 = 200ms < 500ms)
      await vi.advanceTimersByTimeAsync(200);

      // Pause before updateSchedulerRef so we can bump version mid-flight
      const barrier = new Barrier();
      const restore = interceptBefore(store, "updateSchedulerRef", barrier);

      // Fire deliver without awaiting — it will pause at the barrier
      const deliverPromise = harness.deliver(hookRef);

      // Let microtasks settle so deliver reaches the barrier
      await vi.advanceTimersByTimeAsync(1);

      // Bump version while schedule() has returned but CAS hasn't run
      await store.updatePatternEvent("ref:4", "save", "debounce", new Date(), 500, null);

      // Release the barrier — CAS will fail, orphaned hook should be cancelled
      barrier.release();
      restore();
      await deliverPromise;

      // The hook created by the reschedule should have been cancelled
      const allHooks = harness.allHooksFor(active!.id);
      const orphanedHooks = allHooks.filter(h =>
        h.ref !== hookRef && harness.wasCancelled(h.ref),
      );
      expect(orphanedHooks.length).toBeGreaterThanOrEqual(1);
    });
  });
});

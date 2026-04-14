/**
 * Shared store contract suite.
 * Every Store implementation must satisfy these invariants.
 *
 * Run against MemoryStore and PostgresStore to prove the contract.
 * When a test fails, check the invariant first — the code is likely wrong.
 */

import { describe, it, expect, beforeEach } from "vitest";
import type { Store, Job } from "../src/types.js";
import { makeJob, makeDebounceJob, makeThrottleJob, makeStalledJob } from "./helpers/job-factory.js";
import { assertJobInvariants, assertKeyReusable } from "./helpers/invariants.js";

export function storeContractSuite(
  name: string,
  createStore: () => Promise<Store>,
  cleanup?: (store: Store) => Promise<void>,
) {
  describe(`Store contract: ${name}`, () => {
    let store: Store;

    beforeEach(async () => {
      store = await createStore();
      if (cleanup) await cleanup(store);
    });

    // --- CRUD ---

    describe("createJob + getJob", () => {
      it("creates and retrieves a job", async () => {
        const input = makeJob();
        const job = await store.createJob(input);

        expect(job.id).toBe(input.id);
        expect(job.handler).toBe("test");
        expect(job.createdAt).toBeInstanceOf(Date);

        const retrieved = await store.getJob(job.id);
        expect(retrieved).not.toBeNull();
        expect(retrieved!.id).toBe(job.id);
        assertJobInvariants(retrieved!);
      });

      it("returns null for nonexistent job", async () => {
        const result = await store.getJob("nonexistent");
        expect(result).toBeNull();
      });

      it("rejects duplicate active keys (same handler)", async () => {
        await store.createJob(makeJob({ key: "dup:1" }));
        await expect(
          store.createJob(makeJob({ key: "dup:1" })),
        ).rejects.toThrow();
      });

      it("allows same key with different handlers", async () => {
        await store.createJob(makeJob({ key: "shared:1", handler: "handler-a" }));
        const second = await store.createJob(makeJob({ key: "shared:1", handler: "handler-b" }));
        expect(second.key).toBe("shared:1");
        expect(second.handler).toBe("handler-b");
      });

      it("allows same key after terminal state", async () => {
        const job = await store.createJob(makeJob({ key: "reuse:1" }));
        await store.markRunning(job.id, job.version);
        await store.markCompleted(job.id, job.version);

        // Key should be reusable
        const second = await store.createJob(makeJob({ key: "reuse:1" }));
        expect(second.id).not.toBe(job.id);
      });
    });

    describe("getActiveJobByKey", () => {
      it("finds pending job", async () => {
        await store.createJob(makeJob({ key: "active:1" }));
        const found = await store.getActiveJobByKey("test", "active:1");
        expect(found).not.toBeNull();
        expect(found!.key).toBe("active:1");
      });

      it("finds running job", async () => {
        const job = await store.createJob(makeJob({ key: "running:1" }));
        await store.markRunning(job.id, job.version);
        const found = await store.getActiveJobByKey("test", "running:1");
        expect(found).not.toBeNull();
        expect(found!.status).toBe("running");
      });

      it("does not find completed job", async () => {
        const job = await store.createJob(makeJob({ key: "done:1" }));
        await store.markRunning(job.id, 1);
        await store.markCompleted(job.id, 1);
        await assertKeyReusable(store, "test", "done:1");
      });

      it("returns null for nonexistent key", async () => {
        const found = await store.getActiveJobByKey("test", "nope:1");
        expect(found).toBeNull();
      });
    });

    describe("deleteJob", () => {
      it("removes job by id", async () => {
        const job = await store.createJob(makeJob({ key: "del:1" }));
        await store.deleteJob(job.id);
        const found = await store.getJob(job.id);
        expect(found).toBeNull();
      });
    });

    // --- Execution lifecycle ---

    describe("markRunning", () => {
      it("transitions pending → running with version CAS", async () => {
        const job = await store.createJob(makeJob());
        const result = await store.markRunning(job.id, 1);
        expect(result).toBe(true);

        const updated = await store.getJob(job.id);
        expect(updated!.status).toBe("running");
        expect(updated!.startedAt).toBeInstanceOf(Date);
        expect(updated!.claimedVersion).toBe(1);
        assertJobInvariants(updated!);
      });

      it("exactly-once: second claim fails", async () => {
        const job = await store.createJob(makeJob());
        const first = await store.markRunning(job.id, 1);
        const second = await store.markRunning(job.id, 1);

        expect(first).toBe(true);
        expect(second).toBe(false);
      });

      it("fails with wrong version", async () => {
        const job = await store.createJob(makeJob());
        const result = await store.markRunning(job.id, 999);
        expect(result).toBe(false);
      });
    });

    describe("markCompleted", () => {
      it("transitions running → completed", async () => {
        const job = await store.createJob(makeJob());
        await store.markRunning(job.id, 1);
        const result = await store.markCompleted(job.id, 1);
        expect(result).toBe(true);

        const updated = await store.getJob(job.id);
        expect(updated!.status).toBe("completed");
        expect(updated!.completedAt).toBeInstanceOf(Date);
        assertJobInvariants(updated!);
      });

      it("fails with version mismatch (pattern event bumped version)", async () => {
        const job = await store.createJob(makeDebounceJob("ver:1", 500));
        await store.markRunning(job.id, 1);
        // Simulate a new event bumping version while running
        await store.updatePatternEvent("ver:1", "test", "debounce", new Date(), 500, null);

        const result = await store.markCompleted(job.id, 1);
        expect(result).toBe(false); // version is now 2
      });
    });

    describe("markFailed", () => {
      it("transitions running → failed with error", async () => {
        const job = await store.createJob(makeJob());
        await store.markRunning(job.id, 1);
        const result = await store.markFailed(job.id, 1, new Error("boom"));
        expect(result).toBe(true);

        const updated = await store.getJob(job.id);
        expect(updated!.status).toBe("failed");
        expect(updated!.lastError).toBe("boom");
        assertJobInvariants(updated!);
      });
    });

    describe("retryJob", () => {
      it("running → pending with incremented attempt", async () => {
        const job = await store.createJob(makeJob({ maxAttempts: 3 }));
        await store.markRunning(job.id, 1);

        const nextAt = new Date(Date.now() + 5_000);
        const result = await store.retryJob(job.id, 1, 1, nextAt, "first fail");
        expect(result).toBe(true);

        const updated = await store.getJob(job.id);
        expect(updated!.status).toBe("pending");
        expect(updated!.attempt).toBe(1);
        expect(updated!.startedAt).toBeNull();
        expect(updated!.claimedVersion).toBeNull();
        expect(updated!.scheduledFor.getTime()).toBe(nextAt.getTime());
        assertJobInvariants(updated!);
      });

      it("fails if not running", async () => {
        const job = await store.createJob(makeJob({ maxAttempts: 3 }));
        const result = await store.retryJob(job.id, 1, 1, new Date(), "error");
        expect(result).toBe(false);
      });
    });

    // --- Pattern operations ---

    describe("updatePatternEvent", () => {
      it("bumps version and lastAt on existing pending row", async () => {
        const job = await store.createJob(makeDebounceJob("pat:1", 500));
        const now = new Date();
        const updated = await store.updatePatternEvent(
          "pat:1", "test", "debounce", now, 500, null,
        );

        expect(updated).not.toBeNull();
        expect(updated!.version).toBe(2);
        expect(updated!.lastAt!.getTime()).toBe(now.getTime());
      });

      it("returns null for nonexistent key", async () => {
        const result = await store.updatePatternEvent(
          "nope:1", "test", "debounce", new Date(), 500, null,
        );
        expect(result).toBeNull();
      });

      it("rejects kind mismatch", async () => {
        await store.createJob(makeDebounceJob("kind:1", 500));
        await expect(
          store.updatePatternEvent("kind:1", "test", "throttle", new Date(), 500, null),
        ).rejects.toThrow();
      });

      it("rejects config mismatch (waitMs)", async () => {
        await store.createJob(makeDebounceJob("wait:1", 500));
        await expect(
          store.updatePatternEvent("wait:1", "test", "debounce", new Date(), 1000, null),
        ).rejects.toThrow();
      });

      it("resets firstAt only on first event during execution", async () => {
        const job = await store.createJob(makeDebounceJob("first:1", 500));
        await store.markRunning(job.id, 1);

        // Events arrive strictly after startedAt (real-world: handler is running)
        const running = await store.getJob(job.id);
        const eventTime1 = new Date(running!.startedAt!.getTime() + 50);
        const event1 = await store.updatePatternEvent(
          "first:1", "test", "debounce", eventTime1, 500, null,
        );
        // First event during execution: firstAt should reset to event time
        expect(event1!.firstAt!.getTime()).toBe(eventTime1.getTime());

        const eventTime2 = new Date(eventTime1.getTime() + 100);
        const event2 = await store.updatePatternEvent(
          "first:1", "test", "debounce", eventTime2, 500, null,
        );
        // Second event: firstAt should NOT change (anchored to first event)
        expect(event2!.firstAt!.getTime()).toBe(eventTime1.getTime());
        // But lastAt should update
        expect(event2!.lastAt!.getTime()).toBe(eventTime2.getTime());
      });
    });

    describe("rescheduleDueAt", () => {
      it("increments version and computes new scheduledFor", async () => {
        const job = await store.createJob(makeDebounceJob("resch:1", 500));
        const updated = await store.rescheduleDueAt(job.id, 1);

        expect(updated).not.toBeNull();
        expect(updated!.version).toBe(2);
        // scheduledFor should be computed from lastAt + waitMs
        expect(updated!.scheduledFor.getTime()).toBeGreaterThan(Date.now() - 1000);
      });

      it("returns null on version mismatch", async () => {
        const job = await store.createJob(makeDebounceJob("resch:2", 500));
        const result = await store.rescheduleDueAt(job.id, 999);
        expect(result).toBeNull();
      });
    });

    describe("requeueForNextWindow", () => {
      it("transitions running → pending with computed scheduledFor", async () => {
        const job = await store.createJob(makeDebounceJob("req:1", 500));
        await store.markRunning(job.id, 1);
        // Simulate event during execution
        await store.updatePatternEvent("req:1", "test", "debounce", new Date(), 500, null);

        const requeued = await store.requeueForNextWindow(job.id);
        expect(requeued).not.toBeNull();
        expect(requeued!.status).toBe("pending");
        expect(requeued!.startedAt).toBeNull();
        expect(requeued!.claimedVersion).toBeNull();
        expect(requeued!.attempt).toBe(0);
      });

      it("returns null if not running", async () => {
        const job = await store.createJob(makeDebounceJob("req:2", 500));
        const result = await store.requeueForNextWindow(job.id);
        expect(result).toBeNull();
      });
    });

    // --- Recovery ---

    describe("reclaimStalled", () => {
      it("reclaims job with expired lease", async () => {
        const job = await store.createJob(makeStalledJob({ key: "stale:1" }));
        const reclaimed = await store.reclaimStalled(job.id, 30_000);
        expect(reclaimed).not.toBeNull();
        expect(reclaimed!.status).toBe("pending");
        expect(reclaimed!.attempt).toBe(1); // attempt consumed
      });

      it("ignores unexpired lease", async () => {
        const job = await store.createJob(makeJob({
          key: "fresh:1",
          status: "running",
          claimedVersion: 1,
          startedAt: new Date(), // just started
        }));
        const reclaimed = await store.reclaimStalled(job.id, 30_000);
        expect(reclaimed).toBeNull();
      });

      it("pattern with version advance requeues fresh window", async () => {
        const now = new Date();
        const job = await store.createJob(makeDebounceJob("stalep:1", 500, {
          status: "running",
          claimedVersion: 1,
          version: 3, // version advanced past claimedVersion
          startedAt: new Date(Date.now() - 60_000),
          firstAt: new Date(Date.now() - 30_000),
          lastAt: new Date(Date.now() - 10_000),
        }));

        const reclaimed = await store.reclaimStalled(job.id, 30_000);
        expect(reclaimed).not.toBeNull();
        expect(reclaimed!.status).toBe("pending");
        expect(reclaimed!.attempt).toBe(0); // fresh window, not retry
      });
    });

    describe("reclaimStalledJobs", () => {
      it("preserves the default lease floor for rows whose handler isn't registered", async () => {
        // Rolling-deploy / handler-rename scenario: a running row's
        // handler name isn't in the current registration map. The
        // reclaim cutoff must floor at DEFAULT_TIMEOUT_MS so the
        // row isn't reclaimed while another process is still
        // legitimately executing it under the old handler's lease.
        const legacy = await store.createJob(makeJob({
          handler: "legacy-handler",
          key: "legacy:1",
          status: "running",
          claimedVersion: 1,
          startedAt: new Date(Date.now() - 10_000), // 10s old
        }));

        const timeouts = new Map([["current-handler", 500]]);
        const reclaimed = await store.reclaimStalledJobs(timeouts);

        expect(reclaimed).toHaveLength(0);
        const after = await store.getJob(legacy.id);
        expect(after?.status).toBe("running");
      });

      it("reclaims rows past the default lease floor", async () => {
        const legacy = await store.createJob(makeJob({
          handler: "legacy-handler",
          key: "legacy:2",
          status: "running",
          claimedVersion: 1,
          startedAt: new Date(Date.now() - 40_000), // 40s old, past DEFAULT + grace
        }));

        const timeouts = new Map([["current-handler", 500]]);
        const reclaimed = await store.reclaimStalledJobs(timeouts);

        expect(reclaimed).toHaveLength(1);
        expect(reclaimed[0].id).toBe(legacy.id);
        expect(reclaimed[0].status).toBe("pending");
      });

      it("bumps attempt for a stalled pattern row with no version advance", async () => {
        // Debounce row that's been running past the lease but no new
        // events arrived during execution (version == claimedVersion).
        // Should follow the normal reclaim path — bump attempt, back
        // to pending — not the fresh-window requeue.
        const stalled = await store.createJob(makeDebounceJob("pattern-reclaim:1", 500, {
          status: "running",
          version: 2,
          claimedVersion: 2, // no version advance during execution
          attempt: 0,
          startedAt: new Date(Date.now() - 40_000), // past DEFAULT + grace
          firstAt: new Date(Date.now() - 45_000),
          lastAt: new Date(Date.now() - 45_000),
        }));

        const reclaimed = await store.reclaimStalledJobs(new Map());
        expect(reclaimed).toHaveLength(1);
        expect(reclaimed[0].id).toBe(stalled.id);
        expect(reclaimed[0].status).toBe("pending");
        expect(reclaimed[0].attempt).toBe(1); // bumped, not reset
      });
    });

    // --- getDueJobs ---

    describe("getDueJobs", () => {
      it("returns pending jobs past scheduledFor", async () => {
        await store.createJob(makeJob({
          key: "due:1",
          scheduledFor: new Date(Date.now() - 1000),
        }));
        await store.createJob(makeJob({
          key: "future:1",
          scheduledFor: new Date(Date.now() + 60_000),
        }));

        const due = await store.getDueJobs(10);
        expect(due.length).toBe(1);
        expect(due[0].key).toBe("due:1");
      });

      it("orders by scheduledFor ascending", async () => {
        await store.createJob(makeJob({
          key: "later:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.createJob(makeJob({
          key: "earlier:1",
          scheduledFor: new Date(Date.now() - 5_000),
        }));

        const due = await store.getDueJobs(10);
        expect(due[0].key).toBe("earlier:1");
        expect(due[1].key).toBe("later:1");
      });

      it("respects limit", async () => {
        for (let i = 0; i < 5; i++) {
          await store.createJob(makeJob({
            key: `lim:${i}`,
            scheduledFor: new Date(Date.now() - 1_000),
          }));
        }

        const due = await store.getDueJobs(3);
        expect(due.length).toBe(3);
      });

      it("excludes running and terminal jobs", async () => {
        const running = await store.createJob(makeJob({
          key: "run:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.markRunning(running.id, 1);

        const due = await store.getDueJobs(10);
        expect(due.length).toBe(0);
      });
    });

    // --- replaceJob ---

    describe("replaceJob", () => {
      it("increments version and updates scheduledFor", async () => {
        const job = await store.createJob(makeJob({ key: "rep:1" }));
        const newTime = new Date(Date.now() + 60_000);
        const replaced = await store.replaceJob(job.id, newTime, 1);

        expect(replaced).not.toBeNull();
        expect(replaced!.version).toBe(2);
        expect(replaced!.scheduledFor.getTime()).toBe(newTime.getTime());
      });

      it("returns null if not pending", async () => {
        const job = await store.createJob(makeJob({ key: "rep:2" }));
        await store.markRunning(job.id, 1);
        const replaced = await store.replaceJob(job.id, new Date(), 1);
        expect(replaced).toBeNull();
      });

      it("clears defer counters", async () => {
        const job = await store.createJob(makeJob({ key: "rep:defer" }));
        const future = new Date(Date.now() + 60_000);
        await store.deferJob(job.id, 1, future, "missing-deferred", "missing-terminal", 60_000);

        const replaced = await store.replaceJob(job.id, future, 1);
        expect(replaced).not.toBeNull();
        expect(replaced!.deferAttempts).toBe(0);
        expect(replaced!.deferredSince).toBeNull();
      });
    });

    // --- deferJob ---

    describe("deferJob", () => {
      it("defers a pending row with new scheduledFor and error", async () => {
        const job = await store.createJob(makeJob({ key: "defer:1" }));
        const nextAt = new Date(Date.now() + 5_000);
        const deferred = await store.deferJob(job.id, 1, nextAt, "missing-deferred", "missing-terminal", 60_000);

        expect(deferred).not.toBeNull();
        expect(deferred!.status).toBe("pending");
        expect(deferred!.version).toBe(2);
        expect(deferred!.deferAttempts).toBe(1);
        expect(deferred!.deferredSince).not.toBeNull();
        expect(deferred!.scheduledFor.getTime()).toBe(nextAt.getTime());
        expect(deferred!.lastError).toBe("missing-deferred");
        assertJobInvariants(deferred!);
      });

      it("preserves deferredSince across subsequent defers", async () => {
        const job = await store.createJob(makeJob({ key: "defer:2" }));
        const first = await store.deferJob(job.id, 1, new Date(Date.now() + 5_000), "m1-deferred", "m1-terminal", 60_000);
        const originalSince = first!.deferredSince!.getTime();

        const second = await store.deferJob(job.id, first!.version, new Date(Date.now() + 10_000), "m2-deferred", "m2-terminal", 60_000);

        expect(second).not.toBeNull();
        expect(second!.deferAttempts).toBe(2);
        expect(second!.deferredSince!.getTime()).toBe(originalSince);
      });

      it("flips to failed when horizon is exceeded", async () => {
        const job = await store.createJob(makeJob({ key: "defer:3" }));
        // First defer with a 1ms horizon — the next defer call past
        // now > deferredSince + 1ms will flip.
        const first = await store.deferJob(job.id, 1, new Date(Date.now() + 1_000), "m1-deferred", "m1-terminal", 60_000);
        expect(first!.status).toBe("pending");

        // Wait long enough that the stored deferredSince + 1ms is in the past.
        await new Promise((resolve) => setTimeout(resolve, 30));

        const flipped = await store.deferJob(job.id, first!.version, new Date(Date.now() + 5_000), "horizon-deferred", "horizon-terminal", 1);
        expect(flipped).not.toBeNull();
        expect(flipped!.status).toBe("failed");
        expect(flipped!.completedAt).not.toBeNull();
        expect(flipped!.lastError).toBe("horizon-terminal");
        assertJobInvariants(flipped!);
      });

      it("releases the (handler, key) slot on horizon flip", async () => {
        const job = await store.createJob(makeJob({ key: "defer:4" }));
        await store.deferJob(job.id, 1, new Date(Date.now() + 1_000), "m1-deferred", "m1-terminal", 60_000);
        await new Promise((resolve) => setTimeout(resolve, 30));
        await store.deferJob(job.id, 2, new Date(Date.now() + 1_000), "horizon-deferred", "horizon-terminal", 1);

        await assertKeyReusable(store, "test", "defer:4");
      });

      it("returns null when CAS loses on version", async () => {
        const job = await store.createJob(makeJob({ key: "defer:5" }));
        const bad = await store.deferJob(job.id, 999, new Date(), "missing-deferred", "missing-terminal", 60_000);
        expect(bad).toBeNull();
      });

      it("returns null when the row is not pending", async () => {
        const job = await store.createJob(makeJob({ key: "defer:6" }));
        await store.markRunning(job.id, 1);
        const attempted = await store.deferJob(job.id, 1, new Date(), "missing-deferred", "missing-terminal", 60_000);
        expect(attempted).toBeNull();
      });
    });

    describe("markRunning clears defer counters", () => {
      it("zeroes deferAttempts and deferredSince on successful claim", async () => {
        const job = await store.createJob(makeJob({ key: "defer-claim:1" }));
        await store.deferJob(job.id, 1, new Date(Date.now() + 5_000), "missing-deferred", "missing-terminal", 60_000);

        const current = await store.getJob(job.id);
        const claimed = await store.markRunning(job.id, current!.version);
        expect(claimed).toBe(true);

        const afterClaim = await store.getJob(job.id);
        expect(afterClaim!.deferAttempts).toBe(0);
        expect(afterClaim!.deferredSince).toBeNull();
        assertJobInvariants(afterClaim!);
      });
    });

    describe("retryConfig roundtrip", () => {
      it("preserves the snapshot across createJob/getJob", async () => {
        const job = await store.createJob(
          makeJob({
            key: "retry-roundtrip:1",
            maxAttempts: 5,
            retryConfig: {
              attempts: 5,
              backoff: "exponential",
              initialDelayMs: 30_000,
              maxDelayMs: 600_000,
              jitter: true,
            },
          }),
        );
        const read = await store.getJob(job.id);
        expect(read!.retryConfig).toEqual({
          attempts: 5,
          backoff: "exponential",
          initialDelayMs: 30_000,
          maxDelayMs: 600_000,
          jitter: true,
        });
      });

      it("handles Infinity maxDelayMs roundtrip", async () => {
        const job = await store.createJob(
          makeJob({
            key: "retry-roundtrip:inf",
            maxAttempts: 3,
            retryConfig: {
              attempts: 3,
              backoff: "fixed",
              initialDelayMs: 1_000,
              maxDelayMs: Infinity,
              jitter: false,
            },
          }),
        );
        const read = await store.getJob(job.id);
        expect(read!.retryConfig?.maxDelayMs).toBe(Infinity);
      });

      it("null when no retry is configured", async () => {
        const job = await store.createJob(makeJob({ key: "retry-roundtrip:null" }));
        const read = await store.getJob(job.id);
        expect(read!.retryConfig).toBeNull();
      });
    });

    describe("cancelJob clears defer counters", () => {
      it("resets deferAttempts and deferredSince on cancel", async () => {
        const job = await store.createJob(makeJob({ key: "defer-cancel:1" }));
        await store.deferJob(job.id, 1, new Date(Date.now() + 5_000), "missing-deferred", "missing-terminal", 60_000);

        const cancelled = await store.cancelJob(job.id);
        expect(cancelled).toBe(true);

        const after = await store.getJob(job.id);
        expect(after!.status).toBe("cancelled");
        expect(after!.deferAttempts).toBe(0);
        expect(after!.deferredSince).toBeNull();
        assertJobInvariants(after!);
      });
    });

    // --- updateSchedulerRef ---

    describe("updateSchedulerRef", () => {
      it("updates ref when version matches", async () => {
        const job = await store.createJob(makeJob({ key: "ref:1" }));
        const result = await store.updateSchedulerRef(job.id, 1, "hook_abc");
        expect(result).toBe(true);

        const updated = await store.getJob(job.id);
        expect(updated!.schedulerRef).toBe("hook_abc");
      });

      it("rejects when version has advanced", async () => {
        const job = await store.createJob(makeDebounceJob("ref:2", 500));
        // Bump version via pattern event
        await store.updatePatternEvent("ref:2", "test", "debounce", new Date(), 500, null);

        // Try to write ref at old version — should fail
        const result = await store.updateSchedulerRef(job.id, 1, "hook_stale");
        expect(result).toBe(false);

        const current = await store.getJob(job.id);
        expect(current!.schedulerRef).toBeNull();
      });
    });
  });
}

/**
 * Shared store contract suite.
 * Every Store implementation must satisfy these invariants.
 *
 * Run against MemoryStore and PostgresStore to prove the contract.
 * When a test fails, check the invariant first — the code is likely wrong.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type { DelayKitStats, Store, Job } from "../src/types.js";
import { LAST_ERROR_TRUNCATION_MARKER, MAX_LAST_ERROR_CHARS } from "../src/types.js";
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

    afterEach(async () => {
      await store.close();
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
        const result = await store.markFailed(job.id, 1, new Error("boom"), "handler_error");
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

    describe("rescheduleJob", () => {
      it("running → pending with attempt reset and fresh scheduledFor", async () => {
        const job = await store.createJob(makeJob({ key: "rs:basic", maxAttempts: 3 }));
        await store.markRunning(job.id, 1);
        const nextAt = new Date(Date.now() + 60_000);
        const updated = await store.rescheduleJob(job.id, 1, nextAt);

        expect(updated).not.toBeNull();
        expect(updated!.status).toBe("pending");
        expect(updated!.version).toBe(2);
        expect(updated!.attempt).toBe(0);
        expect(updated!.scheduledFor.getTime()).toBe(nextAt.getTime());
        expect(updated!.startedAt).toBeNull();
        expect(updated!.completedAt).toBeNull();
        expect(updated!.claimedVersion).toBeNull();
        expect(updated!.lastError).toBeNull();
        expect(updated!.failureReason).toBeNull();
        expect(updated!.deferAttempts).toBe(0);
        expect(updated!.deferredSince).toBeNull();
        expect(updated!.schedulerRef).toBeNull();
        assertJobInvariants(updated!);
      });

      it("returns null on wrong version (CAS loss)", async () => {
        const job = await store.createJob(makeJob({ key: "rs:cas" }));
        await store.markRunning(job.id, 1);

        const result = await store.rescheduleJob(job.id, 999, new Date(Date.now() + 30_000));
        expect(result).toBeNull();
        // Row stays running, untouched.
        const after = await store.getJob(job.id);
        expect(after!.status).toBe("running");
      });

      it("returns null on non-running status", async () => {
        const pendingJob = await store.createJob(makeJob({ key: "rs:pending" }));
        const pendingResult = await store.rescheduleJob(pendingJob.id, 1, new Date(Date.now() + 30_000));
        expect(pendingResult).toBeNull();

        const completedJob = await store.createJob(makeJob({ key: "rs:completed" }));
        await store.markRunning(completedJob.id, 1);
        await store.markCompleted(completedJob.id, 1);
        const completedResult = await store.rescheduleJob(completedJob.id, 1, new Date(Date.now() + 30_000));
        expect(completedResult).toBeNull();
      });

      it("bumps version so stale operations against the old version lose CAS", async () => {
        const job = await store.createJob(makeJob({ key: "rs:stale", maxAttempts: 3 }));
        await store.markRunning(job.id, 1);

        const updated = await store.rescheduleJob(job.id, 1, new Date(Date.now() + 30_000));
        expect(updated!.version).toBe(2);

        // A stale claim attempt at the old version=1 must fail. The
        // row is now legitimately pending again (post-reschedule), so
        // without the version bump this CAS would succeed and let an
        // in-flight wake from the prior run cycle re-claim a row that
        // shouldn't be its responsibility anymore.
        const staleClaim = await store.markRunning(job.id, 1);
        expect(staleClaim).toBe(false);

        // The fresh claim at the bumped version succeeds.
        const freshClaim = await store.markRunning(job.id, 2);
        expect(freshClaim).toBe(true);
      });

      it("clears lastError from a prior failed attempt window", async () => {
        // Plant a row that failed once, was retried back to pending,
        // then claimed running again. retryJob writes a non-null
        // last_error; rescheduleJob must clear it.
        const job = await store.createJob(makeJob({ key: "rs:lasterror", maxAttempts: 3 }));
        await store.markRunning(job.id, 1);
        await store.retryJob(job.id, 1, 1, new Date(Date.now() - 100), "transient");
        await store.markRunning(job.id, 1);

        const next = new Date(Date.now() + 30_000);
        const updated = await store.rescheduleJob(job.id, 1, next);
        expect(updated!.lastError).toBeNull();
        expect(updated!.failureReason).toBeNull();
        expect(updated!.attempt).toBe(0);
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
        // firstAt is set to 1s in the past so first_at < started_at holds
        // regardless of millisecond-level clock drift between JS and Postgres.
        const past = new Date(Date.now() - 1_000);
        const job = await store.createJob(makeDebounceJob("first:1", 500, {
          firstAt: past,
          lastAt: past,
        }));
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

    // --- claimDueJobs ---

    describe("claimDueJobs", () => {
      const handlerNames = ["test"];

      it("returns settled due rows in toRun (flipped to running)", async () => {
        await store.createJob(makeJob({
          key: "claim:1",
          scheduledFor: new Date(Date.now() - 1000),
        }));
        await store.createJob(makeJob({
          key: "future:1",
          scheduledFor: new Date(Date.now() + 60_000),
        }));

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(rescheduled.length).toBe(0);
        expect(toRun.length).toBe(1);
        expect(toRun[0].key).toBe("claim:1");
        expect(toRun[0].status).toBe("running");
        expect(toRun[0].claimedVersion).toBe(toRun[0].version);
        expect(toRun[0].startedAt).not.toBeNull();
      });

      it("orders toRun by scheduledFor ASC, id ASC under ties", async () => {
        const tiedAt = new Date(Date.now() - 5_000);
        await store.createJob(makeJob({
          id: "00000000-0000-0000-0000-000000000002",
          key: "tied:b",
          scheduledFor: tiedAt,
        }));
        await store.createJob(makeJob({
          id: "00000000-0000-0000-0000-000000000001",
          key: "tied:a",
          scheduledFor: tiedAt,
        }));
        await store.createJob(makeJob({
          key: "later",
          scheduledFor: new Date(Date.now() - 1_000),
        }));

        const { toRun } = await store.claimDueJobs(10, handlerNames);
        expect(toRun.map((j) => j.key)).toEqual(["tied:a", "tied:b", "later"]);
      });

      it("respects budget across both buckets combined", async () => {
        for (let i = 0; i < 5; i++) {
          await store.createJob(makeJob({
            key: `bud:${i}`,
            scheduledFor: new Date(Date.now() - 1_000),
          }));
        }

        const { toRun, rescheduled } = await store.claimDueJobs(3, handlerNames);
        expect(toRun.length + rescheduled.length).toBe(3);
      });

      it("returns empty batch when none are due", async () => {
        await store.createJob(makeJob({
          key: "future-only",
          scheduledFor: new Date(Date.now() + 60_000),
        }));
        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(toRun.length).toBe(0);
        expect(rescheduled.length).toBe(0);
      });

      it("excludes running and terminal jobs", async () => {
        const running = await store.createJob(makeJob({
          key: "run:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.markRunning(running.id, 1);

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(toRun.length).toBe(0);
        expect(rescheduled.length).toBe(0);
      });

      it("skips rows whose handler is not in handlerNames", async () => {
        await store.createJob(makeJob({
          handler: "ghost",
          key: "ghost:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.createJob(makeJob({
          handler: "test",
          key: "ok:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));

        const { toRun } = await store.claimDueJobs(10, ["test"]);
        expect(toRun.length).toBe(1);
        expect(toRun[0].key).toBe("ok:1");

        // Ghost row left available for another replica with that handler.
        const { toRun: ghostRun } = await store.claimDueJobs(10, ["ghost"]);
        expect(ghostRun.length).toBe(1);
        expect(ghostRun[0].key).toBe("ghost:1");
      });

      it("returns empty batch when handlerNames is empty", async () => {
        await store.createJob(makeJob({
          key: "unreachable:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        const { toRun, rescheduled } = await store.claimDueJobs(10, []);
        expect(toRun.length).toBe(0);
        expect(rescheduled.length).toBe(0);
      });

      it("routes un-settled debounce rows to rescheduled (not claimed)", async () => {
        const now = Date.now();
        await store.createJob(makeDebounceJob("unsettled:1", 5_000, {
          firstAt: new Date(now - 10_000),
          lastAt: new Date(now - 1_000), // 1s since last event, wait is 5s → un-settled
          scheduledFor: new Date(now - 100),
        }));

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(toRun.length).toBe(0);
        expect(rescheduled.length).toBe(1);
        expect(rescheduled[0].status).toBe("pending");
        expect(rescheduled[0].startedAt).toBeNull();
        expect(rescheduled[0].scheduledFor.getTime()).toBe((now - 1_000) + 5_000);
        expect(rescheduled[0].version).toBe(2);
      });

      it("routes settled debounce rows to toRun", async () => {
        const now = Date.now();
        await store.createJob(makeDebounceJob("settled:1", 500, {
          firstAt: new Date(now - 10_000),
          lastAt: new Date(now - 2_000),
          scheduledFor: new Date(now - 100),
        }));

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(rescheduled.length).toBe(0);
        expect(toRun.length).toBe(1);
        expect(toRun[0].status).toBe("running");
      });

      it("routes throttle rows to toRun regardless of settlement", async () => {
        const now = Date.now();
        await store.createJob(makeThrottleJob("throttle:1", 5_000, {
          firstAt: new Date(now - 100),
          lastAt: new Date(now - 50),
          scheduledFor: new Date(now - 10),
        }));

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(rescheduled.length).toBe(0);
        expect(toRun.length).toBe(1);
      });

      it("routes un-settled debounce rows past maxWait to toRun", async () => {
        const now = Date.now();
        await store.createJob(makeDebounceJob("maxwait:1", 5_000, {
          firstAt: new Date(now - 10_000),
          lastAt: new Date(now - 100),
          maxWaitMs: 2_000,
          scheduledFor: new Date(now - 50),
        }));

        const { toRun, rescheduled } = await store.claimDueJobs(10, handlerNames);
        expect(rescheduled.length).toBe(0);
        expect(toRun.length).toBe(1);
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


    describe("defer counter reset points", () => {
      it("markRunning (wake path) clears deferAttempts and deferredSince", async () => {
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

      it("markCompleted clears deferAttempts and deferredSince", async () => {
        const job = await store.createJob(makeJob({
          key: "defer-complete:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.deferJob(job.id, 1, new Date(Date.now() - 500), "deferred", "terminal", 60_000);

        const { toRun } = await store.claimDueJobs(10, ["test"]);
        expect(toRun[0].status).toBe("running");

        const ok = await store.markCompleted(toRun[0].id, toRun[0].version);
        expect(ok).toBe(true);

        const after = await store.getJob(toRun[0].id);
        expect(after!.deferAttempts).toBe(0);
        expect(after!.deferredSince).toBeNull();
      });

      it("markFailed clears deferAttempts and deferredSince", async () => {
        const job = await store.createJob(makeJob({
          key: "defer-failed:1",
          scheduledFor: new Date(Date.now() - 1_000),
        }));
        await store.deferJob(job.id, 1, new Date(Date.now() - 500), "deferred", "terminal", 60_000);

        const { toRun } = await store.claimDueJobs(10, ["test"]);

        const ok = await store.markFailed(toRun[0].id, toRun[0].version, new Error("boom"), "handler_error");
        expect(ok).toBe(true);

        const after = await store.getJob(toRun[0].id);
        expect(after!.deferAttempts).toBe(0);
        expect(after!.deferredSince).toBeNull();
      });

      it("retryJob clears deferAttempts and deferredSince", async () => {
        const job = await store.createJob(makeJob({
          key: "defer-retry:1",
          scheduledFor: new Date(Date.now() - 1_000),
          maxAttempts: 3,
        }));
        await store.deferJob(job.id, 1, new Date(Date.now() - 500), "deferred", "terminal", 60_000);

        const { toRun } = await store.claimDueJobs(10, ["test"]);

        const ok = await store.retryJob(
          toRun[0].id, toRun[0].version,
          1, new Date(Date.now() + 5_000), "handler fail",
        );
        expect(ok).toBe(true);

        const after = await store.getJob(toRun[0].id);
        expect(after!.deferAttempts).toBe(0);
        expect(after!.deferredSince).toBeNull();
      });

      it("reclaimStalled clears deferAttempts and deferredSince (wake-path crash recovery)", async () => {
        const job = await store.createJob(makeStalledJob({
          key: "defer-reclaim:1",
          scheduledFor: new Date(Date.now() - 60_000),
          deferAttempts: 3,
          deferredSince: new Date(Date.now() - 23 * 60 * 60 * 1000),
        }));

        const reclaimed = await store.reclaimStalled(job.id, 1_000);
        expect(reclaimed).not.toBeNull();
        expect(reclaimed!.status).toBe("pending");
        expect(reclaimed!.deferAttempts).toBe(0);
        expect(reclaimed!.deferredSince).toBeNull();
      });

      it("reclaimStalledJobs clears deferAttempts and deferredSince (poll-path crash recovery)", async () => {
        await store.createJob(makeStalledJob({
          key: "defer-sweep:1",
          scheduledFor: new Date(Date.now() - 60_000),
          deferAttempts: 2,
          deferredSince: new Date(Date.now() - 20 * 60 * 60 * 1000),
        }));

        const reclaimed = await store.reclaimStalledJobs(new Map([["test", 1_000]]));
        expect(reclaimed.length).toBe(1);
        expect(reclaimed[0].status).toBe("pending");
        expect(reclaimed[0].deferAttempts).toBe(0);
        expect(reclaimed[0].deferredSince).toBeNull();
      });

      it("requeueForNextWindow clears deferAttempts and deferredSince", async () => {
        const now = Date.now();
        const job = await store.createJob(makeDebounceJob("defer-requeue:1", 5_000, {
          firstAt: new Date(now - 10_000),
          lastAt: new Date(now - 6_000),
          scheduledFor: new Date(now - 100),
        }));
        await store.deferJob(job.id, 1, new Date(now - 50), "deferred", "terminal", 60_000);

        const { toRun } = await store.claimDueJobs(10, ["test"]);
        expect(toRun[0].status).toBe("running");

        const requeued = await store.requeueForNextWindow(toRun[0].id);
        expect(requeued).not.toBeNull();
        expect(requeued!.deferAttempts).toBe(0);
        expect(requeued!.deferredSince).toBeNull();
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

    describe("pruneTerminal", () => {
      const terminalStatuses = ["completed", "failed", "cancelled"] as const;

      function makeTerminalJob(
        key: string,
        status: typeof terminalStatuses[number],
        completedAt: Date,
      ): Omit<Job, "createdAt"> {
        return makeJob({ key, status, completedAt });
      }

      it("deletes terminal rows with completedAt < olderThan and returns the count", async () => {
        const old = new Date(Date.now() - 60_000);
        for (const status of terminalStatuses) {
          await store.createJob(makeTerminalJob(`prune:old:${status}`, status, old));
        }
        const cutoff = new Date(Date.now() - 30_000);
        const deleted = await store.pruneTerminal(cutoff);
        expect(deleted).toBe(terminalStatuses.length);
        for (const status of terminalStatuses) {
          const active = await store.getActiveJobByKey("test", `prune:old:${status}`);
          expect(active).toBeNull();
        }
      });

      it("leaves pending and running rows untouched regardless of cutoff", async () => {
        const pending = await store.createJob(makeJob({ key: "prune:pending" }));
        const running = await store.createJob(makeJob({ key: "prune:running" }));
        await store.markRunning(running.id, 1);

        const cutoff = new Date(Date.now() + 60_000);
        const deleted = await store.pruneTerminal(cutoff);
        expect(deleted).toBe(0);

        expect(await store.getJob(pending.id)).not.toBeNull();
        expect(await store.getJob(running.id)).not.toBeNull();
      });

      it("leaves terminal rows newer than the cutoff untouched", async () => {
        const fresh = await store.createJob(
          makeTerminalJob("prune:fresh", "completed", new Date(Date.now() - 1_000)),
        );
        const cutoff = new Date(Date.now() - 30_000);
        const deleted = await store.pruneTerminal(cutoff);
        expect(deleted).toBe(0);
        expect(await store.getJob(fresh.id)).not.toBeNull();
      });

      it("with a limit, deletes at most that many rows (oldest first)", async () => {
        const base = Date.now() - 60_000;
        const created: Job[] = [];
        for (let i = 0; i < 5; i++) {
          const job = await store.createJob(
            makeTerminalJob(`prune:batch:${i}`, "completed", new Date(base + i * 1_000)),
          );
          created.push(job);
        }
        const cutoff = new Date();
        const deleted = await store.pruneTerminal(cutoff, 2);
        expect(deleted).toBe(2);

        // The two oldest rows are gone; the other three remain.
        expect(await store.getJob(created[0]!.id)).toBeNull();
        expect(await store.getJob(created[1]!.id)).toBeNull();
        for (let i = 2; i < 5; i++) {
          expect(await store.getJob(created[i]!.id)).not.toBeNull();
        }
      });

      it("throws when limit is not a positive integer", async () => {
        const cutoff = new Date();
        await expect(store.pruneTerminal(cutoff, 0)).rejects.toThrow(/positive integer/);
        await expect(store.pruneTerminal(cutoff, -1)).rejects.toThrow(/positive integer/);
        await expect(store.pruneTerminal(cutoff, 1.5)).rejects.toThrow(/positive integer/);
        await expect(store.pruneTerminal(cutoff, NaN)).rejects.toThrow(/positive integer/);
        await expect(store.pruneTerminal(cutoff, Infinity)).rejects.toThrow(/positive integer/);
      });

      it("frees the (handler, key) slot after pruning", async () => {
        const old = new Date(Date.now() - 60_000);
        await store.createJob(makeTerminalJob("prune:slot", "completed", old));
        await store.pruneTerminal(new Date(Date.now() - 30_000));

        // New job with same (handler, key) should insert without conflict.
        const reused = await store.createJob(makeJob({ key: "prune:slot" }));
        expect(reused.id).toBeTruthy();
        assertJobInvariants(reused);
      });
    });

    describe("stats", () => {
      it("returns zero counts on an empty store", async () => {
        const s = await store.stats();
        expect(s.pending).toBe(0);
        expect(s.duePending).toBe(0);
        expect(s.running).toBe(0);
        expect(s.deferred).toBe(0);
        expect(s.failed24h).toBe(0);
        expect(s.oldestDuePending).toBeNull();
        expect(s.oldestRunning).toBeNull();
        expect(s.byHandler).toEqual([]);
      });

      it("counts pending, duePending, running, deferred across two handlers", async () => {
        // handler-a: 1 future pending, 1 due pending (deferred), 1 running
        await store.createJob(makeJob({ key: "s:a:future", handler: "handler-a", scheduledFor: new Date(Date.now() + 60_000) }));
        const deferredJob = await store.createJob(makeJob({ key: "s:a:due", handler: "handler-a", scheduledFor: new Date(Date.now() - 1_000) }));
        await store.deferJob(deferredJob.id, 1, new Date(Date.now() + 5_000), "missing", "terminal", 24 * 60 * 60 * 1_000);
        const { toRun } = await store.claimDueJobs(10, ["handler-a", "handler-b"]);
        // handler-a deferred job has scheduledFor in future now — not due
        // create another due pending for handler-a
        await store.createJob(makeJob({ key: "s:a:due2", handler: "handler-a", scheduledFor: new Date(Date.now() - 500) }));
        // handler-b: 1 running
        const runningB = await store.createJob(makeJob({ key: "s:b:run", handler: "handler-b", scheduledFor: new Date(Date.now() - 1_000) }));
        await store.markRunning(runningB.id, 1);

        const s = await store.stats();

        expect(s.running).toBe(1);
        expect(s.deferred).toBe(1);

        const a = s.byHandler.find(h => h.handler === "handler-a");
        const b = s.byHandler.find(h => h.handler === "handler-b");
        expect(a).toBeDefined();
        expect(b).toBeDefined();
        expect(a!.deferred).toBe(1);
        expect(b!.running).toBe(1);
      });

      it("oldestDuePending is the earliest due-pending row by scheduledFor then id", async () => {
        const older = new Date(Date.now() - 2_000);
        const newer = new Date(Date.now() - 500);
        const j1 = await store.createJob(makeJob({ key: "s:oldest:1", scheduledFor: newer }));
        const j2 = await store.createJob(makeJob({ key: "s:oldest:2", scheduledFor: older }));

        const s = await store.stats();
        expect(s.oldestDuePending).not.toBeNull();
        expect(s.oldestDuePending!.id).toBe(j2.id);
        expect(s.oldestDuePending!.scheduledFor).toEqual(older);

        // future-scheduled row is not included
        await store.createJob(makeJob({ key: "s:oldest:future", scheduledFor: new Date(Date.now() + 60_000) }));
        const s2 = await store.stats();
        expect(s2.oldestDuePending!.id).toBe(j2.id);
      });

      it("oldestRunning is the earliest running row by startedAt then id", async () => {
        const j1 = await store.createJob(makeJob({ key: "s:run:1", scheduledFor: new Date(Date.now() - 1_000) }));
        const j2 = await store.createJob(makeJob({ key: "s:run:2", scheduledFor: new Date(Date.now() - 2_000) }));
        await store.markRunning(j1.id, 1);
        await store.markRunning(j2.id, 1);

        const s = await store.stats();
        expect(s.oldestRunning).not.toBeNull();
        // j2 started after j1 (markRunning sets startedAt = now()), but they're both "now" — just assert it's one of them
        expect([j1.id, j2.id]).toContain(s.oldestRunning!.id);
      });

      it("failed24h counts recent failures and excludes old ones and non-failed terminal rows", async () => {
        const recentFailed = makeJob({ key: "s:fail:recent", status: "failed", completedAt: new Date(Date.now() - 60_000) });
        const oldFailed = makeJob({ key: "s:fail:old", status: "failed", completedAt: new Date(Date.now() - 25 * 60 * 60 * 1_000) });
        const completed = makeJob({ key: "s:fail:completed", status: "completed", completedAt: new Date(Date.now() - 60_000) });
        await store.createJob(recentFailed);
        await store.createJob(oldFailed);
        await store.createJob(completed);

        const s = await store.stats();
        expect(s.failed24h).toBe(1);
      });

      it("byHandler excludes handlers with zero counts in all buckets", async () => {
        // Only create a completed job (not tracked by stats)
        await store.createJob(makeJob({ key: "s:zero:1", handler: "zero-handler", status: "completed", completedAt: new Date(Date.now() - 25 * 60 * 60 * 1_000) }));

        const s = await store.stats();
        expect(s.byHandler.find(h => h.handler === "zero-handler")).toBeUndefined();
      });

      it("byHandler includes a handler that only has failed24h > 0", async () => {
        await store.createJob(makeJob({ key: "s:failonly:1", handler: "fail-only-handler", status: "failed", completedAt: new Date(Date.now() - 60_000) }));

        const s = await store.stats();
        const h = s.byHandler.find(b => b.handler === "fail-only-handler");
        expect(h).toBeDefined();
        expect(h!.failed24h).toBe(1);
        expect(h!.pending).toBe(0);
        expect(h!.running).toBe(0);
      });

      it("excludes unsettled debounce rows from duePending and oldestDuePending", async () => {
        const now = Date.now();
        // Unsettled debounce: scheduled_for is in the past but wait window hasn't elapsed since lastAt
        await store.createJob(makeDebounceJob("s:debounce:unsettled", 5_000, {
          scheduledFor: new Date(now - 1_000),  // in the past
          lastAt: new Date(now - 500),           // only 500ms ago — not settled (waitMs=5000)
        }));
        // Settled debounce: lastAt is old enough
        const settled = await store.createJob(makeDebounceJob("s:debounce:settled", 1_000, {
          scheduledFor: new Date(now - 2_000),
          lastAt: new Date(now - 1_500),        // 1500ms ago > waitMs=1000
        }));

        const s = await store.stats();
        expect(s.duePending).toBe(1);
        expect(s.oldestDuePending).not.toBeNull();
        expect(s.oldestDuePending!.id).toBe(settled.id);
      });

      it("byHandler is sorted alphabetically by handler name", async () => {
        await store.createJob(makeJob({ key: "s:sort:z", handler: "zzz-handler" }));
        await store.createJob(makeJob({ key: "s:sort:a", handler: "aaa-handler" }));
        await store.createJob(makeJob({ key: "s:sort:m", handler: "mmm-handler" }));

        const s = await store.stats();
        const names = s.byHandler.map(h => h.handler);
        const relevant = names.filter(n => ["aaa-handler", "mmm-handler", "zzz-handler"].includes(n));
        expect(relevant).toEqual(["aaa-handler", "mmm-handler", "zzz-handler"]);
      });
    });

    describe("lastError truncation", () => {
      const huge = "x".repeat(MAX_LAST_ERROR_CHARS * 2);

      it("markFailed caps lastError at MAX_LAST_ERROR_CHARS", async () => {
        const job = await store.createJob(makeJob({ key: "trunc:fail" }));
        await store.markRunning(job.id, 1);
        await store.markFailed(job.id, 1, new Error(huge), "handler_error");

        const read = await store.getJob(job.id);
        expect(read!.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
        expect(read!.lastError).toContain(LAST_ERROR_TRUNCATION_MARKER);
      });

      it("retryJob caps lastError at MAX_LAST_ERROR_CHARS", async () => {
        const job = await store.createJob(makeJob({ key: "trunc:retry", maxAttempts: 3 }));
        await store.markRunning(job.id, 1);
        await store.retryJob(job.id, 1, 1, new Date(Date.now() + 1_000), huge);

        const read = await store.getJob(job.id);
        expect(read!.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
        expect(read!.lastError).toContain(LAST_ERROR_TRUNCATION_MARKER);
      });

      it("deferJob caps the deferred branch", async () => {
        const job = await store.createJob(makeJob({ key: "trunc:defer" }));
        await store.deferJob(job.id, 1, new Date(Date.now() + 1_000), huge, "terminal", 60_000);

        const read = await store.getJob(job.id);
        expect(read!.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
        expect(read!.lastError).toContain(LAST_ERROR_TRUNCATION_MARKER);
      });

      it("deferJob caps the terminal (horizon-exceeded) branch", async () => {
        const job = await store.createJob(makeJob({ key: "trunc:defer-term" }));
        await store.deferJob(job.id, 1, new Date(Date.now() + 1_000), "m1", "m1", 60_000);
        await new Promise((resolve) => setTimeout(resolve, 30));
        await store.deferJob(job.id, 2, new Date(Date.now() + 1_000), "d", huge, 1);

        const read = await store.getJob(job.id);
        expect(read!.status).toBe("failed");
        expect(read!.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
        expect(read!.lastError).toContain(LAST_ERROR_TRUNCATION_MARKER);
      });

      it("createJob truncates a preloaded lastError (defense in depth)", async () => {
        const job = await store.createJob(makeJob({ key: "trunc:create", lastError: huge }));
        expect(job.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
        expect(job.lastError).toContain(LAST_ERROR_TRUNCATION_MARKER);

        const read = await store.getJob(job.id);
        expect(read!.lastError!.length).toBeLessThanOrEqual(MAX_LAST_ERROR_CHARS);
      });
    });

    describe("resetJob", () => {
      it("returns null for nonexistent id", async () => {
        expect(await store.resetJob("nonexistent")).toBeNull();
      });

      it("returns null for pending job", async () => {
        const job = await store.createJob(makeJob({ key: "reset:pending" }));
        expect(await store.resetJob(job.id)).toBeNull();
      });

      it("returns null for running job", async () => {
        const job = await store.createJob(makeJob({ key: "reset:running" }));
        await store.markRunning(job.id, job.version);
        expect(await store.resetJob(job.id)).toBeNull();
      });

      it("returns null for completed job", async () => {
        const job = await store.createJob(makeJob({ key: "reset:completed" }));
        await store.markRunning(job.id, job.version);
        await store.markCompleted(job.id, job.version);
        expect(await store.resetJob(job.id)).toBeNull();
      });

      it("returns null for cancelled job", async () => {
        const job = await store.createJob(makeJob({ key: "reset:cancelled" }));
        await store.cancelJob(job.id);
        expect(await store.resetJob(job.id)).toBeNull();
      });

      it("resets a failed job: clears execution state, bumps version, preserves retryConfig", async () => {
        const retryConfig = { attempts: 3, backoff: "exponential" as const, initialDelayMs: 1_000, maxDelayMs: 60_000, jitter: false };
        const job = await store.createJob(makeJob({ key: "reset:ok", maxAttempts: 3, schedulerRef: "old-ref", retryConfig }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), "handler_error");

        const beforeReset = await store.getJob(job.id);
        const nowBefore = Date.now();
        const reset = await store.resetJob(job.id);

        expect(reset).not.toBeNull();
        expect(reset!.status).toBe("pending");
        expect(reset!.attempt).toBe(0);
        expect(reset!.version).toBe(beforeReset!.version + 1);
        expect(reset!.scheduledFor.getTime()).toBeGreaterThanOrEqual(nowBefore - 100);
        expect(reset!.startedAt).toBeNull();
        expect(reset!.completedAt).toBeNull();
        expect(reset!.claimedVersion).toBeNull();
        expect(reset!.lastError).toBeNull();
        expect(reset!.deferAttempts).toBe(0);
        expect(reset!.deferredSince).toBeNull();
        expect(reset!.schedulerRef).toBeNull();
        expect(reset!.maxAttempts).toBe(3);
        expect(reset!.retryConfig).toEqual(retryConfig);
        assertJobInvariants(reset!);
      });

      it("preserves pattern fields for a failed debounce job", async () => {
        const now = Date.now();
        const firstAt = new Date(now - 10_000);
        const lastAt = new Date(now - 6_000);
        const job = await store.createJob(makeDebounceJob("reset:debounce", 5_000, {
          firstAt,
          lastAt,
          scheduledFor: new Date(now - 100),
          maxAttempts: 2,
        }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), "handler_error");

        const reset = await store.resetJob(job.id);

        expect(reset).not.toBeNull();
        expect(reset!.kind).toBe("debounce");
        expect(reset!.waitMs).toBe(5_000);
        expect(reset!.firstAt?.getTime()).toBe(firstAt.getTime());
        expect(reset!.lastAt?.getTime()).toBe(lastAt.getTime());
        assertJobInvariants(reset!);
      });

      it("returns null when the key slot is already held by a newer active row", async () => {
        const job = await store.createJob(makeJob({ key: "reset:conflict" }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), "handler_error");

        // New active row takes the same key
        await store.createJob(makeJob({ key: "reset:conflict" }));

        // Resurrect attempt must fail — would violate at-most-one-active
        expect(await store.resetJob(job.id)).toBeNull();
      });

      it("makes the reset job claimable via claimDueJobs", async () => {
        const job = await store.createJob(makeJob({ key: "reset:claim" }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), "handler_error");

        await store.resetJob(job.id);
        const { toRun } = await store.claimDueJobs(10, ["test"]);
        expect(toRun.some((j) => j.id === job.id)).toBe(true);
      });
    });

    describe("resetJobAt", () => {
      async function plantFailed(key: string): Promise<Job> {
        const job = await store.createJob(makeJob({ key }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), "handler_error");
        return (await store.getJob(job.id))!;
      }

      it("returns null for nonexistent id", async () => {
        expect(await store.resetJobAt("nonexistent", 1, new Date())).toBeNull();
      });

      it("returns null when status is not failed", async () => {
        const job = await store.createJob(makeJob({ key: "rja:pending" }));
        expect(await store.resetJobAt(job.id, job.version, new Date())).toBeNull();
      });

      it("returns null when version doesn't match (CAS guard)", async () => {
        const failed = await plantFailed("rja:cas");
        expect(await store.resetJobAt(failed.id, failed.version + 99, new Date())).toBeNull();
      });

      it("resets to pending with the supplied scheduledFor", async () => {
        const failed = await plantFailed("rja:ok");
        const target = new Date(Date.now() + 30_000);
        const reset = await store.resetJobAt(failed.id, failed.version, target);

        expect(reset).not.toBeNull();
        expect(reset!.status).toBe("pending");
        expect(reset!.attempt).toBe(0);
        expect(reset!.version).toBe(failed.version + 1);
        expect(reset!.scheduledFor.getTime()).toBe(target.getTime());
        expect(reset!.lastError).toBeNull();
        expect(reset!.failureReason).toBeNull();
        expect(reset!.schedulerRef).toBeNull();
        assertJobInvariants(reset!);
      });

      it("returns null when the key slot is already held by a newer active row", async () => {
        const failed = await plantFailed("rja:conflict");
        await store.createJob(makeJob({ key: "rja:conflict" }));
        expect(await store.resetJobAt(failed.id, failed.version, new Date())).toBeNull();
      });
    });

    describe("listFailed", () => {
      async function plantFailed(
        key: string,
        opts: { handler?: string; reason?: any } = {},
      ): Promise<Job> {
        const job = await store.createJob(makeJob({ key, handler: opts.handler ?? "test" }));
        await store.markRunning(job.id, job.version);
        await store.markFailed(job.id, job.version, new Error("boom"), opts.reason ?? "handler_error");
        return (await store.getJob(job.id))!;
      }

      it("returns empty page when no failed rows match", async () => {
        const page = await store.listFailed({ limit: 10 });
        expect(page.jobs).toEqual([]);
        expect(page.cursor).toBeNull();
      });

      it("returns failed rows newest-first by completed_at, id", async () => {
        const a = await plantFailed("lf:a");
        await new Promise((r) => setTimeout(r, 5));
        const b = await plantFailed("lf:b");
        await new Promise((r) => setTimeout(r, 5));
        const c = await plantFailed("lf:c");

        const page = await store.listFailed({ limit: 10 });
        const ids = page.jobs.map((j) => j.id);
        expect(ids).toEqual([c.id, b.id, a.id]);
        expect(page.cursor).toBeNull();
        for (const j of page.jobs) assertJobInvariants(j);
      });

      it("paginates correctly when rows share completed_at (same-ms / same-µs tiebreak)", async () => {
        // Plant rows back-to-back with no setTimeout. On Memory/SQLite they
        // share a millisecond; on Postgres they may share a microsecond
        // bucket under load. Cursor must still visit every row exactly once
        // via the (timestamp, id) tiebreak.
        const planted: Job[] = [];
        for (let i = 0; i < 5; i++) {
          planted.push(await plantFailed(`lf:tie:${i}`));
        }

        const seen = new Set<string>();
        let cursor: string | null = null;
        for (let pages = 0; pages < 10; pages++) {
          const page: { jobs: Job[]; cursor: string | null } = await store.listFailed({
            limit: 2,
            cursor: cursor ?? undefined,
          });
          for (const j of page.jobs) seen.add(j.id);
          if (page.cursor == null) break;
          cursor = page.cursor;
        }

        expect(seen.size).toBe(planted.length);
        for (const p of planted) expect(seen.has(p.id)).toBe(true);
        // Each returned row must satisfy core job invariants.
        for (const p of planted) {
          const fresh = await store.getJob(p.id);
          assertJobInvariants(fresh!);
        }
      });

      it("paginates via cursor across multiple calls", async () => {
        const planted: Job[] = [];
        for (let i = 0; i < 5; i++) {
          planted.push(await plantFailed(`lf:p${i}`));
          await new Promise((r) => setTimeout(r, 2));
        }

        const first = await store.listFailed({ limit: 2 });
        expect(first.jobs).toHaveLength(2);
        expect(first.cursor).not.toBeNull();

        const second = await store.listFailed({ limit: 2, cursor: first.cursor! });
        expect(second.jobs).toHaveLength(2);
        expect(second.cursor).not.toBeNull();

        const third = await store.listFailed({ limit: 2, cursor: second.cursor! });
        expect(third.jobs).toHaveLength(1);
        expect(third.cursor).toBeNull();

        const seen = [...first.jobs, ...second.jobs, ...third.jobs].map((j) => j.id);
        expect(new Set(seen).size).toBe(5);
      });

      it("filters by handler", async () => {
        await plantFailed("lf:h-a", { handler: "alpha" });
        await plantFailed("lf:h-b", { handler: "beta" });
        const page = await store.listFailed({ handler: "alpha", limit: 10 });
        expect(page.jobs).toHaveLength(1);
        expect(page.jobs[0].handler).toBe("alpha");
      });

      it("filters by reason", async () => {
        await plantFailed("lf:r-h", { reason: "handler_error" });
        await plantFailed("lf:r-t", { reason: "timeout" });
        await plantFailed("lf:r-s", { reason: "stalled" });
        const page = await store.listFailed({ reason: "timeout", limit: 10 });
        expect(page.jobs).toHaveLength(1);
        expect(page.jobs[0].failureReason).toBe("timeout");
      });

      it("filters by since/until window", async () => {
        const a = await plantFailed("lf:t-a");
        await new Promise((r) => setTimeout(r, 50));
        const t1 = new Date();
        await new Promise((r) => setTimeout(r, 50));
        const b = await plantFailed("lf:t-b");

        const inWindow = await store.listFailed({ since: t1, limit: 10 });
        expect(inWindow.jobs.map((j) => j.id)).toEqual([b.id]);

        const beforeWindow = await store.listFailed({ until: t1, limit: 10 });
        expect(beforeWindow.jobs.map((j) => j.id)).toEqual([a.id]);
      });

      it("excludes non-failed rows", async () => {
        await plantFailed("lf:x-f");
        const completed = await store.createJob(makeJob({ key: "lf:x-c" }));
        await store.markRunning(completed.id, completed.version);
        await store.markCompleted(completed.id, completed.version);

        const page = await store.listFailed({ limit: 10 });
        expect(page.jobs).toHaveLength(1);
        expect(page.jobs[0].status).toBe("failed");
      });

      it("rejects limit > 1000", async () => {
        await expect(store.listFailed({ limit: 1001 })).rejects.toThrow(/limit/);
      });

      it("rejects non-positive limit", async () => {
        await expect(store.listFailed({ limit: 0 })).rejects.toThrow(/limit/);
      });
    });

    describe("unknownDueJobs", () => {
      it("returns empty when no rows exist", async () => {
        const rows = await store.unknownDueJobs(["any"], 50);
        expect(rows).toEqual([]);
      });

      it("returns due-now pending rows whose handler is not in known", async () => {
        const orphan = await store.createJob(makeJob({ key: "udj:orphan", handler: "ghost" }));
        await store.createJob(makeJob({ key: "udj:claimable", handler: "live" }));

        const rows = await store.unknownDueJobs(["live"], 50);
        expect(rows.map((j) => j.id)).toEqual([orphan.id]);
        for (const j of rows) assertJobInvariants(j);
      });

      it("excludes rows whose handler is in known", async () => {
        await store.createJob(makeJob({ key: "udj:in-1", handler: "live" }));
        await store.createJob(makeJob({ key: "udj:in-2", handler: "live" }));

        const rows = await store.unknownDueJobs(["live"], 50);
        expect(rows).toEqual([]);
      });

      it("excludes rows scheduled for the future", async () => {
        await store.createJob(
          makeJob({ key: "udj:future", handler: "ghost", scheduledFor: new Date(Date.now() + 60_000) }),
        );
        const rows = await store.unknownDueJobs(["live"], 50);
        expect(rows).toEqual([]);
      });

      it("excludes non-pending rows", async () => {
        const running = await store.createJob(makeJob({ key: "udj:running", handler: "ghost" }));
        await store.markRunning(running.id, running.version);

        const failed = await store.createJob(makeJob({ key: "udj:failed", handler: "ghost" }));
        await store.markRunning(failed.id, failed.version);
        await store.markFailed(failed.id, failed.version, new Error("x"), "handler_error");

        const rows = await store.unknownDueJobs(["live"], 50);
        expect(rows).toEqual([]);
      });

      it("orders by (scheduled_for ASC, id ASC) and respects limit", async () => {
        const t = Date.now();
        const a = await store.createJob(
          makeJob({ key: "udj:a", handler: "ghost", scheduledFor: new Date(t - 3_000) }),
        );
        const b = await store.createJob(
          makeJob({ key: "udj:b", handler: "ghost", scheduledFor: new Date(t - 2_000) }),
        );
        await store.createJob(
          makeJob({ key: "udj:c", handler: "ghost", scheduledFor: new Date(t - 1_000) }),
        );

        const rows = await store.unknownDueJobs([], 2);
        expect(rows).toHaveLength(2);
        expect(rows[0].id).toBe(a.id);
        expect(rows[1].id).toBe(b.id);
      });

      it("treats empty known as 'no handlers known' (returns all due-pending rows)", async () => {
        const orphan = await store.createJob(makeJob({ key: "udj:e", handler: "ghost" }));
        const rows = await store.unknownDueJobs([], 50);
        expect(rows.map((j) => j.id)).toContain(orphan.id);
      });

      it("excludes unsettled debounce rows", async () => {
        const now = Date.now();
        // Unsettled: lastAt recent, wait window not elapsed.
        await store.createJob(
          makeDebounceJob("udj:deb-unsettled", 60_000, {
            handler: "ghost",
            scheduledFor: new Date(now - 1_000),
            firstAt: new Date(now - 500),
            lastAt: new Date(now - 100),
          }),
        );
        // Settled: lastAt is older than waitMs ago.
        const settled = await store.createJob(
          makeDebounceJob("udj:deb-settled", 100, {
            handler: "ghost",
            scheduledFor: new Date(now - 500),
            firstAt: new Date(now - 5_000),
            lastAt: new Date(now - 1_000),
          }),
        );

        const rows = await store.unknownDueJobs([], 50);
        expect(rows.map((j) => j.id)).toEqual([settled.id]);
      });

      it("includes debounce rows past maxWait even if not idle-settled", async () => {
        const now = Date.now();
        // Not idle-settled (lastAt recent), but maxWait window elapsed.
        const maxWaitJob = await store.createJob(
          makeDebounceJob("udj:deb-maxwait", 60_000, {
            handler: "ghost",
            scheduledFor: new Date(now - 1_000),
            firstAt: new Date(now - 30_000),
            lastAt: new Date(now - 100),
            maxWaitMs: 10_000,
          }),
        );

        const rows = await store.unknownDueJobs([], 50);
        expect(rows.map((j) => j.id)).toContain(maxWaitJob.id);
      });
    });

    describe("noteMissingHandler", () => {
      it("increments deferAttempts and sets deferredSince without moving scheduledFor", async () => {
        const original = new Date(Date.now() - 1_000);
        const job = await store.createJob(
          makeJob({ key: "nmh:basic", handler: "ghost", scheduledFor: original }),
        );

        const updated = await store.noteMissingHandler(
          job.id, 1, "deferred-msg", "terminal-msg", 60_000,
        );
        expect(updated).not.toBeNull();
        expect(updated!.status).toBe("pending");
        expect(updated!.version).toBe(2);
        expect(updated!.deferAttempts).toBe(1);
        expect(updated!.deferredSince).not.toBeNull();
        expect(updated!.scheduledFor.getTime()).toBe(original.getTime());
        expect(updated!.lastError).toBe("deferred-msg");
        assertJobInvariants(updated!);
      });

      it("preserves deferredSince across consecutive notes", async () => {
        const job = await store.createJob(makeJob({ key: "nmh:since", handler: "ghost" }));

        const first = await store.noteMissingHandler(
          job.id, 1, "d1", "t1", 60_000,
        );
        const originalSince = first!.deferredSince!.getTime();

        await new Promise((r) => setTimeout(r, 10));

        const second = await store.noteMissingHandler(
          job.id, first!.version, "d2", "t2", 60_000,
        );
        expect(second!.deferredSince!.getTime()).toBe(originalSince);
        expect(second!.deferAttempts).toBe(2);
        void job;
      });

      it("flips to failed with reason defer_horizon when horizon exceeded", async () => {
        const job = await store.createJob(makeJob({ key: "nmh:horizon", handler: "ghost" }));

        // Establish deferredSince.
        const first = await store.noteMissingHandler(
          job.id, 1, "d1", "t1", 60_000,
        );
        expect(first!.status).toBe("pending");

        await new Promise((r) => setTimeout(r, 20));

        // Tiny horizon — `now - deferredSince` exceeds it.
        const flipped = await store.noteMissingHandler(
          job.id, first!.version, "d2", "terminal-msg", 1,
        );
        expect(flipped!.status).toBe("failed");
        expect(flipped!.failureReason).toBe("defer_horizon");
        expect(flipped!.lastError).toBe("terminal-msg");
        expect(flipped!.completedAt).not.toBeNull();
        expect(flipped!.attempt).toBe(0);
        assertJobInvariants(flipped!);
        void job;
      });

      it("returns null on version mismatch (CAS loss)", async () => {
        const job = await store.createJob(makeJob({ key: "nmh:cas", handler: "ghost" }));
        const result = await store.noteMissingHandler(
          job.id, 999, "d", "t", 60_000,
        );
        expect(result).toBeNull();
        void job;
      });

      it("returns null when status is not pending", async () => {
        const job = await store.createJob(makeJob({ key: "nmh:running", handler: "ghost" }));
        await store.markRunning(job.id, 1);

        const result = await store.noteMissingHandler(
          job.id, 2, "d", "t", 60_000,
        );
        expect(result).toBeNull();
        void job;
      });
    });
  });
}

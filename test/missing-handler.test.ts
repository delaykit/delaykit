import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { executeJob } from "../src/executor.js";
import { computeDeferBackoffMs } from "../src/result-handler.js";
import {
  DEFER_HORIZON_MS,
  DEFER_INITIAL_MS,
  DEFER_MAX_MS,
} from "../src/types.js";
import { makeJob } from "./helpers/job-factory.js";
import { assertJobInvariants } from "./helpers/invariants.js";
import { ExternalSchedulerHarness } from "./helpers/external-scheduler-harness.js";

describe("computeDeferBackoffMs", () => {
  it("returns initial on first miss", () => {
    expect(computeDeferBackoffMs(1)).toBe(DEFER_INITIAL_MS);
  });

  it("doubles on each attempt", () => {
    expect(computeDeferBackoffMs(2)).toBe(DEFER_INITIAL_MS * 2);
    expect(computeDeferBackoffMs(3)).toBe(DEFER_INITIAL_MS * 4);
    expect(computeDeferBackoffMs(4)).toBe(DEFER_INITIAL_MS * 8);
  });

  it("caps at DEFER_MAX_MS", () => {
    expect(computeDeferBackoffMs(10)).toBe(DEFER_MAX_MS);
    expect(computeDeferBackoffMs(1000)).toBe(DEFER_MAX_MS);
  });
});

describe("executor returns handler_not_registered", () => {
  it("does not touch the row when the handler is missing", async () => {
    const store = new MemoryStore();
    const job = await store.createJob(makeJob({ handler: "gone", key: "k1" }));

    const result = await executeJob(
      { jobId: job.id, version: job.version },
      store,
      new Map(),
    );

    expect(result.status).toBe("handler_not_registered");

    const current = await store.getJob(job.id);
    expect(current!.status).toBe("pending");
    expect(current!.version).toBe(job.version);
    expect(current!.attempt).toBe(0);
    expect(current!.deferAttempts).toBe(0);
    await store.close();
  });
});

describe("PollingScheduler defers missing-handler jobs", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("keeps the row pending and grows backoff across ticks", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 10 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});
    try {
      // Row's handler is not in this kit's registry.
      const job = await store.createJob(
        makeJob({
          handler: "gone",
          key: "k-poll",
          scheduledFor: new Date(),
        }),
      );

      await dk.start();

      await vi.advanceTimersByTimeAsync(20);
      let current = (await store.getJob(job.id))!;
      expect(current.status).toBe("pending");
      expect(current.attempt).toBe(0);
      expect(current.deferAttempts).toBe(1);
      expect(current.deferredSince).not.toBeNull();
      expect(current.scheduledFor.getTime()).toBeGreaterThan(Date.now());
      assertJobInvariants(current);
      const firstScheduled = current.scheduledFor.getTime();

      // Advance past the first backoff (5s) → next poll finds it due again.
      await vi.advanceTimersByTimeAsync(DEFER_INITIAL_MS + 50);
      current = (await store.getJob(job.id))!;
      expect(current.status).toBe("pending");
      expect(current.deferAttempts).toBe(2);
      // Second backoff is 10s → scheduled_for advances further.
      expect(current.scheduledFor.getTime()).toBeGreaterThan(firstScheduled);
      assertJobInvariants(current);
    } finally {
      await dk.stop();
      await store.close();
    }
  });

  it("runs the handler once it is registered on a subsequent process", async () => {
    const store = new MemoryStore();

    // Process #1: handler not registered. Defer at least once.
    {
      const scheduler = new PollingScheduler({ interval: 10 });
      const dk = new DelayKit({ store, scheduler });
      dk.handle("other", async () => {});
      await store.createJob(
        makeJob({
          handler: "gone",
          key: "k-recover",
          scheduledFor: new Date(),
        }),
      );
      try {
        vi.useFakeTimers();
        await dk.start();
        await vi.advanceTimersByTimeAsync(20);
      } finally {
        await dk.stop();
        vi.useRealTimers();
      }
    }

    const afterDefer = await store.getActiveJobByKey("gone", "k-recover");
    expect(afterDefer!.deferAttempts).toBeGreaterThan(0);
    expect(afterDefer!.status).toBe("pending");

    // Process #2: handler is registered. Force the row due and tick.
    let ran = 0;
    {
      // Reset scheduled_for to now so the next poll picks it up without
      // waiting 5+ seconds of fake-timer drift.
      await store.updateScheduledFor(afterDefer!.id, new Date());

      const scheduler = new PollingScheduler({ interval: 10 });
      const dk = new DelayKit({ store, scheduler });
      dk.handle("gone", async () => {
        ran++;
      });
      try {
        vi.useFakeTimers();
        await dk.start();
        await vi.advanceTimersByTimeAsync(30);
      } finally {
        await dk.stop();
        vi.useRealTimers();
      }
    }

    expect(ran).toBe(1);
    const final = await store.getJob(afterDefer!.id);
    expect(final!.status).toBe("completed");
    expect(final!.deferAttempts).toBe(0);
    expect(final!.deferredSince).toBeNull();
    expect(final!.attempt).toBe(0);

    await store.close();
  });
});

describe("dk.poll defers missing-handler jobs", () => {
  it("single-cycle Vercel cron path keeps the row pending", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 1_000 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    const job = await store.createJob(
      makeJob({
        handler: "gone",
        key: "k-dk-poll",
        scheduledFor: new Date(),
      }),
    );

    await dk.poll({ batchSize: 10 });

    const current = (await store.getJob(job.id))!;
    expect(current.status).toBe("pending");
    expect(current.deferAttempts).toBe(1);
    expect(current.attempt).toBe(0);
    expect(current.scheduledFor.getTime()).toBeGreaterThan(Date.now());
    assertJobInvariants(current);
    await store.close();
  });
});

describe("horizon flips the row to failed", () => {
  it("transitions the row from pending directly to failed", async () => {
    const store = new MemoryStore();
    const job = await store.createJob(
      makeJob({ handler: "gone", key: "k-horizon", scheduledFor: new Date() }),
    );

    // First defer establishes `deferredSince`.
    const first = await store.deferJob(
      job.id, 1, new Date(Date.now() + 1_000),
      "deferred-msg", "terminal-msg",
      DEFER_HORIZON_MS,
    );
    expect(first!.status).toBe("pending");

    await new Promise((r) => setTimeout(r, 20));

    // Second defer with a 1ms horizon — `now - deferredSince >= 1ms`
    // so the row flips straight to failed without ever going through
    // `running`.
    const flipped = await store.deferJob(
      job.id, first!.version, new Date(Date.now() + 1_000),
      "deferred-msg", "terminal-msg",
      1,
    );
    expect(flipped!.status).toBe("failed");
    expect(flipped!.completedAt).not.toBeNull();
    expect(flipped!.attempt).toBe(0);
    expect(flipped!.lastError).toBe("terminal-msg");
    assertJobInvariants(flipped!);
    await store.close();
  });
});

describe("createHandler defers missing-handler deliveries", () => {
  it("returns 200 and materializes a replacement hook", async () => {
    const store = new MemoryStore();
    const harness = new ExternalSchedulerHarness();
    const dk = new DelayKit({ store, scheduler: harness });
    dk.handle("other", async () => {});

    // Pre-insert a job with a hook ref that the harness knows about.
    const hookRef = await harness.schedule({
      id: "00000000-0000-0000-0000-000000000001",
      version: 1,
      at: new Date(),
      handler: "gone",
      key: "k-ext",
    });
    await store.createJob(
      makeJob({
        id: "00000000-0000-0000-0000-000000000001",
        handler: "gone",
        key: "k-ext",
        schedulerRef: hookRef!,
        scheduledFor: new Date(),
      }),
    );

    harness.setHandler(dk.createHandler());

    const response = await harness.deliver(hookRef!);
    expect(response.status).toBe(200);

    const after = (await store.getJob("00000000-0000-0000-0000-000000000001"))!;
    expect(after.status).toBe("pending");
    expect(after.deferAttempts).toBe(1);
    expect(after.deferredSince).not.toBeNull();
    expect(after.scheduledFor.getTime()).toBeGreaterThan(Date.now());

    // Replacement hook materialized and stored. The old hook is left
    // uncancelled — the schedulerRef guard in createHandler rejects
    // any stale redelivery.
    expect(after.schedulerRef).not.toBe(hookRef);
    expect(harness.hookFor(after.id)?.ref).toBe(after.schedulerRef);

    // Re-delivering the original hook now lands on a stale ref and
    // is silently ignored with 200.
    const stale = await harness.deliver(hookRef!);
    expect(stale.status).toBe(200);
    const unchanged = await store.getJob(after.id);
    expect(unchanged!.schedulerRef).toBe(after.schedulerRef);
    expect(unchanged!.deferAttempts).toBe(1);

    assertJobInvariants(after);
    await store.close();
  });

  it("preserves the original retry shape on the replacement hook", async () => {
    const store = new MemoryStore();
    const harness = new ExternalSchedulerHarness();
    const dk = new DelayKit({ store, scheduler: harness });
    dk.handle("other", async () => {});

    const hookRef = await harness.schedule({
      id: "00000000-0000-0000-0000-000000000002",
      version: 1,
      at: new Date(),
      handler: "gone",
      key: "k-retry",
    });
    // Row carries the retry snapshot written at schedule time. The
    // handler is absent on this instance, so `materializeWake` has
    // to read from the row rather than the handler registry.
    await store.createJob(
      makeJob({
        id: "00000000-0000-0000-0000-000000000002",
        handler: "gone",
        key: "k-retry",
        schedulerRef: hookRef!,
        scheduledFor: new Date(),
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
    harness.setHandler(dk.createHandler());

    const response = await harness.deliver(hookRef!);
    expect(response.status).toBe(200);

    const after = (await store.getJob("00000000-0000-0000-0000-000000000002"))!;
    const replacement = harness.hookFor(after.id)!;
    expect(replacement.ref).toBe(after.schedulerRef);
    expect(replacement.retry).toEqual({
      attempts: 5,
      backoff: "exponential",
      initialDelayMs: 30_000,
      maxDelayMs: 600_000,
      jitter: true,
    });
    await store.close();
  });
});

describe("job:failed on horizon flip", () => {
  it("emits the same error message that was persisted to the row", async () => {
    const store = new MemoryStore();
    const harness = new ExternalSchedulerHarness();
    const dk = new DelayKit({ store, scheduler: harness });
    dk.handle("other", async () => {});

    const events: { error: Error; lastError: string | null }[] = [];
    dk.on("job:failed", (e) => events.push({ error: e.error, lastError: e.job.lastError }));

    const hookRef = await harness.schedule({
      id: "00000000-0000-0000-0000-000000000099",
      version: 1,
      at: new Date(),
      handler: "gone",
      key: "k-emit",
    });
    // Backdate deferredSince so the production DEFER_HORIZON_MS is
    // already exceeded at delivery time — no need to stub the constant.
    const created = await store.createJob(
      makeJob({
        id: "00000000-0000-0000-0000-000000000099",
        handler: "gone",
        key: "k-emit",
        schedulerRef: hookRef!,
        scheduledFor: new Date(),
        deferAttempts: 1,
        deferredSince: new Date(Date.now() - DEFER_HORIZON_MS - 1_000),
      }),
    );

    harness.setHandler(dk.createHandler());
    const response = await harness.deliver(hookRef!);
    expect(response.status).toBe(200);

    const final = (await store.getJob(created.id))!;
    expect(final.status).toBe("failed");
    expect(events).toHaveLength(1);
    expect(events[0].error.message).toBe(final.lastError);
    await store.close();
  });
});

describe("dk.cancel / dk.unschedule on a deferred row", () => {
  it("clears defer metadata and marks the row cancelled", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 1_000 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    const job = await store.createJob(
      makeJob({
        handler: "gone",
        key: "k-cancel-defer",
        scheduledFor: new Date(),
      }),
    );
    await dk.poll({ batchSize: 10 });
    const deferred = (await store.getJob(job.id))!;
    expect(deferred.deferAttempts).toBe(1);
    expect(deferred.deferredSince).not.toBeNull();

    const cancelled = await dk.cancel(job.id);
    expect(cancelled).toBe(true);

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("cancelled");
    expect(after.deferAttempts).toBe(0);
    expect(after.deferredSince).toBeNull();
    assertJobInvariants(after);
    await store.close();
  });

  it("unschedule by (handler, key) clears defer metadata", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 1_000 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    await store.createJob(
      makeJob({
        handler: "gone",
        key: "k-unschedule-defer",
        scheduledFor: new Date(),
      }),
    );
    await dk.poll({ batchSize: 10 });

    const before = await store.getActiveJobByKey("gone", "k-unschedule-defer");
    expect(before!.deferAttempts).toBe(1);

    const removed = await dk.unschedule("gone", "k-unschedule-defer");
    expect(removed).toBe(true);

    const active = await store.getActiveJobByKey("gone", "k-unschedule-defer");
    expect(active).toBeNull();
    await store.close();
  });
});

describe("deferHorizon option", () => {
  it("respects a custom horizon at the API level", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 10 });
    const dk = new DelayKit({ store, scheduler, deferHorizon: "50ms" });
    dk.handle("other", async () => {});

    const job = await store.createJob(
      makeJob({
        handler: "gone",
        key: "k-horizon-opt",
        scheduledFor: new Date(),
      }),
    );

    // First tick defers the job.
    await dk.poll({ batchSize: 10 });
    let current = (await store.getJob(job.id))!;
    expect(current.status).toBe("pending");
    expect(current.deferAttempts).toBe(1);

    // Wait past the 50ms horizon, force the row due again, and tick.
    await new Promise((r) => setTimeout(r, 80));
    await store.updateScheduledFor(job.id, new Date());
    await dk.poll({ batchSize: 10 });

    current = (await store.getJob(job.id))!;
    expect(current.status).toBe("failed");
    expect(current.completedAt).not.toBeNull();
    await store.close();
  });
});

describe("pattern race during defer", () => {
  it("returns null and leaves the row to the pattern flow when version advances", async () => {
    const store = new MemoryStore();
    const job = await store.createJob(
      makeJob({ handler: "gone", key: "k-race", scheduledFor: new Date() }),
    );

    // Simulate a concurrent pattern-style version bump by calling
    // deferJob once to move the version forward.
    await store.deferJob(job.id, 1, new Date(Date.now() + 5_000), "m1d", "m1t", DEFER_HORIZON_MS);

    // Caller still holds the original version — CAS loses.
    const attempted = await store.deferJob(
      job.id, 1, new Date(Date.now() + 10_000),
      "m2d", "m2t",
      DEFER_HORIZON_MS,
    );
    expect(attempted).toBeNull();

    const current = await store.getJob(job.id);
    expect(current!.deferAttempts).toBe(1);
    await store.close();
  });
});

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
import { makeJob, makeDebounceJob } from "./helpers/job-factory.js";
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

describe("PollingScheduler skips missing-handler rows", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("leaves rows untouched when the handler isn't registered", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 10 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});
    try {
      const originalScheduledFor = new Date();
      const job = await store.createJob(
        makeJob({
          handler: "gone",
          key: "k-poll",
          scheduledFor: originalScheduledFor,
        }),
      );

      await dk.start();
      await vi.advanceTimersByTimeAsync(100);

      const current = (await store.getJob(job.id))!;
      // Row was never claimed — handler filter excluded it.
      expect(current.status).toBe("pending");
      expect(current.claimedVersion).toBeNull();
      expect(current.attempt).toBe(0);
      expect(current.deferAttempts).toBe(0);
      expect(current.deferredSince).toBeNull();
      expect(current.scheduledFor.getTime()).toBe(originalScheduledFor.getTime());
      assertJobInvariants(current);
    } finally {
      await dk.stop();
      await store.close();
    }
  });

  it("runs the handler once it is registered on a subsequent process", async () => {
    const store = new MemoryStore();

    // Process #1: handler not registered. Row stays untouched.
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
        await vi.advanceTimersByTimeAsync(50);
      } finally {
        await dk.stop();
        vi.useRealTimers();
      }
    }

    const afterSkip = await store.getActiveJobByKey("gone", "k-recover");
    expect(afterSkip!.status).toBe("pending");
    expect(afterSkip!.claimedVersion).toBeNull();

    // Process #2: handler is registered. Row claims and runs.
    let ran = 0;
    {
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
    const final = await store.getJob(afterSkip!.id);
    expect(final!.status).toBe("completed");
    expect(final!.attempt).toBe(0);

    await store.close();
  });
});

describe("dk.poll notes missing-handler rows", () => {
  it("starts the horizon clock without advancing scheduledFor", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 1_000 });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    const originalScheduledFor = new Date();
    const job = await store.createJob(
      makeJob({
        handler: "gone",
        key: "k-dk-poll",
        scheduledFor: originalScheduledFor,
      }),
    );

    await dk.poll({ batchSize: 10 });

    const current = (await store.getJob(job.id))!;
    // Row stays pending and DUE — capable replicas in mixed-handler
    // deployments must still see it as claimable. The horizon clock
    // is recorded; scheduled_for is unchanged.
    expect(current.status).toBe("pending");
    expect(current.claimedVersion).toBeNull();
    expect(current.deferAttempts).toBe(1);
    expect(current.deferredSince).not.toBeNull();
    expect(current.scheduledFor.getTime()).toBe(originalScheduledFor.getTime());
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

describe("dk.cancel / dk.unschedule on a missing-handler row", () => {
  it("cancels an unclaimed row whose handler isn't registered locally", async () => {
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
    // Row untouched — handler filter skipped it.
    const skipped = (await store.getJob(job.id))!;
    expect(skipped.status).toBe("pending");

    const cancelled = await dk.cancel(job.id);
    expect(cancelled).toBe(true);

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("cancelled");
    await store.close();
  });

  it("unschedule by (handler, key) works on a missing-handler row", async () => {
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

    const removed = await dk.unschedule("gone", "k-unschedule-defer");
    expect(removed).toBe(true);

    const active = await store.getActiveJobByKey("gone", "k-unschedule-defer");
    expect(active).toBeNull();
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

describe("PollingScheduler missing-handler horizon pass", () => {
  it("starts the horizon clock at sweep cadence without moving scheduledFor", async () => {
    vi.useFakeTimers();
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({
      interval: 100,
      stalledCheckInterval: 1_000,
    });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    const original = new Date();
    const job = await store.createJob(
      makeJob({ handler: "gone", key: "ds:1", scheduledFor: original }),
    );

    try {
      await dk.start();
      await vi.advanceTimersByTimeAsync(1_100);
    } finally {
      await dk.stop();
      vi.useRealTimers();
    }

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("pending");
    expect(after.deferAttempts).toBe(1);
    expect(after.deferredSince).not.toBeNull();
    expect(after.scheduledFor.getTime()).toBe(original.getTime());
    assertJobInvariants(after);
    await store.close();
  });

  it("does not emit job:awaiting_handler from the poll path", async () => {
    vi.useFakeTimers();
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({
      interval: 100,
      stalledCheckInterval: 1_000,
    });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    const events: any[] = [];
    dk.on("job:awaiting_handler", (e) => events.push(e));

    await store.createJob(
      makeJob({ handler: "gone", key: "aw:1", scheduledFor: new Date() }),
    );

    try {
      await dk.start();
      await vi.advanceTimersByTimeAsync(1_100);
    } finally {
      await dk.stop();
      vi.useRealTimers();
    }

    // Poll-path operators get the unknownDueHandlers console warning
    // and the terminal job:failed at horizon — no per-cycle event.
    expect(events).toHaveLength(0);
    await store.close();
  });

  it("flips the row to failed with reason defer_horizon after the horizon elapses", async () => {
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({ interval: 100, stalledCheckInterval: 50 });
    const dk = new DelayKit({ store, scheduler, deferHorizon: "1ms" });
    dk.handle("other", async () => {});

    const failed: any[] = [];
    dk.on("job:failed", (e) => failed.push(e));

    const job = await store.createJob(
      makeJob({ handler: "gone", key: "h:1", scheduledFor: new Date() }),
    );

    try {
      await dk.start();
      // Two sweeps: first sets deferredSince, second finds the row
      // still due (scheduled_for unchanged) and flips it because
      // now - deferredSince > 1ms.
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("failed");
    expect(after.failureReason).toBe("defer_horizon");
    expect(failed).toHaveLength(1);
    expect(failed[0].reason).toBe("defer_horizon");
    await store.close();
  });

  it("does not steal work from a replica that registers the handler (P1 regression)", async () => {
    const store = new MemoryStore();
    const job = await store.createJob(
      makeJob({ handler: "shared", key: "x:1", scheduledFor: new Date(Date.now() - 1_000) }),
    );

    // Replica A lacks "shared". A 1ms horizon would normally flip a
    // row on its second pass — this test proves the row stays
    // claimable for replica B because A's note doesn't move
    // scheduled_for.
    const dkA = new DelayKit({
      store,
      scheduler: new PollingScheduler(),
      deferHorizon: "1ms",
    });
    dkA.handle("other", async () => {});
    await dkA.poll({ batchSize: 10 });

    const afterA = (await store.getJob(job.id))!;
    expect(afterA.status).toBe("pending");
    expect(afterA.deferAttempts).toBe(1);
    expect(afterA.scheduledFor.getTime()).toBe(job.scheduledFor.getTime());

    // Replica B has "shared" and polls immediately — no waiting for a
    // defer push to elapse. The row is still due, so B claims and
    // runs.
    let ran = 0;
    const dkB = new DelayKit({ store, scheduler: new PollingScheduler() });
    dkB.handle("shared", async () => { ran++; });
    await dkB.poll({ batchSize: 10 });

    expect(ran).toBe(1);
    const final = (await store.getJob(job.id))!;
    expect(final.status).toBe("completed");
    await store.close();
  });

  it("does not start the horizon for unsettled debounce rows (P2 regression)", async () => {
    const store = new MemoryStore();
    const dk = new DelayKit({ store, scheduler: new PollingScheduler() });
    dk.handle("other", async () => {});

    // Unsettled debounce row: lastAt is recent, wait window has not
    // elapsed. claimDueJobs would reschedule rather than dispatch;
    // the missing-handler horizon must not fire on it.
    const now = Date.now();
    const job = await store.createJob(
      makeDebounceJob("deb:1", 60_000, {
        handler: "gone",
        scheduledFor: new Date(now - 1_000),
        firstAt: new Date(now - 500),
        lastAt: new Date(now - 100),
      }),
    );

    await dk.poll({ batchSize: 10 });

    const after = (await store.getJob(job.id))!;
    expect(after.deferAttempts).toBe(0);
    expect(after.deferredSince).toBeNull();
    await store.close();
  });

  it("respects the per-sweep budget", async () => {
    vi.useFakeTimers();
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({
      interval: 100,
      stalledCheckInterval: 1_000,
    });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    // Plant 75 unknown-handler rows; per-sweep budget caps at 50.
    for (let i = 0; i < 75; i++) {
      await store.createJob(
        makeJob({
          handler: "gone",
          key: `b:${i}`,
          scheduledFor: new Date(Date.now() - 1_000 + i),
        }),
      );
    }

    try {
      await dk.start();
      await vi.advanceTimersByTimeAsync(1_100);
    } finally {
      await dk.stop();
      vi.useRealTimers();
    }

    let noted = 0;
    for (let i = 0; i < 75; i++) {
      const row = await store.getActiveJobByKey("gone", `b:${i}`);
      if (row && row.deferAttempts === 1) noted++;
    }
    expect(noted).toBe(50);
    await store.close();
  });

  it("clock-starts every orphan within ceil(count/budget) sweeps (P2 regression)", async () => {
    vi.useFakeTimers();
    const store = new MemoryStore();
    const scheduler = new PollingScheduler({
      interval: 100,
      stalledCheckInterval: 1_000,
    });
    const dk = new DelayKit({ store, scheduler });
    dk.handle("other", async () => {});

    // 75 orphans + budget=50: first sweep notes 50, second sweep
    // must reach the remaining 25 (because ordering prioritizes
    // deferred_since=NULL rows).
    for (let i = 0; i < 75; i++) {
      await store.createJob(
        makeJob({
          handler: "gone",
          key: `r:${i}`,
          scheduledFor: new Date(Date.now() - 1_000 + i),
        }),
      );
    }

    try {
      await dk.start();
      await vi.advanceTimersByTimeAsync(2_100);
    } finally {
      await dk.stop();
      vi.useRealTimers();
    }

    let unnoted = 0;
    for (let i = 0; i < 75; i++) {
      const row = await store.getActiveJobByKey("gone", `r:${i}`);
      if (row && row.deferredSince === null) unnoted++;
    }
    expect(unnoted).toBe(0);
    await store.close();
  });

  it("dk.poll() flips the row to failed at the horizon", async () => {
    const store = new MemoryStore();
    const dk = new DelayKit({
      store,
      scheduler: new PollingScheduler(),
      deferHorizon: "1ms",
    });
    dk.handle("other", async () => {});

    const failed: any[] = [];
    dk.on("job:failed", (e) => failed.push(e));

    const job = await store.createJob(
      makeJob({ handler: "gone", key: "p:1", scheduledFor: new Date() }),
    );

    // First poll establishes deferredSince. Second poll, after >1ms
    // wall clock, finds the horizon exceeded. No `updateScheduledFor`
    // needed — `noteMissingHandler` doesn't move scheduledFor, so the
    // row stays due naturally.
    await dk.poll({ batchSize: 10 });
    const afterFirst = (await store.getJob(job.id))!;
    expect(afterFirst.status).toBe("pending");
    expect(afterFirst.deferAttempts).toBe(1);

    await new Promise((r) => setTimeout(r, 10));
    await dk.poll({ batchSize: 10 });

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("failed");
    expect(after.failureReason).toBe("defer_horizon");
    expect(failed).toHaveLength(1);
    expect(failed[0].reason).toBe("defer_horizon");
    await store.close();
  });

  it("still emits the unknownDueHandlers warning when orphans exist", async () => {
    const store = new MemoryStore();
    const dk = new DelayKit({ store, scheduler: new PollingScheduler() });
    dk.handle("other", async () => {});
    await store.createJob(
      makeJob({ handler: "gone", key: "w:1", scheduledFor: new Date() }),
    );

    const original = console.warn;
    const messages: string[] = [];
    console.warn = (msg: unknown) => { if (typeof msg === "string") messages.push(msg); };
    try {
      await dk.poll({ batchSize: 10 });
    } finally {
      console.warn = original;
    }

    const fired = messages.some((msg) => msg.includes("not registered on this replica"));
    expect(fired).toBe(true);
    await store.close();
  });
});

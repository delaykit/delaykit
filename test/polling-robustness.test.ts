/**
 * PollingScheduler production-hardening tests.
 *
 * - **DB outage backoff** — when a store call throws, the next poll
 *   (or stalled sweep) is delayed with exponential backoff capped at
 *   30s, so a paused DB doesn't cause a 1Hz hot-spin. Counters are
 *   independent between poll and stalled sweep.
 * - **Graceful drain on stop** — `stop({ drainMs })` waits for
 *   in-flight handlers to finish before returning, so a deploy
 *   rollover doesn't cut handlers mid-execution.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

function createKit(options?: { interval?: number; stalledCheckInterval?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({
    interval: options?.interval ?? 100,
    stalledCheckInterval: options?.stalledCheckInterval ?? 100,
  });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("PollingScheduler robustness", () => {
  let dk: DelayKit;
  let errorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    // Suppress the intentional error logs this suite produces.
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    errorSpy.mockRestore();
    vi.useRealTimers();
  });

  describe("DB outage backoff", () => {
    it("backs off exponentially on repeated poll errors", async () => {
      const { dk: kit, store } = createKit({ interval: 100 });
      dk = kit;

      let callCount = 0;
      store.getDueJobs = async () => {
        callCount++;
        throw new Error("db down");
      };

      dk.handle("task", async () => {});
      await dk.start();

      // Timer fires at t=interval; then backoff 2x, 4x, 8x.
      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(1);
      await vi.advanceTimersByTimeAsync(200);
      expect(callCount).toBe(2);
      await vi.advanceTimersByTimeAsync(400);
      expect(callCount).toBe(3);
      await vi.advanceTimersByTimeAsync(800);
      expect(callCount).toBe(4);
    });

    it("caps backoff at 30 seconds", async () => {
      const { dk: kit, store } = createKit({ interval: 1_000 });
      dk = kit;

      let callCount = 0;
      store.getDueJobs = async () => {
        callCount++;
        throw new Error("db down");
      };

      dk.handle("task", async () => {});
      await dk.start();

      // Tick through several backoff doublings; eventually delay caps at 30s.
      // interval=1s, doublings: 2s, 4s, 8s, 16s — then cap at 30s (32s would exceed).
      await vi.advanceTimersByTimeAsync(1_000); // t=1s
      await vi.advanceTimersByTimeAsync(2_000); // t=3s
      await vi.advanceTimersByTimeAsync(4_000); // t=7s
      await vi.advanceTimersByTimeAsync(8_000); // t=15s
      await vi.advanceTimersByTimeAsync(16_000); // t=31s (5th failure)
      expect(callCount).toBe(5);

      // 6th would be at 1s * 2^5 = 32s, but capped at 30s.
      await vi.advanceTimersByTimeAsync(30_000);
      expect(callCount).toBe(6);

      // 7th also at 30s cap.
      await vi.advanceTimersByTimeAsync(30_000);
      expect(callCount).toBe(7);
    });

    it("resets backoff on first successful poll", async () => {
      const { dk: kit, store } = createKit({ interval: 100 });
      dk = kit;

      let callCount = 0;
      let shouldFail = true;
      const original = store.getDueJobs.bind(store);
      store.getDueJobs = async (limit) => {
        callCount++;
        if (shouldFail) throw new Error("db down");
        return original(limit);
      };

      dk.handle("task", async () => {});
      await dk.start();

      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(1);
      await vi.advanceTimersByTimeAsync(200);
      expect(callCount).toBe(2);

      // DB comes back.
      shouldFail = false;
      // Next tick is at 4x interval (400ms); after it succeeds, the
      // following tick returns to base interval.
      await vi.advanceTimersByTimeAsync(400);
      expect(callCount).toBe(3);
      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(4);
    });

    it("never retries faster than the configured base interval", async () => {
      // Regression: with interval > BACKOFF_MAX_MS (30s), an earlier
      // formula capped the doubled delay at 30s, which was faster
      // than the configured cadence — doubling load on a flaky DB.
      const { dk: kit, store } = createKit({ interval: 60_000 });
      dk = kit;

      let callCount = 0;
      store.getDueJobs = async () => {
        callCount++;
        throw new Error("db down");
      };

      dk.handle("task", async () => {});
      await dk.start();

      // First attempt at t=60s.
      await vi.advanceTimersByTimeAsync(60_000);
      expect(callCount).toBe(1);

      // With the old (buggy) formula, next attempt would be at 30s
      // (min(30_000, 60_000*2)) — i.e. 30s after the first, so
      // total t=90s. Assert that's NOT the case: nothing fires
      // between t=60s and t=119s.
      await vi.advanceTimersByTimeAsync(59_000); // advance to t=119s
      expect(callCount).toBe(1);

      // Second attempt at t=120s (60s after the first).
      await vi.advanceTimersByTimeAsync(1_000); // t=120s
      expect(callCount).toBe(2);
    });

    it("tracks poll and stalled-sweep backoff independently", async () => {
      const { dk: kit, store } = createKit({
        interval: 100,
        stalledCheckInterval: 100,
      });
      dk = kit;

      store.getDueJobs = async () => {
        throw new Error("select failed");
      };
      let sweepCount = 0;
      const origReclaim = store.reclaimStalledJobs.bind(store);
      store.reclaimStalledJobs = async (t) => {
        sweepCount++;
        return origReclaim(t);
      };

      dk.handle("task", async () => {});
      await dk.start();

      // In 1 second of fake time, the stalled sweep should fire on
      // its steady cadence even though poll is backing off.
      await vi.advanceTimersByTimeAsync(1_000);
      expect(sweepCount).toBeGreaterThanOrEqual(8);
    });
  });

  describe("graceful drain on stop", () => {
    it("waits for in-flight handlers before returning from stop({ drainMs })", async () => {
      const { dk: kit } = createKit({ interval: 50 });
      dk = kit;

      let gateResolve: (() => void) | null = null;
      const gate = new Promise<void>((resolve) => {
        gateResolve = resolve;
      });
      let handlerFinished = false;

      dk.handle("slow", async () => {
        await gate;
        handlerFinished = true;
      });

      await dk.schedule("slow", { key: "k:1", at: new Date() });
      await dk.start();

      // Let the handler enter.
      await vi.advanceTimersByTimeAsync(60);

      // stop begins drain; handler is parked on gate.
      let stopResolved = false;
      const stopPromise = dk.stop({ drainMs: 5_000 }).then(() => {
        stopResolved = true;
      });

      // Advance past 100ms of drain polling — stop shouldn't be done yet.
      await vi.advanceTimersByTimeAsync(100);
      expect(stopResolved).toBe(false);
      expect(handlerFinished).toBe(false);

      // Release the gate. The handler finishes; drain completes.
      gateResolve!();
      await vi.advanceTimersByTimeAsync(100);
      await stopPromise;
      expect(handlerFinished).toBe(true);
      expect(stopResolved).toBe(true);
    });

    it("returns after drainMs with a warning when handlers don't finish", async () => {
      const { dk: kit } = createKit({ interval: 50 });
      dk = kit;

      dk.handle("forever", async () => {
        // Never resolves, ignores signal — simulates an uncooperative
        // handler that blocks drain.
        await new Promise<void>(() => {});
      });

      await dk.schedule("forever", { key: "k:1", at: new Date() });
      await dk.start();

      await vi.advanceTimersByTimeAsync(60);

      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      const stopPromise = dk.stop({ drainMs: 200 });
      await vi.advanceTimersByTimeAsync(300);
      await stopPromise;

      expect(warnSpy).toHaveBeenCalled();
      const logged = warnSpy.mock.calls.map((c) => c.join(" ")).join(" ");
      expect(logged).toContain("drain timeout");

      warnSpy.mockRestore();
    });

    it("waits for a poll that's mid-await to finish dispatching before resolving drain", async () => {
      // Regression: if stop({ drainMs }) is called while poll() is
      // awaiting getDueJobs, the drain loop must NOT resolve until
      // the in-flight poll finishes dispatching and those handlers
      // complete. Otherwise handlers dispatched after stop returns
      // can be cut off by process exit.
      const { dk: kit, store } = createKit({ interval: 50 });
      dk = kit;

      let releaseGetDueJobs: ((jobs: unknown[]) => void) | null = null;
      const origGetDueJobs = store.getDueJobs.bind(store);
      let getDueJobsIntercepted = false;
      store.getDueJobs = async (limit) => {
        if (getDueJobsIntercepted) return origGetDueJobs(limit);
        getDueJobsIntercepted = true;
        const real = await origGetDueJobs(limit);
        // Hold the result until the test releases.
        return await new Promise<typeof real>((resolve) => {
          releaseGetDueJobs = (jobs) => resolve(jobs as typeof real);
        });
      };

      let handlerFinished = false;
      dk.handle("task", async () => {
        handlerFinished = true;
      });

      await dk.schedule("task", { key: "k:1", at: new Date() });
      await dk.start();

      // Let the poll timer fire. poll() is now parked on the
      // intercepted getDueJobs.
      await vi.advanceTimersByTimeAsync(60);
      expect(releaseGetDueJobs).not.toBeNull();

      // Stop during the awaited getDueJobs. With the old code
      // `inFlight` is 0 so drain would return immediately.
      let stopResolved = false;
      const stopPromise = dk.stop({ drainMs: 5_000 }).then(() => {
        stopResolved = true;
      });
      await vi.advanceTimersByTimeAsync(100);
      expect(stopResolved).toBe(false);
      expect(handlerFinished).toBe(false);

      // Now let getDueJobs resolve. poll() will increment inFlight
      // and dispatch the handler; drain must keep waiting until the
      // handler finishes.
      const real = await origGetDueJobs(10);
      releaseGetDueJobs!(real);

      await vi.advanceTimersByTimeAsync(200);
      await stopPromise;
      expect(handlerFinished).toBe(true);
      expect(stopResolved).toBe(true);
    });

    it("fast-stops when drainMs is omitted (preserves current behavior)", async () => {
      const { dk: kit } = createKit({ interval: 50 });
      dk = kit;
      dk.handle("noop", async () => {});
      await dk.start();

      // No handlers in flight; stop returns immediately without waiting.
      await dk.stop();
      // Pass criterion: the test didn't hang.
    });
  });
});

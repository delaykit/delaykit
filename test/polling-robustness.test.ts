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
import { PollingScheduler, computeBackoffDelay } from "../src/schedulers/polling.js";
import { makeStalledJob } from "./helpers/job-factory.js";

describe("computeBackoffDelay", () => {
  it("returns baseMs when attempts is 0, regardless of rand", () => {
    expect(computeBackoffDelay(1_000, 0, 0)).toBe(1_000);
    expect(computeBackoffDelay(1_000, 0, 1)).toBe(1_000);
  });

  it("doubles per attempt (rand=0.5 → zero jitter)", () => {
    expect(computeBackoffDelay(1_000, 1, 0.5)).toBe(2_000);
    expect(computeBackoffDelay(1_000, 2, 0.5)).toBe(4_000);
    expect(computeBackoffDelay(1_000, 3, 0.5)).toBe(8_000);
  });

  it("applies −25% jitter when rand=0", () => {
    // delay=2000, jitter=2000*0.25*-1=-500 → 1500
    expect(computeBackoffDelay(1_000, 1, 0)).toBe(1_500);
  });

  it("applies +25% jitter when rand=1", () => {
    // delay=2000, jitter=2000*0.25*1=+500 → 2500
    expect(computeBackoffDelay(1_000, 1, 1)).toBe(2_500);
  });

  it("caps at BACKOFF_MAX_MS (30s) after positive jitter", () => {
    // attempts=10: delay=min(30000,1000*1024)=30000; +25%=37500 → capped at 30000
    expect(computeBackoffDelay(1_000, 10, 1)).toBe(30_000);
  });

  it("floors at baseMs when jitter would push below it", () => {
    // baseMs=60000 > BACKOFF_MAX_MS; delay=max(60000,min(30000,120000))=60000
    // rand=0: jitter=-15000 → max(60000, min(30000, 45000))=60000
    expect(computeBackoffDelay(60_000, 1, 0)).toBe(60_000);
  });
});

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
  let randomSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    // Suppress the intentional error logs this suite produces.
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    // Neutralize jitter so timing-sensitive backoff tests are deterministic.
    // Math.random() * 2 - 1 = 0 when random = 0.5, so jitter term = 0.
    randomSpy = vi.spyOn(Math, "random").mockReturnValue(0.5);
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    errorSpy.mockRestore();
    randomSpy.mockRestore();
    vi.useRealTimers();
  });

  describe("DB outage backoff", () => {
    it("backs off exponentially on repeated poll errors", async () => {
      const { dk: kit, store } = createKit({ interval: 100 });
      dk = kit;

      let callCount = 0;
      store.claimDueJobs = async () => {
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
      store.claimDueJobs = async () => {
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
      const original = store.claimDueJobs.bind(store);
      store.claimDueJobs = async (limit, handlerNames) => {
        callCount++;
        if (shouldFail) throw new Error("db down");
        return original(limit, handlerNames);
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
      store.claimDueJobs = async () => {
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

      store.claimDueJobs = async () => {
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

  describe("jitter on poll-error backoff", () => {
    it("reduces delay by 25% when Math.random returns 0 (maximum negative jitter)", async () => {
      // attempts=1: delay = max(100, min(30000, 100*2)) = 200ms
      // jitter = 200 * 0.25 * (0*2-1) = -50ms → actual = 150ms
      const { dk: kit, store } = createKit({ interval: 100 });
      dk = kit;
      randomSpy.mockReturnValue(0);

      let callCount = 0;
      store.claimDueJobs = async () => { callCount++; throw new Error("db down"); };

      dk.handle("task", async () => {});
      await dk.start();

      await vi.advanceTimersByTimeAsync(100); // first poll at base interval (no jitter)
      expect(callCount).toBe(1);

      await vi.advanceTimersByTimeAsync(149);
      expect(callCount).toBe(1); // not yet at 149ms

      await vi.advanceTimersByTimeAsync(1);
      expect(callCount).toBe(2); // fired at 150ms
    });

    it("increases delay by 25% when Math.random returns 1 (maximum positive jitter)", async () => {
      // attempts=1: delay = 200ms, jitter = 200 * 0.25 * 1 = +50ms → actual = 250ms
      const { dk: kit, store } = createKit({ interval: 100 });
      dk = kit;
      randomSpy.mockReturnValue(1);

      let callCount = 0;
      store.claimDueJobs = async () => { callCount++; throw new Error("db down"); };

      dk.handle("task", async () => {});
      await dk.start();

      await vi.advanceTimersByTimeAsync(100);
      expect(callCount).toBe(1);

      await vi.advanceTimersByTimeAsync(249);
      expect(callCount).toBe(1); // not yet at 249ms

      await vi.advanceTimersByTimeAsync(1);
      expect(callCount).toBe(2); // fired at 250ms
    });

    it("never delays faster than the base interval regardless of jitter", async () => {
      // interval=60_000 > BACKOFF_MAX_MS=30_000, so backoff floor kicks in.
      // delay = max(60000, min(30000, 60000*2^1)) = 60000
      // With random=0: jitter = 60000 * 0.25 * -1 = -15000 → max(60000, 45000) = 60000
      const { dk: kit, store } = createKit({ interval: 60_000 });
      dk = kit;
      randomSpy.mockReturnValue(0);

      let callCount = 0;
      store.claimDueJobs = async () => { callCount++; throw new Error("db down"); };

      dk.handle("task", async () => {});
      await dk.start();

      await vi.advanceTimersByTimeAsync(60_000);
      expect(callCount).toBe(1);

      await vi.advanceTimersByTimeAsync(59_999);
      expect(callCount).toBe(1); // jitter could not push below the 60s floor

      await vi.advanceTimersByTimeAsync(1);
      expect(callCount).toBe(2);
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
      // awaiting claimDueJobs, the drain loop must NOT resolve until
      // the in-flight poll finishes dispatching and those handlers
      // complete. Otherwise handlers dispatched after stop returns
      // can be cut off by process exit.
      const { dk: kit, store } = createKit({ interval: 50 });
      dk = kit;

      // Park the poll mid-await: the first claim call runs the real
      // claim then suspends before returning. Because the claim
      // mutates state, we capture its result inside the interceptor
      // and release it later via releaseClaim() rather than calling
      // origClaimDueJobs() a second time.
      const origClaimDueJobs = store.claimDueJobs.bind(store);
      let releaseClaim: (() => void) | null = null;
      let claimDueJobsIntercepted = false;
      store.claimDueJobs = async (limit, handlerNames) => {
        if (claimDueJobsIntercepted) return origClaimDueJobs(limit, handlerNames);
        claimDueJobsIntercepted = true;
        const real = await origClaimDueJobs(limit, handlerNames);
        return await new Promise<typeof real>((resolve) => {
          releaseClaim = () => resolve(real);
        });
      };

      let handlerFinished = false;
      dk.handle("task", async () => {
        handlerFinished = true;
      });

      await dk.schedule("task", { key: "k:1", at: new Date() });
      await dk.start();

      // Let the poll timer fire. poll() is now parked on the
      // intercepted claimDueJobs.
      await vi.advanceTimersByTimeAsync(60);
      expect(releaseClaim).not.toBeNull();

      // Stop during the awaited claimDueJobs. With the old code
      // `inFlight` is 0 so drain would return immediately.
      let stopResolved = false;
      const stopPromise = dk.stop({ drainMs: 5_000 }).then(() => {
        stopResolved = true;
      });
      await vi.advanceTimersByTimeAsync(100);
      expect(stopResolved).toBe(false);
      expect(handlerFinished).toBe(false);

      // Release the captured claim result. poll() will increment
      // inFlight and dispatch the handler; drain must keep waiting
      // until the handler finishes.
      releaseClaim!();

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

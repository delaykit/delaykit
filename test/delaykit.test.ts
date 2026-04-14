/**
 * DelayKit API-level tests.
 *
 * Tests that are specific to the DelayKit class API surface:
 * - handler registration
 * - input validation
 * - scheduler materialization (SpyScheduler)
 * - concurrent insert retry (MemoryStore race simulation)
 *
 * Behavioral execution tests (fire, cancel, replace, retry, debounce, throttle)
 * live in the scheduler contract suite and pattern-specific test files.
 */

import { randomUUID as crypto_randomUUID } from "node:crypto";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { Job } from "../src/types.js";

const crypto = { randomUUID: crypto_randomUUID };

function createKit(options?: { interval?: number; maxConcurrent?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({
    interval: options?.interval ?? 50,
    maxConcurrent: options?.maxConcurrent,
  });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("DelayKit", () => {
  let dk: DelayKit;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  describe("handle", () => {
    it("registers a handler", () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
    });

    it("throws on duplicate handler registration", () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
      expect(() => dk.handle("test", async () => {})).toThrow(
        'Handler "test" is already registered'
      );
    });
  });

  describe("schedule input validation", () => {
    it("requires a registered handler", async () => {
      ({ dk } = createKit());
      await expect(
        dk.schedule("nonexistent", { key: "test:1", delay: "5s" })
      ).rejects.toThrow('No handler registered for "nonexistent"');
    });

    it("requires a key", async () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
      await expect(
        dk.schedule("test", { key: "", delay: "5s" })
      ).rejects.toThrow("Key is required");
    });

    it("requires delay or at", async () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
      await expect(
        dk.schedule("test", { key: "test:1" } as any)
      ).rejects.toThrow('Either "delay"');
    });

    it("rejects both delay and at", async () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
      await expect(
        dk.schedule("test", {
          key: "test:1",
          delay: "5s",
          at: new Date(),
        } as any)
      ).rejects.toThrow('Provide either "delay" or "at"');
    });

    it("schedules with absolute time", async () => {
      ({ dk } = createKit());
      dk.handle("test", async () => {});
      const future = new Date(Date.now() + 5_000);
      const { job } = await dk.schedule("test", { key: "test:1", at: future });
      expect(job.scheduledFor.getTime()).toBe(future.getTime());
    });
  });

  describe("concurrent insert retry", () => {
    function createKitWithRace() {
      const store = new MemoryStore();
      const scheduler = new PollingScheduler({ interval: 50 });
      const dk = new DelayKit({ store, scheduler });

      function simulateRace(winnerJob: Omit<Job, "createdAt">) {
        let firstLookup = true;
        const origGetActive = store.getActiveJobByKey.bind(store);
        store.getActiveJobByKey = async (handler: string, key: string) => {
          if (firstLookup && key === winnerJob.key) {
            firstLookup = false;
            return null;
          }
          return origGetActive(handler, key);
        };
        store.createJob(winnerJob);
      }

      return { dk, store, scheduler, simulateRace };
    }

    it("retries and applies replace semantics on concurrent insert", async () => {
      const { dk: kit, simulateRace } = createKitWithRace();
      dk = kit;
      dk.handle("h", async () => {});

      simulateRace({
        id: crypto.randomUUID(),
        kind: "once", handler: "h", key: "race:1", version: 1,
        claimedVersion: null, status: "pending",
        scheduledFor: new Date(Date.now() + 10_000),
        startedAt: null, completedAt: null, attempt: 0, maxAttempts: 1,
        schedulerRef: null, lastError: null,
        firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
      });

      const result = await dk.schedule("h", {
        key: "race:1", delay: "20s", onDuplicate: "replace",
      });
      expect(result.created).toBe(true);
    });

    it("retries and rejects pattern collision on concurrent insert", async () => {
      const { dk: kit, simulateRace } = createKitWithRace();
      dk = kit;
      dk.handle("save", async () => {});

      simulateRace({
        id: crypto.randomUUID(),
        kind: "debounce", handler: "save", key: "race:3", version: 1,
        claimedVersion: null, status: "pending",
        scheduledFor: new Date(Date.now() + 10_000),
        startedAt: null, completedAt: null, attempt: 0, maxAttempts: 1,
        schedulerRef: null, lastError: null,
        firstAt: new Date(), lastAt: new Date(), waitMs: 500, maxWaitMs: null,
      });

      await expect(
        dk.schedule("save", { key: "race:3", delay: "1h" })
      ).rejects.toThrow("pattern is active");
    });
  });

  describe("scheduler materialization", () => {
    class SpyScheduler {
      calls: Array<{ id: string; version: number; at: Date; handler: string }> = [];
      async schedule(req: { id: string; version: number; at: Date; handler: string }) {
        this.calls.push({ id: req.id, version: req.version, at: req.at, handler: req.handler });
        return `ref-${this.calls.length}`;
      }
      async cancel() {}
      async start() {}
      async stop() {}
    }

    it("schedule() calls scheduler.schedule before insert", async () => {
      const store = new MemoryStore();
      const spy = new SpyScheduler();
      dk = new DelayKit({ store, scheduler: spy });
      dk.handle("test", async () => {});

      const { job } = await dk.schedule("test", { key: "s:1", delay: "5s" });
      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].id).toBe(job.id);
      expect(spy.calls[0].version).toBe(1);
    });

    it("debounce calls scheduler.schedule for new window only", async () => {
      const store = new MemoryStore();
      const spy = new SpyScheduler();
      dk = new DelayKit({ store, scheduler: spy });
      dk.handle("save", async () => {});

      await dk.debounce("save", { key: "d:1", wait: "500ms" });
      expect(spy.calls).toHaveLength(1);

      await dk.debounce("save", { key: "d:1", wait: "500ms" });
      expect(spy.calls).toHaveLength(1); // existing window — no new call
    });

    it("throttle calls scheduler.schedule for new window only", async () => {
      const store = new MemoryStore();
      const spy = new SpyScheduler();
      dk = new DelayKit({ store, scheduler: spy });
      dk.handle("notify", async () => {});

      await dk.throttle("notify", { key: "t:1", wait: "500ms" });
      expect(spy.calls).toHaveLength(1);

      await dk.throttle("notify", { key: "t:1", wait: "500ms" });
      expect(spy.calls).toHaveLength(1);
    });

    it("replace calls scheduler.schedule with new time", async () => {
      const store = new MemoryStore();
      const spy = new SpyScheduler();
      dk = new DelayKit({ store, scheduler: spy });
      dk.handle("test", async () => {});

      await dk.schedule("test", { key: "rp:1", delay: "1h" });
      expect(spy.calls).toHaveLength(1);

      await dk.schedule("test", { key: "rp:1", delay: "30m", onDuplicate: "replace" });
      expect(spy.calls).toHaveLength(2);
    });
  });

  describe("maxConcurrent", () => {
    it("caps in-flight handlers and drains excess on later polls", async () => {
      ({ dk } = createKit({ maxConcurrent: 3 }));

      let inFlight = 0;
      let peak = 0;
      const gates: Array<() => void> = [];

      dk.handle("slow", async () => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise<void>((resolve) => gates.push(resolve));
        inFlight--;
      });

      for (let i = 0; i < 10; i++) {
        await dk.schedule("slow", { key: `k:${i}`, delay: "1ms" });
      }

      await dk.start();

      await vi.advanceTimersByTimeAsync(60);
      expect(gates).toHaveLength(3);
      expect(peak).toBe(3);

      let totalReleased = 0;
      while (totalReleased < 10) {
        const wave = gates.splice(0);
        expect(wave.length).toBeLessThanOrEqual(3);
        wave.forEach((resolve) => resolve());
        totalReleased += wave.length;
        await vi.advanceTimersByTimeAsync(60);
      }

      expect(peak).toBe(3);
      expect(inFlight).toBe(0);
    });

    it("releases the slot when a handler throws", async () => {
      ({ dk } = createKit({ maxConcurrent: 2 }));

      let inFlight = 0;
      let peak = 0;
      let invocations = 0;

      dk.handle("boom", async () => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        invocations++;
        inFlight--;
        throw new Error("nope");
      });

      for (let i = 0; i < 6; i++) {
        await dk.schedule("boom", { key: `b:${i}`, delay: "1ms" });
      }

      await dk.start();

      // If the slot leaked, invocations would plateau below 6 and
      // inFlight would pin at the cap.
      for (let t = 0; t < 8; t++) {
        await vi.advanceTimersByTimeAsync(60);
      }

      expect(peak).toBeLessThanOrEqual(2);
      expect(inFlight).toBe(0);
      expect(invocations).toBe(6);
    });

    it("dispatches above 100 handlers in a single tick", async () => {
      ({ dk } = createKit({ maxConcurrent: 150 }));

      const gates: Array<() => void> = [];
      dk.handle("slow", async () => {
        await new Promise<void>((resolve) => gates.push(resolve));
      });

      for (let i = 0; i < 150; i++) {
        await dk.schedule("slow", { key: `k:${i}`, delay: "1ms" });
      }

      await dk.start();
      await vi.advanceTimersByTimeAsync(60);
      expect(gates).toHaveLength(150);

      gates.forEach((resolve) => resolve());
      await vi.advanceTimersByTimeAsync(100);
    });

    it("treats throw undefined as a handler failure", async () => {
      ({ dk } = createKit({ maxConcurrent: 1 }));

      let invocations = 0;
      dk.handle("rejector", async () => {
        invocations++;
        // eslint-disable-next-line no-throw-literal
        throw undefined;
      });

      await dk.schedule("rejector", { key: "r:1", delay: "1ms" });
      await dk.start();

      await vi.advanceTimersByTimeAsync(150);

      expect(invocations).toBe(1);
      const active = await dk.getJobByKey("rejector", "r:1");
      expect(active).toBeNull();
    });

    it("awaits an uncooperative handler past its timeout before freeing the slot", async () => {
      ({ dk } = createKit({ maxConcurrent: 1 }));

      let firstFinished = false;
      let secondStarted = false;

      dk.handle("uncooperative", {
        handler: async () => {
          // Ignores ctx.signal entirely.
          await new Promise<void>((resolve) => setTimeout(resolve, 500));
          firstFinished = true;
        },
        timeout: "100ms",
      });

      dk.handle("waiting", async () => {
        secondStarted = true;
      });

      await dk.schedule("uncooperative", { key: "u:1", delay: "1ms" });
      await dk.schedule("waiting", { key: "w:1", delay: "1ms" });

      await dk.start();

      await vi.advanceTimersByTimeAsync(60);
      expect(secondStarted).toBe(false);

      // Past the 100ms timeout but within the handler's 500ms sleep:
      // the slot must stay held.
      await vi.advanceTimersByTimeAsync(150);
      expect(firstFinished).toBe(false);
      expect(secondStarted).toBe(false);

      // Past the handler's sleep: slot frees, next poll picks up "waiting".
      await vi.advanceTimersByTimeAsync(500);
      expect(firstFinished).toBe(true);
      await vi.advanceTimersByTimeAsync(60);
      expect(secondStarted).toBe(true);

      const firstJob = await dk.getJobByKey("uncooperative", "u:1");
      expect(firstJob).toBeNull();
    });
  });

  describe("retry maxDelay default", () => {
    it("caps exponential backoff at 1h when maxDelay is unset", async () => {
      ({ dk } = createKit());
      dk.handle("flaky", {
        handler: async () => {},
        retry: { attempts: 5, backoff: "exponential", initialDelay: "1s" },
      });
      const { job } = await dk.schedule("flaky", { key: "k", delay: "1h" });
      expect(job.retryConfig?.maxDelayMs).toBe(60 * 60 * 1000);
    });

    it("does not cap fixed backoff when maxDelay is unset", async () => {
      ({ dk } = createKit());
      dk.handle("spaced", {
        handler: async () => {},
        retry: { attempts: 3, backoff: "fixed", initialDelay: "2h" },
      });
      const { job } = await dk.schedule("spaced", { key: "k", delay: "1h" });
      expect(job.retryConfig?.maxDelayMs).toBe(Infinity);
    });

    it("does not cap linear backoff when maxDelay is unset", async () => {
      ({ dk } = createKit());
      dk.handle("growing", {
        handler: async () => {},
        retry: { attempts: 10, backoff: "linear", initialDelay: "30m" },
      });
      const { job } = await dk.schedule("growing", { key: "k", delay: "1h" });
      expect(job.retryConfig?.maxDelayMs).toBe(Infinity);
    });

    it("honors an explicit maxDelay override regardless of backoff", async () => {
      ({ dk } = createKit());
      dk.handle("flaky", {
        handler: async () => {},
        retry: {
          attempts: 5,
          backoff: "exponential",
          initialDelay: "1s",
          maxDelay: "30m",
        },
      });
      const { job } = await dk.schedule("flaky", { key: "k", delay: "1h" });
      expect(job.retryConfig?.maxDelayMs).toBe(30 * 60 * 1000);
    });
  });
});

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { ExternalSchedulerHarness } from "./helpers/external-scheduler-harness.js";

function makeKit(opts?: { interval?: number }) {
  return new DelayKit({
    store: new MemoryStore(),
    scheduler: new PollingScheduler({ interval: opts?.interval ?? 1_000 }),
  });
}

function makeExternalKit() {
  const harness = new ExternalSchedulerHarness();
  const dk = new DelayKit({ store: new MemoryStore(), scheduler: harness });
  return { dk, harness };
}

describe("DelayKit lifecycle guards", () => {
  describe("schedule entry points reject after stop", () => {
    it("schedule() throws after stop", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      await dk.stop({ drainMs: 0 });

      await expect(
        dk.schedule("task", { key: "k", delay: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
    });

    it("debounce() throws after stop", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      await dk.stop({ drainMs: 0 });

      await expect(
        dk.debounce("task", { key: "k", wait: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
    });

    it("throttle() throws after stop", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      await dk.stop({ drainMs: 0 });

      await expect(
        dk.throttle("task", { key: "k", wait: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
    });

    it("schedule() throws while stopping", async () => {
      vi.useFakeTimers();
      try {
        const dk = makeKit({ interval: 50 });

        let releaseHandler: (() => void) | null = null;
        const handlerGate = new Promise<void>((resolve) => {
          releaseHandler = resolve;
        });

        dk.handle("slow", async () => {
          await handlerGate;
        });
        await dk.schedule("slow", { key: "k:1", at: new Date() });
        await dk.start();

        // Handler enters.
        await vi.advanceTimersByTimeAsync(60);

        // Begin drain — state flips to "stopping" before the handler resolves.
        const stopPromise = dk.stop({ drainMs: 5_000 });

        // Yield microtasks so stop's state transition lands.
        await Promise.resolve();

        await expect(
          dk.schedule("slow", { key: "k:2", delay: "1s" }),
        ).rejects.toThrow(/DelayKit is stopping/);

        // Unblock handler and let drain finish.
        releaseHandler!();
        await vi.advanceTimersByTimeAsync(100);
        await stopPromise;
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("start/poll/createHandler reject after stop", () => {
    it("start() throws after stop", async () => {
      const dk = makeKit();
      await dk.start();
      await dk.stop({ drainMs: 0 });

      await expect(dk.start()).rejects.toThrow(/Cannot start/);
    });

    it("poll() throws after stop", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      await dk.stop({ drainMs: 0 });

      await expect(dk.poll()).rejects.toThrow(/DelayKit has stopped/);
    });

    it("createHandler() throws after stop", async () => {
      const { dk } = makeExternalKit();
      dk.handle("task", async () => {});
      // createHandler starts the kit; then stop + retry should throw.
      dk.createHandler();
      await dk.stop({ drainMs: 0 });

      expect(() => dk.createHandler()).toThrow(/DelayKit has stopped/);
    });

    it("handle() throws after stop", async () => {
      const dk = makeKit();
      await dk.start();
      await dk.stop({ drainMs: 0 });

      expect(() => dk.handle("new", async () => {})).toThrow(
        /Cannot register handler/,
      );
    });
  });

  describe("stop() on unstarted kit is a no-op (defensive-finally pattern)", () => {
    it("stop() before start() leaves the kit usable", async () => {
      const dk = makeKit();

      // Defensive finally — stop() with no start() should just return.
      await dk.stop();

      // Kit is still usable.
      dk.handle("task", async () => {});
      await dk.start();
      await dk.schedule("task", { key: "k", delay: "1s" });
      await dk.stop({ drainMs: 0 });
    });
  });

  describe("cancel/unschedule remain allowed during shutdown", () => {
    it("cancel() works after stop (cleanup)", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      const { job } = await dk.schedule("task", {
        key: "k",
        delay: "1h",
      });
      await dk.stop({ drainMs: 0 });

      // No throw — cancel is a cleanup operation.
      await expect(dk.cancel(job.id)).resolves.toBe(true);
    });

    it("unschedule() works after stop (cleanup)", async () => {
      const dk = makeKit();
      dk.handle("task", async () => {});
      await dk.start();
      await dk.schedule("task", { key: "k", delay: "1h" });
      await dk.stop({ drainMs: 0 });

      await expect(dk.unschedule("task", "k")).resolves.toBe(true);
    });
  });

  describe("createHandler webhook function returns 500 during shutdown", () => {
    it("returned webhook fn returns 500 after stop", async () => {
      const { dk, harness } = makeExternalKit();
      dk.handle("task", async () => {});

      const webhook = dk.createHandler();
      harness.setHandler(webhook);

      await dk.schedule("task", { key: "k:1", at: new Date() });
      const hookRef = harness.activeHooks()[0]!.ref;

      await dk.stop({ drainMs: 0 });

      const res = await harness.deliver(hookRef);
      expect(res.status).toBe(500);
      const body = await res.json();
      expect(body).toMatchObject({ status: "retry" });
    });
  });

  describe("stop() is terminal", () => {
    it("propagates scheduler.stop failure and closes the kit", async () => {
      const { dk, harness } = makeExternalKit();
      dk.handle("task", async () => {});
      await dk.start();

      const stopSpy = vi.spyOn(harness, "stop");
      stopSpy.mockRejectedValueOnce(new Error("cleanup failed"));

      await expect(dk.stop({ drainMs: 0 })).rejects.toThrow("cleanup failed");

      // Kit is closed after failure — scheduling rejects, subsequent
      // stop() calls no-op. Recovery is "instantiate a new DelayKit".
      await expect(
        dk.schedule("task", { key: "k", delay: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
      await expect(dk.stop({ drainMs: 0 })).resolves.toBeUndefined();
    });

    it("concurrent stop() calls share the same outcome", async () => {
      const { dk, harness } = makeExternalKit();
      dk.handle("task", async () => {});
      await dk.start();

      let release: (() => void) | null = null;
      const stopSpy = vi.spyOn(harness, "stop");
      stopSpy.mockImplementationOnce(
        () => new Promise<void>((resolve) => {
          release = resolve;
        }),
      );

      const first = dk.stop({ drainMs: 0 });
      const second = dk.stop({ drainMs: 0 });

      // Only one scheduler.stop() call regardless of how many stop()
      // callers there are.
      expect(stopSpy).toHaveBeenCalledTimes(1);

      release!();
      await Promise.all([first, second]);
    });
  });

  describe("cross-path parity", () => {
    it("schedule-after-stop throws with PosthookScheduler-shaped scheduler too", async () => {
      // The guard lives in DelayKit, not the scheduler. Prove it fires
      // regardless of which scheduler is wired up.
      const { dk, harness } = makeExternalKit();
      dk.handle("task", async () => {});

      const webhook = dk.createHandler();
      harness.setHandler(webhook);

      await dk.stop({ drainMs: 0 });

      await expect(
        dk.schedule("task", { key: "k", delay: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
      await expect(
        dk.debounce("task", { key: "k", wait: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
      await expect(
        dk.throttle("task", { key: "k", wait: "1s" }),
      ).rejects.toThrow(/DelayKit has stopped/);
    });
  });
});

describe("stop() default drainMs", () => {
  let errorSpy: ReturnType<typeof vi.spyOn>;
  let warnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
    warnSpy.mockRestore();
    vi.useRealTimers();
  });

  it("waits for in-flight handlers when no drainMs is passed", async () => {
    const dk = makeKit({ interval: 50 });

    let release: (() => void) | null = null;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let finished = false;

    dk.handle("slow", {
      handler: async () => {
        await gate;
        finished = true;
      },
      timeout: "10s",
    });

    await dk.schedule("slow", { key: "k", at: new Date() });
    await dk.start();
    await vi.advanceTimersByTimeAsync(60);

    let stopResolved = false;
    const stopPromise = dk.stop().then(() => {
      stopResolved = true;
    });

    // Default drain is handler timeout (10s) + grace (5s) = 15s.
    // With no resolution yet, stop must still be waiting.
    await vi.advanceTimersByTimeAsync(500);
    expect(stopResolved).toBe(false);
    expect(finished).toBe(false);

    // Handler resolves → drain's busy() flips and stop returns.
    release!();
    await vi.advanceTimersByTimeAsync(100);
    await stopPromise;
    expect(finished).toBe(true);
    expect(stopResolved).toBe(true);
  });

  it("drainMs: 0 opts out of the default drain", async () => {
    const dk = makeKit({ interval: 50 });

    dk.handle("forever", {
      handler: async () => {
        await new Promise<void>(() => {});
      },
      timeout: "5m",
    });

    await dk.schedule("forever", { key: "k", at: new Date() });
    await dk.start();
    await vi.advanceTimersByTimeAsync(60);

    // drainMs: 0 must return immediately even with a stuck handler and
    // a 5m handler timeout that would otherwise derive a long default.
    const stopPromise = dk.stop({ drainMs: 0 });
    await vi.advanceTimersByTimeAsync(10);
    await stopPromise;
  });

  it("default drain uses the max registered handler timeout", async () => {
    const dk = makeKit({ interval: 50 });

    // Short timeout + long timeout → drain should bound to the long one.
    dk.handle("short", { handler: async () => {}, timeout: "1s" });
    dk.handle("long", {
      handler: async () => {
        await new Promise<void>(() => {});
      },
      timeout: "2m",
    });

    await dk.schedule("long", { key: "k", at: new Date() });
    await dk.start();
    await vi.advanceTimersByTimeAsync(60);

    let stopResolved = false;
    const stopPromise = dk.stop().then(() => {
      stopResolved = true;
    });

    // After 1s + grace (well past "short"'s bound) the drain must still
    // be active — the 2m handler's bound dominates.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(stopResolved).toBe(false);

    // Advance past 2m + 5s = 125s total (already advanced 10s).
    await vi.advanceTimersByTimeAsync(120_000);
    await stopPromise;
    expect(warnSpy).toHaveBeenCalled();
  });
});

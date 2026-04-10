/**
 * Debounce behavioral invariant tests.
 *
 * Tests assert on user-visible outcomes (handler fires, doesn't fire,
 * errors thrown, key reusable) — not internal row fields.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";

function createKit(options?: { interval?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: options?.interval ?? 50 });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("debounce", () => {
  let dk: DelayKit;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  // --- core: fires once after settling ---

  describe("settlement", () => {
    it("fires handler once after activity settles", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);

      expect(received).toHaveBeenCalledOnce();
      expect(received).toHaveBeenCalledWith("doc:1");
    });

    it("does not fire before wait elapses", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async () => { received(); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).not.toHaveBeenCalled();
    });

    it("resets wait timer on each new event", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async () => { received(); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      // Event at 300ms — resets the timer
      await vi.advanceTimersByTimeAsync(300);
      await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      // 500ms from first event — should NOT have fired (timer reset)
      await vi.advanceTimersByTimeAsync(200);
      expect(received).not.toHaveBeenCalled();

      // 500ms from second event — should fire
      await vi.advanceTimersByTimeAsync(600);
      expect(received).toHaveBeenCalledOnce();
    });

    it("key is reusable after handler completes", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "300ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).toHaveBeenCalledTimes(1);

      // Same key again — should work
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).toHaveBeenCalledTimes(2);
    });
  });

  // --- return value ---

  describe("return value", () => {
    it("returns settlesAt for a new window", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const before = Date.now();
      const result = await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      const after = Date.now();

      expect(result.settlesAt).toBeInstanceOf(Date);
      expect(result.settlesAt.getTime()).toBeGreaterThanOrEqual(before + 500);
      expect(result.settlesAt.getTime()).toBeLessThanOrEqual(after + 500);
    });

    it("returns a later settlesAt on each subsequent call", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const first = await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(200);
      const second = await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(200);
      const third = await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      expect(second.settlesAt.getTime()).toBeGreaterThan(first.settlesAt.getTime());
      expect(third.settlesAt.getTime()).toBeGreaterThan(second.settlesAt.getTime());

      // Each settlesAt should be exactly waitMs after its call time
      expect(second.settlesAt.getTime() - first.settlesAt.getTime()).toBe(200);
      expect(third.settlesAt.getTime() - second.settlesAt.getTime()).toBe(200);
    });

    it("clamps settlesAt to maxWait deadline on a new window", async () => {
      // For a new window, firstAt = lastAt = now, so the clamp is
      // min(now + waitMs, now + maxWaitMs).
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const before = Date.now();
      const result = await dk.debounce("save", {
        key: "doc:1",
        wait: "500ms",
        maxWait: "200ms",
      });

      // settlesAt is min(wait, maxWait) from now, not wait
      expect(result.settlesAt.getTime()).toBe(before + 200);
    });

    it("clamps settlesAt to maxWait deadline as the burst gets long", async () => {
      // Build a continuous burst. As lastAt + waitMs creeps past
      // firstAt + maxWaitMs, the maxWait deadline should win.
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const t0 = Date.now();
      const first = await dk.debounce("save", {
        key: "doc:1",
        wait: "500ms",
        maxWait: "1000ms",
      });
      // First call: t0 + min(500, 1000) = t0 + 500
      expect(first.settlesAt.getTime()).toBe(t0 + 500);

      // Advance 400ms, debounce again. lastAt = t0+400, firstAt = t0.
      // trailing = t0+400+500 = t0+900. deadline = t0+1000. min = t0+900.
      await vi.advanceTimersByTimeAsync(400);
      const second = await dk.debounce("save", {
        key: "doc:1",
        wait: "500ms",
        maxWait: "1000ms",
      });
      expect(second.settlesAt.getTime()).toBe(t0 + 900);

      // Advance another 300ms. lastAt = t0+700.
      // trailing = t0+700+500 = t0+1200. deadline = t0+1000. min = t0+1000.
      // The maxWait deadline wins now.
      await vi.advanceTimersByTimeAsync(300);
      const third = await dk.debounce("save", {
        key: "doc:1",
        wait: "500ms",
        maxWait: "1000ms",
      });
      expect(third.settlesAt.getTime()).toBe(t0 + 1000);
    });
  });

  // --- maxWait ---

  describe("maxWait", () => {
    it("forces execution when maxWait exceeded even if not settled", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async () => { received(); });
      await dk.start();

      // Continuous activity with maxWait
      await dk.debounce("save", { key: "doc:1", wait: "500ms", maxWait: "1s" });
      await vi.advanceTimersByTimeAsync(400);
      await dk.debounce("save", { key: "doc:1", wait: "500ms", maxWait: "1s" });
      await vi.advanceTimersByTimeAsync(400);
      await dk.debounce("save", { key: "doc:1", wait: "500ms", maxWait: "1s" });

      // Not settled yet, but maxWait (1s from first event) forces execution
      await vi.advanceTimersByTimeAsync(300);
      expect(received).toHaveBeenCalledOnce();
    });
  });

  // --- event during execution ---

  describe("event during execution", () => {
    it("fires again for events that arrive during handler execution", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("save", async () => {
        callCount++;
        if (callCount === 1) {
          await new Promise((r) => setTimeout(r, 200));
        }
      });

      await dk.start();
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });

      // Handler starts executing
      await vi.advanceTimersByTimeAsync(350);

      // New event while handler is running
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });

      // Let first execution complete + second window settle
      await vi.advanceTimersByTimeAsync(250);
      await vi.advanceTimersByTimeAsync(350);

      expect(callCount).toBe(2);
    });
  });

  // --- cancel ---

  describe("cancel", () => {
    it("prevents handler from firing", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async () => { received(); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });

      const cancelled = await dk.unschedule("save", "doc:1");
      expect(cancelled).toBe(true);

      await vi.advanceTimersByTimeAsync(600);
      expect(received).not.toHaveBeenCalled();
    });

    it("returns false for idle key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      expect(await dk.unschedule("save", "doc:1")).toBe(false);
    });

    it("allows rescheduling after cancel", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async () => { received(); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "300ms" });
      await dk.unschedule("save", "doc:1");

      // Re-debounce same key
      await dk.debounce("save", { key: "doc:1", wait: "300ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).toHaveBeenCalledOnce();
    });
  });

  // --- config validation ---

  describe("config", () => {
    it("rejects mismatched wait on same key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await expect(
        dk.debounce("save", { key: "doc:1", wait: "1s" })
      ).rejects.toThrow();
    });

    it("requires key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});
      await expect(
        dk.debounce("save", { key: "", wait: "500ms" })
      ).rejects.toThrow("Key is required");
    });

    it("requires wait", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});
      await expect(
        dk.debounce("save", { key: "doc:1", wait: "" })
      ).rejects.toThrow("Wait is required");
    });

    it("requires registered handler", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      await expect(
        dk.debounce("nope", { key: "doc:1", wait: "500ms" })
      ).rejects.toThrow('No handler registered');
    });
  });

  // --- independent keys ---

  describe("independent keys", () => {
    it("different keys settle independently", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await dk.debounce("save", { key: "doc:2", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(600);

      expect(received).toHaveBeenCalledTimes(2);
      expect(received).toHaveBeenCalledWith("doc:1");
      expect(received).toHaveBeenCalledWith("doc:2");
    });
  });

  // --- handler context ---

  describe("handler context", () => {
    it("receives the user business key", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("save", async ({ key }) => { received(key); });
      await dk.start();

      await dk.debounce("save", { key: "doc:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);

      expect(received).toHaveBeenCalledWith("doc:1");
    });
  });
});

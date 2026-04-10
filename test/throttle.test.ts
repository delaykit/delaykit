/**
 * Throttle behavioral invariant tests.
 *
 * Tests assert on user-visible outcomes — not internal row fields.
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

describe("throttle", () => {
  let dk: DelayKit;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  // --- core: fires once per window ---

  describe("window", () => {
    it("fires once at end of window regardless of event count", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async ({ key }) => { received(key); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(100);
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(100);
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(400);

      expect(received).toHaveBeenCalledOnce();
      expect(received).toHaveBeenCalledWith("proj:1");
    });

    it("does not fire before window elapses", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async () => { received(); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(400);
      expect(received).not.toHaveBeenCalled();
    });

    it("new events do not extend the window", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async () => { received(); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(400);
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });

      // Should still fire at ~500ms from first event, not 500ms from second
      await vi.advanceTimersByTimeAsync(200);
      expect(received).toHaveBeenCalledOnce();
    });

    it("starts fresh window on next event after completion", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async ({ key }) => { received(key); });
      await dk.start();

      // First window
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);
      expect(received).toHaveBeenCalledTimes(1);

      // Second window
      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);
      expect(received).toHaveBeenCalledTimes(2);
    });
  });

  // --- event during execution ---

  describe("event during execution", () => {
    it("fires again for events that arrive during handler execution", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("notify", async () => {
        callCount++;
        if (callCount === 1) {
          await new Promise((r) => setTimeout(r, 200));
        }
      });

      await dk.start();
      await dk.throttle("notify", { key: "proj:1", wait: "300ms" });

      // Handler starts executing
      await vi.advanceTimersByTimeAsync(350);

      // New event during execution
      await dk.throttle("notify", { key: "proj:1", wait: "300ms" });

      // Let execution complete + second window
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
      dk.handle("notify", async () => { received(); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });

      const cancelled = await dk.unschedule("notify", "proj:1");
      expect(cancelled).toBe(true);

      await vi.advanceTimersByTimeAsync(600);
      expect(received).not.toHaveBeenCalled();
    });

    it("returns false for idle key", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("notify", async () => {});

      expect(await dk.unschedule("notify", "proj:1")).toBe(false);
    });
  });

  // --- independent keys ---

  describe("independent keys", () => {
    it("different keys have independent windows", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async ({ key }) => { received(key); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await dk.throttle("notify", { key: "proj:2", wait: "500ms" });

      await vi.advanceTimersByTimeAsync(600);

      expect(received).toHaveBeenCalledTimes(2);
      expect(received).toHaveBeenCalledWith("proj:1");
      expect(received).toHaveBeenCalledWith("proj:2");
    });
  });

  // --- handler context ---

  describe("handler context", () => {
    it("receives the user business key", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      const received = vi.fn();
      dk.handle("notify", async ({ key }) => { received(key); });
      await dk.start();

      await dk.throttle("notify", { key: "proj:1", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);

      expect(received).toHaveBeenCalledWith("proj:1");
    });
  });
});

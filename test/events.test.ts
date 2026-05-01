/**
 * Lifecycle event integration tests.
 *
 * Collects events via dk.on() into arrays and asserts after time advancement.
 * Events are synchronous, so they're in the array before the await resolves.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { JobEvent, JobScheduledEvent, JobStartedEvent, JobCompletedEvent, JobFailedEvent, JobRetryingEvent, JobCancelledEvent, JobStalledEvent } from "../src/types.js";

function createKit(options?: { interval?: number }) {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: options?.interval ?? 50, stalledCheckInterval: 200 });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("lifecycle events", () => {
  let dk: DelayKit;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  // =========================================================================
  // job:scheduled
  // =========================================================================

  describe("job:scheduled", () => {
    it("emits on schedule()", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobScheduledEvent[] = [];
      dk.on("job:scheduled", (e) => events.push(e));

      await dk.schedule("task", { key: "s:1", delay: "5s" });

      expect(events).toHaveLength(1);
      expect(events[0].job.key).toBe("s:1");
      expect(events[0].job.status).toBe("pending");
      expect(events[0].timestamp).toBeInstanceOf(Date);
    });

    it("emits on debounce() first window only", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const events: JobScheduledEvent[] = [];
      dk.on("job:scheduled", (e) => events.push(e));

      await dk.debounce("save", { key: "d:1", wait: "500ms" });
      await dk.debounce("save", { key: "d:1", wait: "500ms" }); // bump, not new window

      expect(events).toHaveLength(1);
      expect(events[0].job.kind).toBe("debounce");
    });

    it("emits on throttle() first window only", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("notify", async () => {});

      const events: JobScheduledEvent[] = [];
      dk.on("job:scheduled", (e) => events.push(e));

      await dk.throttle("notify", { key: "t:1", wait: "500ms" });
      await dk.throttle("notify", { key: "t:1", wait: "500ms" });

      expect(events).toHaveLength(1);
      expect(events[0].job.kind).toBe("throttle");
    });

    it("does NOT emit on skip (duplicate key)", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobScheduledEvent[] = [];
      dk.on("job:scheduled", (e) => events.push(e));

      await dk.schedule("task", { key: "dup:1", delay: "5s" });
      await dk.schedule("task", { key: "dup:1", delay: "10s" }); // skip

      expect(events).toHaveLength(1);
    });

    it("emits on replace", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobScheduledEvent[] = [];
      dk.on("job:scheduled", (e) => events.push(e));

      await dk.schedule("task", { key: "rep:1", delay: "5s" });
      await dk.schedule("task", { key: "rep:1", delay: "10s", onDuplicate: "replace" });

      expect(events).toHaveLength(2);
      expect(events[1].job.version).toBe(2);
    });
  });

  // =========================================================================
  // job:started
  // =========================================================================

  describe("job:started", () => {
    it("emits when handler begins executing", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobStartedEvent[] = [];
      dk.on("job:started", (e) => events.push(e));

      await dk.start();
      await dk.schedule("task", { key: "st:1", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      expect(events).toHaveLength(1);
      expect(events[0].job.key).toBe("st:1");
      expect(events[0].attempt).toBe(0);
    });

    it("listener mutation cannot corrupt completion bookkeeping", async () => {
      const { dk: kit, store } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      dk.on("job:started", (e) => {
        e.job.version = 999;
        e.job.scheduledFor.setTime(0);
      });

      await dk.start();
      const { job } = await dk.schedule("task", { key: "st:isolated-listener", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      const row = await store.getJob(job.id);
      expect(row?.status).toBe("completed");
    });

    it("handler context mutation cannot corrupt completion bookkeeping", async () => {
      const { dk: kit, store } = createKit();
      dk = kit;
      dk.handle("task", async (ctx) => {
        ctx.job.version = 999;
        ctx.job.scheduledFor.setTime(0);
      });

      await dk.start();
      const { job } = await dk.schedule("task", { key: "st:isolated-context", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      const row = await store.getJob(job.id);
      expect(row?.status).toBe("completed");
    });

    it("includes correct attempt number on retry", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          if (callCount === 1) throw new Error("fail");
        },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "1s" },
      });

      const events: JobStartedEvent[] = [];
      dk.on("job:started", (e) => events.push(e));

      await dk.start();
      await dk.schedule("flaky", { key: "st:2", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100); // first attempt
      await vi.advanceTimersByTimeAsync(1_100); // retry

      expect(events).toHaveLength(2);
      expect(events[0].attempt).toBe(0);
      expect(events[1].attempt).toBe(1);
    });
  });

  // =========================================================================
  // job:completed
  // =========================================================================

  describe("job:completed", () => {
    it("emits on successful once job", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobCompletedEvent[] = [];
      dk.on("job:completed", (e) => events.push(e));

      await dk.start();
      await dk.schedule("task", { key: "c:1", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      expect(events).toHaveLength(1);
      expect(events[0].job.key).toBe("c:1");
      expect(events[0].durationMs).toBeGreaterThanOrEqual(0);
    });

    it("emits on successful debounce (terminal)", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("save", async () => {});

      const events: JobCompletedEvent[] = [];
      dk.on("job:completed", (e) => events.push(e));

      await dk.start();
      await dk.debounce("save", { key: "c:2", wait: "500ms" });
      await vi.advanceTimersByTimeAsync(600);

      expect(events).toHaveLength(1);
      expect(events[0].job.kind).toBe("debounce");
    });
  });

  // =========================================================================
  // job:failed
  // =========================================================================

  describe("job:failed", () => {
    it("emits on terminal failure (retries exhausted)", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("doomed", {
        handler: async () => { throw new Error("always fails"); },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "1s" },
      });

      const events: JobFailedEvent[] = [];
      dk.on("job:failed", (e) => events.push(e));

      await dk.start();
      await dk.schedule("doomed", { key: "f:1", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100); // attempt 1
      await vi.advanceTimersByTimeAsync(1_100); // attempt 2, exhausted

      expect(events).toHaveLength(1);
      expect(events[0].error.message).toBe("always fails");
      expect(events[0].attempts).toBe(2);
      expect(events[0].durationMs).toBeGreaterThanOrEqual(0);
    });

    it("listener mutation cannot alter the error passed to onFailure", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let onFailureMessage: string | undefined;
      dk.handle("doomed", {
        handler: async () => { throw new Error("original"); },
        retry: {
          attempts: 1,
          backoff: "fixed",
          initialDelay: "1s",
        },
        onFailure: async ({ error }) => {
          onFailureMessage = error.message;
        },
      });

      dk.on("job:failed", (e) => {
        e.error.message = "mutated";
      });

      await dk.start();
      await dk.schedule("doomed", { key: "f:isolated-error", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      expect(onFailureMessage).toBe("original");
    });

    it("does NOT emit on non-terminal failure", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          if (callCount === 1) throw new Error("fail");
        },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "1s" },
      });

      const failed: JobFailedEvent[] = [];
      dk.on("job:failed", (e) => failed.push(e));

      await dk.start();
      await dk.schedule("flaky", { key: "f:2", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100); // fails, retries

      // First failure is NOT terminal — should not emit job:failed
      expect(failed).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(1_100); // succeeds
      expect(failed).toHaveLength(0); // still no failed event
    });
  });

  // =========================================================================
  // job:retrying
  // =========================================================================

  describe("job:retrying", () => {
    it("emits when handler fails but retries remain", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          if (callCount < 3) throw new Error("not yet");
        },
        retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
      });

      const events: JobRetryingEvent[] = [];
      dk.on("job:retrying", (e) => events.push(e));

      await dk.start();
      await dk.schedule("flaky", { key: "r:1", delay: "1s" });

      await vi.advanceTimersByTimeAsync(1_100); // attempt 0 fails
      expect(events).toHaveLength(1);
      expect(events[0].attempt).toBe(0);
      expect(events[0].nextAttempt).toBe(1);
      expect(events[0].error.message).toBe("not yet");

      await vi.advanceTimersByTimeAsync(1_100); // attempt 1 fails
      expect(events).toHaveLength(2);
      expect(events[1].attempt).toBe(1);
      expect(events[1].nextAttempt).toBe(2);
    });

    it("does NOT emit on final failure", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("doomed", {
        handler: async () => { throw new Error("always"); },
        retry: { attempts: 1, backoff: "fixed", initialDelay: "1s" },
      });

      const retrying: JobRetryingEvent[] = [];
      dk.on("job:retrying", (e) => retrying.push(e));

      await dk.start();
      await dk.schedule("doomed", { key: "r:2", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      // 1 attempt, no retries → should NOT emit retrying
      expect(retrying).toHaveLength(0);
    });
  });

  // =========================================================================
  // job:cancelled
  // =========================================================================

  describe("job:cancelled", () => {
    it("emits on cancel()", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobCancelledEvent[] = [];
      dk.on("job:cancelled", (e) => events.push(e));

      const { job } = await dk.schedule("task", { key: "can:1", delay: "5s" });
      await dk.cancel(job.id);

      expect(events).toHaveLength(1);
      expect(events[0].job.key).toBe("can:1");
      expect(events[0].job.status).toBe("cancelled");
    });

    it("emits on unschedule()", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobCancelledEvent[] = [];
      dk.on("job:cancelled", (e) => events.push(e));

      await dk.schedule("task", { key: "can:2", delay: "5s" });
      await dk.unschedule("task", "can:2");

      expect(events).toHaveLength(1);
    });

    it("does NOT emit when cancel returns false", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobCancelledEvent[] = [];
      dk.on("job:cancelled", (e) => events.push(e));

      await dk.cancel("nonexistent-id");
      expect(events).toHaveLength(0);
    });
  });

  // =========================================================================
  // job:stalled
  // =========================================================================

  describe("job:stalled", () => {
    it("emits when poll() reclaims a stalled job", async () => {
      const { dk: kit, store } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobStalledEvent[] = [];
      dk.on("job:stalled", (e) => events.push(e));

      const { randomUUID } = await import("node:crypto");
      await store.createJob({
        id: randomUUID(),
        kind: "once", handler: "task", key: "stalled:1",
        version: 1, claimedVersion: 1, status: "running",
        scheduledFor: new Date(Date.now() - 60_000),
        startedAt: new Date(Date.now() - 60_000),
        completedAt: null, attempt: 0, maxAttempts: 3,
        schedulerRef: null, lastError: null,
        firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
      });

      await dk.poll();

      expect(events).toHaveLength(1);
      expect(events[0].reclaimed).toBe(true);
      expect(events[0].stalledMs).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // Event ordering
  // =========================================================================

  describe("event ordering", () => {
    it("scheduled → started → completed for success", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const types: string[] = [];
      dk.on("job:scheduled", () => types.push("scheduled"));
      dk.on("job:started", () => types.push("started"));
      dk.on("job:completed", () => types.push("completed"));

      await dk.start();
      await dk.schedule("task", { key: "ord:1", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      expect(types).toEqual(["scheduled", "started", "completed"]);
    });

    it("scheduled → started → retrying → started → completed for retry+success", async () => {
      const { dk: kit } = createKit();
      dk = kit;

      let callCount = 0;
      dk.handle("flaky", {
        handler: async () => {
          callCount++;
          if (callCount === 1) throw new Error("fail");
        },
        retry: { attempts: 2, backoff: "fixed", initialDelay: "1s" },
      });

      const types: string[] = [];
      dk.on("job:scheduled", () => types.push("scheduled"));
      dk.on("job:started", () => types.push("started"));
      dk.on("job:retrying", () => types.push("retrying"));
      dk.on("job:completed", () => types.push("completed"));

      await dk.start();
      await dk.schedule("flaky", { key: "ord:2", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100); // fails
      await vi.advanceTimersByTimeAsync(1_100); // succeeds

      expect(types).toEqual(["scheduled", "started", "retrying", "started", "completed"]);
    });

    it("scheduled → started → failed for exhausted failure", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("doomed", {
        handler: async () => { throw new Error("always"); },
      });

      const types: string[] = [];
      dk.on("job:scheduled", () => types.push("scheduled"));
      dk.on("job:started", () => types.push("started"));
      dk.on("job:failed", () => types.push("failed"));

      await dk.start();
      await dk.schedule("doomed", { key: "ord:3", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      expect(types).toEqual(["scheduled", "started", "failed"]);
    });

    it("scheduled → cancelled", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const types: string[] = [];
      dk.on("job:scheduled", () => types.push("scheduled"));
      dk.on("job:cancelled", () => types.push("cancelled"));

      const { job } = await dk.schedule("task", { key: "ord:4", delay: "5s" });
      await dk.cancel(job.id);

      expect(types).toEqual(["scheduled", "cancelled"]);
    });
  });

  // =========================================================================
  // Listener behavior
  // =========================================================================

  describe("listener behavior", () => {
    it("unsubscribe prevents future calls", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      dk.handle("task", async () => {});

      const events: JobScheduledEvent[] = [];
      const unsub = dk.on("job:scheduled", (e) => events.push(e));

      await dk.schedule("task", { key: "unsub:1", delay: "5s" });
      expect(events).toHaveLength(1);

      unsub();
      await dk.schedule("task", { key: "unsub:2", delay: "5s" });
      expect(events).toHaveLength(1); // no second event
    });

    it("listener errors do not break the job", async () => {
      const { dk: kit } = createKit();
      dk = kit;
      vi.spyOn(console, "error").mockImplementation(() => {});

      const received = vi.fn();
      dk.handle("task", async () => { received(); });

      dk.on("job:started", () => { throw new Error("listener boom"); });

      await dk.start();
      await dk.schedule("task", { key: "err:1", delay: "1s" });
      await vi.advanceTimersByTimeAsync(1_100);

      // Handler still executed despite listener error
      expect(received).toHaveBeenCalledOnce();

      vi.restoreAllMocks();
    });
  });
});

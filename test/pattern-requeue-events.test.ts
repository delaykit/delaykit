/**
 * `job:requeued` fires when a pattern handler runs an attempt while new
 * events arrive for the same key. Without this event, operators using
 * `job:completed` / `job:retrying` / `job:failed` for metrics undercount
 * pattern outcomes whenever the handler is concurrent with its own events.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { JobRequeuedEvent, JobCompletedEvent, JobFailedEvent, JobRetryingEvent } from "../src/types.js";

function createKit() {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: 50 });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

describe("job:requeued", () => {
  let dk: DelayKit;

  beforeEach(() => vi.useFakeTimers());
  afterEach(async () => {
    if (dk) await dk.stop();
    vi.useRealTimers();
  });

  it("succeeded: handler completes while events arrive → job:requeued fires for the first window", async () => {
    const { dk: kit } = createKit();
    dk = kit;

    let runs = 0;
    dk.handle("save", async () => {
      runs++;
      // First run sleeps so a concurrent debounce can bump the version;
      // later requeued runs would also sleep but get cancelled by stop().
      await new Promise((r) => setTimeout(r, 400));
    });

    const requeued: JobRequeuedEvent[] = [];
    const completed: JobCompletedEvent[] = [];
    dk.on("job:requeued", (e) => requeued.push(e));
    dk.on("job:completed", (e) => completed.push(e));

    await dk.start();
    await dk.debounce("save", { key: "doc:1", wait: "300ms" });
    await vi.advanceTimersByTimeAsync(350); // handler mid-execution
    await dk.debounce("save", { key: "doc:1", wait: "300ms" }); // bumps version
    await vi.advanceTimersByTimeAsync(500); // first run completes; markCompleted CAS loses; requeue
    await dk.stop({ drainMs: 0 });

    expect(runs).toBeGreaterThanOrEqual(1);
    expect(completed).toHaveLength(0); // first window's markCompleted CAS lost
    expect(requeued.length).toBeGreaterThanOrEqual(1);
    const first = requeued[0];
    expect(first.outcome).toBe("succeeded");
    expect(first.error).toBeUndefined();
    expect(first.attempts).toBe(1);
    expect(first.durationMs).toBeGreaterThan(0);
    expect(first.job.status).toBe("pending");
  });

  it("failed_with_retries: pattern handler errors with retries left while events arrive", async () => {
    const { dk: kit } = createKit();
    dk = kit;

    let runs = 0;
    dk.handle("flaky", {
      handler: async () => {
        runs++;
        if (runs === 1) {
          await new Promise((r) => setTimeout(r, 400));
          throw new Error("boom");
        }
      },
      retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
    });

    const requeued: JobRequeuedEvent[] = [];
    const retrying: JobRetryingEvent[] = [];
    dk.on("job:requeued", (e) => requeued.push(e));
    dk.on("job:retrying", (e) => retrying.push(e));

    await dk.start();
    await dk.debounce("flaky", { key: "f:1", wait: "300ms" });
    await vi.advanceTimersByTimeAsync(350); // handler is sleeping
    await dk.debounce("flaky", { key: "f:1", wait: "300ms" }); // bumps version
    await vi.advanceTimersByTimeAsync(500); // first run throws; requeue happens

    expect(retrying).toHaveLength(0); // retryJob CAS lost
    expect(requeued).toHaveLength(1);
    expect(requeued[0].outcome).toBe("failed_with_retries");
    expect(requeued[0].error?.message).toBe("boom");
    expect(requeued[0].attempts).toBe(1);
  });

  it("failed_exhausted: handler fails on its last attempt while events arrive → job:requeued for the first window", async () => {
    const { dk: kit } = createKit();
    dk = kit;

    dk.handle("doomed", {
      handler: async () => {
        await new Promise((r) => setTimeout(r, 400));
        throw new Error("always");
      },
      retry: { attempts: 1, backoff: "fixed", initialDelay: "1s" },
    });

    const requeued: JobRequeuedEvent[] = [];
    dk.on("job:requeued", (e) => requeued.push(e));

    await dk.start();
    await dk.debounce("doomed", { key: "d:1", wait: "300ms" });
    await vi.advanceTimersByTimeAsync(350); // handler sleeping
    await dk.debounce("doomed", { key: "d:1", wait: "300ms" }); // bumps version
    await vi.advanceTimersByTimeAsync(500); // first run throws (exhausted); markFailed CAS loses
    await dk.stop({ drainMs: 0 });

    expect(requeued.length).toBeGreaterThanOrEqual(1);
    const first = requeued[0];
    expect(first.outcome).toBe("failed_exhausted");
    expect(first.error?.message).toBe("always");
    expect(first.attempts).toBe(1);
  });

  it("does not fire for `once` jobs (no requeue path)", async () => {
    const { dk: kit } = createKit();
    dk = kit;
    dk.handle("task", async () => {});

    const requeued: JobRequeuedEvent[] = [];
    dk.on("job:requeued", (e) => requeued.push(e));

    await dk.start();
    await dk.schedule("task", { key: "o:1", delay: "100ms" });
    await vi.advanceTimersByTimeAsync(200);

    expect(requeued).toHaveLength(0);
  });

  it("does not fire when no concurrent events arrive (markCompleted CAS wins)", async () => {
    const { dk: kit } = createKit();
    dk = kit;
    dk.handle("save", async () => {});

    const requeued: JobRequeuedEvent[] = [];
    const completed: JobCompletedEvent[] = [];
    dk.on("job:requeued", (e) => requeued.push(e));
    dk.on("job:completed", (e) => completed.push(e));

    await dk.start();
    await dk.debounce("save", { key: "quiet:1", wait: "100ms" });
    await vi.advanceTimersByTimeAsync(200);

    expect(completed).toHaveLength(1);
    expect(requeued).toHaveLength(0);
  });
});

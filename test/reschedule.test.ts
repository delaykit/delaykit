import { describe, it, expect } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import type { JobRescheduledEvent } from "../src/types.js";
import { assertJobInvariants } from "./helpers/invariants.js";

function createKit() {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: 30, stalledCheckInterval: 5_000 });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store };
}

describe("ctx.reschedule", () => {
  it("transitions running → pending with the new scheduledFor", async () => {
    const { dk, store } = createKit();
    let ranAt: number | null = null;
    dk.handle("poll", async ({ reschedule }) => {
      ranAt = Date.now();
      reschedule({ delay: "1h" });
    });

    const { job } = await dk.schedule("poll", { key: "p:1", at: new Date(Date.now() - 10) });
    const initialVersion = job.version;
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 200));
    } finally {
      await dk.stop();
    }

    expect(ranAt).not.toBeNull();
    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("pending");
    expect(after.attempt).toBe(0);
    expect(after.startedAt).toBeNull();
    expect(after.completedAt).toBeNull();
    expect(after.lastError).toBeNull();
    expect(after.failureReason).toBeNull();
    expect(after.schedulerRef).toBeNull();
    // rescheduleJob bumps version to invalidate stale snapshots.
    expect(after.version).toBe(initialVersion + 1);
    // ~1h in the future. Allow generous slack.
    const expected = ranAt! + 60 * 60 * 1000;
    expect(Math.abs(after.scheduledFor.getTime() - expected)).toBeLessThan(1_000);
    assertJobInvariants(after);
    await store.close();
  });

  it("emits job:rescheduled with scheduledFor and durationMs", async () => {
    const { dk, store } = createKit();
    const events: JobRescheduledEvent[] = [];
    dk.on("job:rescheduled", (e) => { events.push(e); });

    dk.handle("poll", async ({ reschedule }) => {
      await new Promise((r) => setTimeout(r, 30));
      reschedule({ delay: "5m" });
    });
    await dk.schedule("poll", { key: "p:event", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    expect(events).toHaveLength(1);
    expect(events[0].type).toBe("job:rescheduled");
    expect(events[0].scheduledFor).toBeInstanceOf(Date);
    expect(events[0].durationMs).toBeGreaterThanOrEqual(20);
    expect(events[0].job.status).toBe("pending");
    await store.close();
  });

  it("does not emit job:completed when reschedule is called", async () => {
    const { dk, store } = createKit();
    const completedEvents: unknown[] = [];
    dk.on("job:completed", (e) => { completedEvents.push(e); });

    dk.handle("poll", async ({ reschedule }) => {
      reschedule({ delay: "1m" });
    });
    await dk.schedule("poll", { key: "p:nc", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 200));
    } finally {
      await dk.stop();
    }

    expect(completedEvents).toHaveLength(0);
    await store.close();
  });

  it("accepts an absolute Date via at", async () => {
    const { dk, store } = createKit();
    const at = new Date(Date.now() + 60 * 60 * 1000);
    dk.handle("poll", async ({ reschedule }) => {
      reschedule({ at });
    });

    const { job } = await dk.schedule("poll", { key: "p:at", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 200));
    } finally {
      await dk.stop();
    }

    const after = (await store.getJob(job.id))!;
    expect(after.scheduledFor.getTime()).toBe(at.getTime());
    await store.close();
  });

  it("last reschedule call wins", async () => {
    const { dk, store } = createKit();
    dk.handle("poll", async ({ reschedule }) => {
      reschedule({ delay: "1s" });
      reschedule({ delay: "10s" });
      reschedule({ delay: "1h" });
    });

    const before = Date.now();
    const { job } = await dk.schedule("poll", { key: "p:last", at: new Date(before - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 200));
    } finally {
      await dk.stop();
    }

    const after = (await store.getJob(job.id))!;
    const elapsed = after.scheduledFor.getTime() - before;
    expect(elapsed).toBeGreaterThan(60 * 60 * 1000 - 1_000);
    expect(elapsed).toBeLessThan(60 * 60 * 1000 + 1_000);
    await store.close();
  });

  it("throwing from the handler discards the reschedule intent", async () => {
    const { dk, store } = createKit();
    let calls = 0;
    dk.handle("poll", async ({ reschedule }) => {
      calls++;
      reschedule({ delay: "1h" });
      throw new Error("boom");
    });
    const { job } = await dk.schedule("poll", { key: "p:throw", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    expect(calls).toBe(1);
    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("failed");
    expect(after.failureReason).toBe("handler_error");
    await store.close();
  });

  it("completes normally when handler does not call reschedule", async () => {
    const { dk, store } = createKit();
    dk.handle("poll", async () => {});
    const { job } = await dk.schedule("poll", { key: "p:done", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 200));
    } finally {
      await dk.stop();
    }

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("completed");
    await store.close();
  });

  it("supports the full poll-until-done loop", async () => {
    const { dk, store } = createKit();
    let calls = 0;
    dk.handle("poll", async ({ reschedule }) => {
      calls++;
      if (calls < 3) {
        reschedule({ delay: "10ms" });
      }
    });

    const { job } = await dk.schedule("poll", { key: "p:loop", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      // 3 iterations × ~10ms reschedule + ~30ms poll cadence = ~150ms+ to settle.
      await new Promise((r) => setTimeout(r, 400));
    } finally {
      await dk.stop();
    }

    expect(calls).toBe(3);
    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("completed");
    await store.close();
  });

  it("throws synchronously when called from a debounce handler", async () => {
    const { dk, store } = createKit();
    let caught: unknown = null;
    dk.handle("deb", async ({ reschedule }) => {
      try {
        reschedule({ delay: "1m" });
      } catch (err) {
        caught = err;
      }
    });
    await dk.debounce("deb", { key: "d:1", wait: "20ms" });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toMatch(/only supported on kind="once"/);
    await store.close();
  });

  it("throws synchronously when called from a throttle handler", async () => {
    const { dk, store } = createKit();
    let caught: unknown = null;
    dk.handle("thr", async ({ reschedule }) => {
      try {
        reschedule({ delay: "1m" });
      } catch (err) {
        caught = err;
      }
    });
    await dk.throttle("thr", { key: "t:1", wait: "20ms" });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toMatch(/only supported on kind="once"/);
    await store.close();
  });

  describe("validation", () => {
    async function run(opts: unknown): Promise<unknown> {
      const { dk, store } = createKit();
      let caught: unknown = null;
      dk.handle("poll", async ({ reschedule }) => {
        try {
          reschedule(opts as never);
        } catch (err) {
          caught = err;
        }
      });
      await dk.schedule("poll", { key: `v:${Math.random()}`, at: new Date(Date.now() - 10) });
      try {
        await dk.start();
        await new Promise((r) => setTimeout(r, 200));
      } finally {
        await dk.stop();
      }
      await store.close();
      return caught;
    }

    it("rejects calls with neither delay nor at", async () => {
      const err = (await run({})) as Error;
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toMatch(/either "delay" .* or "at"/);
    });

    it("rejects calls with both delay and at", async () => {
      const err = (await run({ delay: "1m", at: new Date(Date.now() + 60_000) })) as Error;
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toMatch(/either "delay" or "at", not both/);
    });

    it("rejects an invalid at Date", async () => {
      const err = (await run({ at: new Date("not a date") })) as Error;
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toMatch(/invalid "at" Date/);
    });

    it("rejects an at more than 10 years in the future", async () => {
      const tooFar = new Date(Date.now() + 11 * 366 * 24 * 60 * 60 * 1000);
      const err = (await run({ at: tooFar })) as Error;
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toMatch(/more than 10 years in the future/);
    });

    it("rejects an invalid delay string", async () => {
      const err = (await run({ delay: "not a duration" })) as Error;
      expect(err).toBeInstanceOf(Error);
    });

    it("rejects a delay more than 10 years in the future", async () => {
      // Eleven years of milliseconds — beyond SCHEDULE_MAX_FUTURE_MS.
      const tooLong = `${11 * 366 * 24}h`;
      const err = (await run({ delay: tooLong })) as Error;
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toMatch(/more than 10 years in the future/);
    });
  });

  it("CAS loss leaves the row in its concurrent terminal state", async () => {
    // Race: handler finishes and is about to call rescheduleJob, but
    // a concurrent cancel hits first. The reschedule's CAS must lose
    // and leave the row cancelled.
    const { dk, store } = createKit();
    dk.handle("poll", async ({ reschedule, job }) => {
      // Cancel the row while we're still "running" — simulates a
      // concurrent cancellation arriving mid-handler. The current
      // store impl marks pending rows cancelled, but the handler
      // already flipped it to running. Use a different approach:
      // reschedule, then before result-handler runs the CAS, mutate
      // the row through a sibling DelayKit instance.
      reschedule({ delay: "1m" });
      await store.markCompleted(job.id, job.version);
    });

    const { job } = await dk.schedule("poll", { key: "p:cas", at: new Date(Date.now() - 10) });
    try {
      await dk.start();
      await new Promise((r) => setTimeout(r, 250));
    } finally {
      await dk.stop();
    }

    const after = (await store.getJob(job.id))!;
    expect(after.status).toBe("completed");
    expect(after.deferAttempts).toBe(0);
    await store.close();
  });
});

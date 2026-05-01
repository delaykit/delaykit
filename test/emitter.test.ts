import { describe, it, expect, vi } from "vitest";
import { JobEventEmitter, emitJobRequeued } from "../src/emitter.js";
import type { EmitFn, Job, JobScheduledEvent, JobCompletedEvent, JobRequeuedEvent } from "../src/types.js";
import { makeJob } from "./helpers/job-factory.js";

function makeEvent(type: string, overrides?: Record<string, unknown>) {
  return {
    type,
    job: { id: "j1", key: "k:1" } as any,
    timestamp: new Date(),
    ...overrides,
  };
}

describe("JobEventEmitter", () => {
  it("on() returns unsubscribe function", () => {
    const emitter = new JobEventEmitter();
    const listener = vi.fn();
    const unsub = emitter.on("job:scheduled", listener);

    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);
    expect(listener).toHaveBeenCalledOnce();

    unsub();
    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);
    expect(listener).toHaveBeenCalledOnce(); // no second call
  });

  it("calls all registered listeners for the event type", () => {
    const emitter = new JobEventEmitter();
    const a = vi.fn();
    const b = vi.fn();
    emitter.on("job:completed", a);
    emitter.on("job:completed", b);

    emitter.emit(makeEvent("job:completed", { durationMs: 42 }) as JobCompletedEvent);
    expect(a).toHaveBeenCalledOnce();
    expect(b).toHaveBeenCalledOnce();
  });

  it("does not call listeners for other event types", () => {
    const emitter = new JobEventEmitter();
    const listener = vi.fn();
    emitter.on("job:completed", listener);

    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);
    expect(listener).not.toHaveBeenCalled();
  });

  it("catches listener errors and logs them", () => {
    const emitter = new JobEventEmitter();
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});

    emitter.on("job:scheduled", () => { throw new Error("boom"); });
    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);

    expect(spy).toHaveBeenCalledOnce();
    expect(spy.mock.calls[0][0]).toContain("job:scheduled");
    spy.mockRestore();
  });

  it("listener error does not prevent other listeners", () => {
    const emitter = new JobEventEmitter();
    vi.spyOn(console, "error").mockImplementation(() => {});
    const after = vi.fn();

    emitter.on("job:scheduled", () => { throw new Error("boom"); });
    emitter.on("job:scheduled", after);

    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);
    expect(after).toHaveBeenCalledOnce();

    vi.restoreAllMocks();
  });

  it("emit with no listeners does not throw", () => {
    const emitter = new JobEventEmitter();
    expect(() => {
      emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);
    }).not.toThrow();
  });

  it("traps async listener rejection (no unhandled rejection)", async () => {
    const emitter = new JobEventEmitter();
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});

    emitter.on("job:scheduled", (async () => {
      await Promise.resolve(); // first await succeeds
      throw new Error("async boom"); // rejection after await
    }) as any);

    emitter.emit(makeEvent("job:scheduled") as JobScheduledEvent);

    // Flush microtask queue so the async rejection is caught
    await new Promise((r) => setTimeout(r, 0));

    expect(spy).toHaveBeenCalledOnce();
    expect(spy.mock.calls[0][0]).toContain("Async event listener error");
    spy.mockRestore();
  });

  it("helper emitters isolate job payloads from listener mutation", () => {
    const scheduledFor = new Date(1_000);
    const retryConfig = {
      attempts: 3,
      backoff: "fixed" as const,
      initialDelayMs: 100,
      maxDelayMs: 1_000,
      jitter: false,
    };
    const job: Job = {
      ...makeJob({
        key: "requeue:isolated",
        scheduledFor,
        retryConfig,
      }),
      createdAt: new Date(500),
    };
    const error = new Error("original");

    let eventJob: Job | undefined;
    let eventError: Error | undefined;
    const emit: EmitFn = (event) => {
      const requeued = event as JobRequeuedEvent;
      eventJob = requeued.job;
      eventError = requeued.error;
      requeued.job.version = 999;
      requeued.job.scheduledFor.setTime(0);
      requeued.job.retryConfig!.attempts = 999;
      requeued.error!.message = "mutated";
    };

    emitJobRequeued(emit, {
      job,
      outcome: "failed_with_retries",
      error,
      attempts: 1,
      durationMs: 10,
    });

    expect(eventJob).not.toBe(job);
    expect(eventJob!.scheduledFor).not.toBe(job.scheduledFor);
    expect(eventJob!.retryConfig).not.toBe(job.retryConfig);
    expect(eventError).not.toBe(error);
    expect(job.version).toBe(1);
    expect(job.scheduledFor.getTime()).toBe(1_000);
    expect(job.retryConfig!.attempts).toBe(3);
    expect(error.message).toBe("original");
  });
});

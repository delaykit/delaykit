import { describe, it, expect, vi } from "vitest";
import { JobEventEmitter } from "../src/emitter.js";
import type { JobScheduledEvent, JobCompletedEvent } from "../src/types.js";

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
});

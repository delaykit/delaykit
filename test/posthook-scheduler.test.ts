/**
 * PosthookScheduler and createHandler tests.
 *
 * Uses mocked Posthook API calls and simulated webhook deliveries
 * to test the full PosthookScheduler flow without a real Posthook account.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PosthookScheduler } from "../src/schedulers/posthook.js";
import { createHmac } from "node:crypto";

// Mock the @posthook/node SDK
const mockSchedule = vi.fn();
const mockDelete = vi.fn();
const mockParseDelivery = vi.fn();

vi.mock("@posthook/node", () => {
  return {
    default: class MockPosthook {
      hooks = {
        schedule: mockSchedule,
        delete: mockDelete,
      };
      signatures = {
        parseDelivery: mockParseDelivery,
      };
    },
  };
});

const SIGNING_KEY = "test_signing_key_123";

function createPosthookKit() {
  const store = new MemoryStore();
  const scheduler = new PosthookScheduler({
    apiKey: "pk_test",
    signingKey: SIGNING_KEY,
    basePath: "/api/delaykit",
  });
  const dk = new DelayKit({ store, scheduler });
  return { dk, store, scheduler };
}

function makeDeliveryRequest(data: Record<string, unknown>): Request {
  const body = JSON.stringify({
    id: "hook_123",
    path: "/api/delaykit",
    data,
    postAt: new Date().toISOString(),
    postedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });

  return new Request("http://localhost/api/delaykit", {
    method: "POST",
    body,
    headers: {
      "Content-Type": "application/json",
      "Posthook-Id": "hook_123",
      "Posthook-Timestamp": String(Math.floor(Date.now() / 1000)),
      "Posthook-Signature": "v1,test",
    },
  });
}

describe("PosthookScheduler", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("schedule", () => {
    it("calls Posthook API with handler-scoped path", async () => {
      const { scheduler } = createPosthookKit();

      mockSchedule.mockResolvedValue({ id: "hook_abc" });

      const ref = await scheduler.schedule({ id: "job_123", version: 1, at: new Date("2026-06-01T12:00:00Z"), handler: "send-reminder" });

      expect(ref).toBe("hook_abc");
      expect(mockSchedule).toHaveBeenCalledWith({
        path: "/api/delaykit/send-reminder",
        postAt: "2026-06-01T12:00:00.000Z",
        data: { jobId: "job_123" },
      });
    });
  });

  describe("cancel", () => {
    it("calls Posthook API to delete a hook", async () => {
      const { scheduler } = createPosthookKit();

      mockDelete.mockResolvedValue(undefined);

      await scheduler.cancel("hook_abc");

      expect(mockDelete).toHaveBeenCalledWith("hook_abc");
    });
  });

  describe("verifyDelivery", () => {
    it("delegates to SDK signatures.parseDelivery", () => {
      const { scheduler } = createPosthookKit();

      mockParseDelivery.mockReturnValue({
        hookId: "hook_123",
        data: { jobId: "job_456" },
      });

      const result = scheduler.verifyDelivery(
        '{"test": true}',
        { "posthook-signature": "v1,abc" },
      );

      expect(result.hookId).toBe("hook_123");
      expect(result.data).toEqual({ jobId: "job_456" });
      expect(mockParseDelivery).toHaveBeenCalled();
    });
  });
});

describe("createHandler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns 401 on invalid signature", async () => {
    const { dk } = createPosthookKit();
    dk.handle("test", async () => {});

    mockParseDelivery.mockImplementation(() => {
      throw new Error("Invalid signature");
    });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId: "job_1", version: 1 });
    const res = await handler(req);

    expect(res.status).toBe(401);
  });

  it("executes handler on valid delivery", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("greet", async ({ key }) => { received(key); });

    // Create a pending job in the store
    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId,
      kind: "once",
      handler: "greet",
      key: "user:123",
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 1,
      schedulerRef: "hook_abc",
      lastError: null,
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
    });

    mockParseDelivery.mockReturnValue({
      hookId: "hook_abc",
      data: { jobId },
    });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId, version: 1 });
    const res = await handler(req);

    expect(res.status).toBe(200);
    expect(received).toHaveBeenCalledWith("user:123");
  });

  it("returns 200 for delivery to nonexistent job", async () => {
    const { dk } = createPosthookKit();
    dk.handle("test", async () => {});

    mockParseDelivery.mockReturnValue({
      hookId: "hook_old",
      data: { jobId: "nonexistent-id" },
    });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId: "nonexistent-id" });
    const res = await handler(req);

    expect(res.status).toBe(200); // nothing to do
  });

  it("returns 500 on retriable handler failure", async () => {
    const { dk, store } = createPosthookKit();
    dk.handle("flaky", {
      handler: async () => { throw new Error("boom"); },
      retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
    });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId,
      kind: "once",
      handler: "flaky",
      key: "retry:1",
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 3,
      schedulerRef: null,
      lastError: null,
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
    });

    mockParseDelivery.mockReturnValue({
      hookId: "hook_retry",
      data: { jobId },
    });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId, version: 1 });
    const res = await handler(req);

    expect(res.status).toBe(500); // retry → Posthook redelivers
  });

  it("returns 200 on exhausted failure and calls onFailure", async () => {
    const { dk, store } = createPosthookKit();

    const onFailure = vi.fn();
    dk.handle("doomed", {
      handler: async () => { throw new Error("always fails"); },
      onFailure,
    });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId,
      kind: "once",
      handler: "doomed",
      key: "exhaust:1",
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(),
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 1, // only 1 attempt
      schedulerRef: null,
      lastError: null,
      firstAt: null,
      lastAt: null,
      waitMs: null,
      maxWaitMs: null,
    });

    mockParseDelivery.mockReturnValue({
      hookId: "hook_doom",
      data: { jobId },
    });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId, version: 1 });
    const res = await handler(req);

    expect(res.status).toBe(200); // exhausted → stop retries
    expect(onFailure).toHaveBeenCalledWith(
      expect.objectContaining({ key: "exhaust:1" }),
    );
  });

  it("redelivery after failure finds job pending (not stuck running)", async () => {
    const { dk, store } = createPosthookKit();

    let callCount = 0;
    dk.handle("flaky", {
      handler: async () => {
        callCount++;
        if (callCount === 1) throw new Error("first fail");
      },
      retry: { attempts: 3, backoff: "fixed", initialDelay: "1s" },
    });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId, kind: "once", handler: "flaky", key: "redeliver:1",
      version: 1, claimedVersion: null, status: "pending",
      scheduledFor: new Date(), startedAt: null, completedAt: null,
      attempt: 0, maxAttempts: 3, schedulerRef: null, lastError: null,
      firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
    });

    mockParseDelivery.mockReturnValue({ hookId: "hook_1", data: { jobId } });

    const handler = dk.createHandler();

    // First delivery — fails, returns 500, job transitions to pending
    const res1 = await handler(makeDeliveryRequest({ jobId }));
    expect(res1.status).toBe(500);
    expect(callCount).toBe(1);

    // Verify job is pending (not stuck running)
    const afterFail = await store.getJob(jobId);
    expect(afterFail!.status).toBe("pending");
    expect(afterFail!.attempt).toBe(1);

    // Second delivery (redelivery) — succeeds
    const res2 = await handler(makeDeliveryRequest({ jobId }));
    expect(res2.status).toBe(200);
    expect(callCount).toBe(2);
  });

  it("stale artifact: hookId !== schedulerRef → ignored (200)", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("task", async ({ key }) => { received(key); });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId, kind: "once", handler: "task", key: "replace:1",
      version: 2, claimedVersion: null, status: "pending",
      scheduledFor: new Date(Date.now() + 3_600_000),
      startedAt: null, completedAt: null,
      attempt: 0, maxAttempts: 1, schedulerRef: "hook_new", lastError: null,
      firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
    });

    // Old hook delivers — hookId doesn't match schedulerRef
    mockParseDelivery.mockReturnValue({ hookId: "hook_old", data: { jobId } });

    const handler = dk.createHandler();
    const res = await handler(makeDeliveryRequest({ jobId }));

    expect(res.status).toBe(200); // acknowledged, not executed
    expect(received).not.toHaveBeenCalled();
  });

  it("early delivery: hookId matches but scheduledFor > now → retry (500)", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("task", async ({ key }) => { received(key); });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId, kind: "once", handler: "task", key: "early:1",
      version: 1, claimedVersion: null, status: "pending",
      scheduledFor: new Date(Date.now() + 3_600_000), // 1 hour from now
      startedAt: null, completedAt: null,
      attempt: 0, maxAttempts: 1, schedulerRef: "hook_current", lastError: null,
      firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
    });

    // Current hook delivers early (before scheduledFor)
    mockParseDelivery.mockReturnValue({ hookId: "hook_current", data: { jobId } });

    const handler = dk.createHandler();
    const res = await handler(makeDeliveryRequest({ jobId }));

    // Should return 500 so Posthook retries later — not 200 which would strand the job
    expect(res.status).toBe(500);
    expect(received).not.toHaveBeenCalled();
  });

  it("schedulerRef null: ref guard skipped, falls through to timing guard", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("task", async ({ key }) => { received(key); });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    await store.createJob({
      id: jobId, kind: "once", handler: "task", key: "null-ref:1",
      version: 1, claimedVersion: null, status: "pending",
      scheduledFor: new Date(), // due now
      startedAt: null, completedAt: null,
      attempt: 0, maxAttempts: 1, schedulerRef: null, lastError: null,
      firstAt: null, lastAt: null, waitMs: null, maxWaitMs: null,
    });

    // Any hookId — ref guard skipped because schedulerRef is null
    mockParseDelivery.mockReturnValue({ hookId: "hook_any", data: { jobId } });

    const handler = dk.createHandler();
    const res = await handler(makeDeliveryRequest({ jobId }));

    expect(res.status).toBe(200);
    expect(received).toHaveBeenCalledOnce();
  });

  it("pattern delivery works after version bump (no stale skip)", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("save", async ({ key }) => { received(key); });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    const now = new Date();
    // Debounce job — version was bumped to 3 by subsequent events
    await store.createJob({
      id: jobId, kind: "debounce", handler: "save", key: "doc:1",
      version: 3, claimedVersion: null, status: "pending",
      scheduledFor: new Date(now.getTime() - 1000),
      startedAt: null, completedAt: null,
      attempt: 0, maxAttempts: 1, schedulerRef: "hook_v1", lastError: null,
      firstAt: new Date(now.getTime() - 5000),
      lastAt: new Date(now.getTime() - 1000), // settled
      waitMs: 500, maxWaitMs: null,
    });

    // Original hook (scheduled at version 1) delivers now.
    // Should still work because createHandler uses current version, not hook's.
    mockParseDelivery.mockReturnValue({ hookId: "hook_v1", data: { jobId } });
    mockSchedule.mockResolvedValue({ id: "hook_new" });

    const handler = dk.createHandler();
    const res = await handler(makeDeliveryRequest({ jobId }));

    expect(res.status).toBe(200);
    expect(received).toHaveBeenCalledWith("doc:1");
  });

  it("handles debounce delivery with settlement check", async () => {
    const { dk, store } = createPosthookKit();

    const received = vi.fn();
    dk.handle("save", async ({ key }) => { received(key); });

    const { randomUUID } = await import("node:crypto");
    const jobId = randomUUID();
    const now = new Date();
    await store.createJob({
      id: jobId,
      kind: "debounce",
      handler: "save",
      key: "doc:1",
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: new Date(now.getTime() - 1000), // past due
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: 1,
      schedulerRef: null,
      lastError: null,
      firstAt: new Date(now.getTime() - 2000),
      lastAt: new Date(now.getTime() - 1000), // settled (1s ago, wait is 500ms)
      waitMs: 500,
      maxWaitMs: null,
    });

    mockParseDelivery.mockReturnValue({
      hookId: "hook_debounce",
      data: { jobId },
    });
    mockSchedule.mockResolvedValue({ id: "hook_new" });

    const handler = dk.createHandler();
    const req = makeDeliveryRequest({ jobId, version: 1 });
    const res = await handler(req);

    expect(res.status).toBe(200);
    expect(received).toHaveBeenCalledWith("doc:1");
  });
});

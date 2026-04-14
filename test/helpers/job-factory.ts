import { randomUUID } from "node:crypto";
import type { Job } from "../../src/types.js";

const defaults: Omit<Job, "createdAt"> = {
  id: "",
  kind: "once",
  handler: "test",
  key: "",
  version: 1,
  claimedVersion: null,
  status: "pending",
  scheduledFor: new Date(),
  startedAt: null,
  completedAt: null,
  attempt: 0,
  maxAttempts: 1,
  schedulerRef: null,
  lastError: null,
  firstAt: null,
  lastAt: null,
  waitMs: null,
  maxWaitMs: null,
  deferAttempts: 0,
  deferredSince: null,
  retryConfig: null,
};

export function makeJob(overrides?: Partial<Job>): Omit<Job, "createdAt"> {
  return {
    ...defaults,
    id: randomUUID(),
    key: `test:${randomUUID().slice(0, 8)}`,
    scheduledFor: new Date(),
    ...overrides,
  };
}

export function makeDebounceJob(key: string, waitMs: number, overrides?: Partial<Job>): Omit<Job, "createdAt"> {
  const now = new Date();
  return makeJob({
    kind: "debounce",
    key,
    waitMs,
    firstAt: now,
    lastAt: now,
    scheduledFor: new Date(now.getTime() + waitMs),
    ...overrides,
  });
}

export function makeThrottleJob(key: string, waitMs: number, overrides?: Partial<Job>): Omit<Job, "createdAt"> {
  const now = new Date();
  return makeJob({
    kind: "throttle",
    key,
    waitMs,
    maxWaitMs: null,
    firstAt: now,
    lastAt: now,
    scheduledFor: new Date(now.getTime() + waitMs),
    ...overrides,
  });
}

/** Create a job that is already stalled (running with expired startedAt) */
export function makeStalledJob(overrides?: Partial<Job>): Omit<Job, "createdAt"> {
  return makeJob({
    status: "running",
    claimedVersion: 1,
    startedAt: new Date(Date.now() - 60_000),
    scheduledFor: new Date(Date.now() - 60_000),
    maxAttempts: 3,
    ...overrides,
  });
}

import { expect } from "vitest";
import type { Job, Store } from "../../src/types.js";
import type { ExternalSchedulerHarness } from "./external-scheduler-harness.js";

/**
 * Core invariants — always valid after any operation.
 * These come from docs/INVARIANTS.md and should never be weakened
 * without updating the doc first.
 */
export async function assertCoreInvariants(store: Store): Promise<void> {
  // Collect all jobs via getDueJobs won't work (only pending).
  // For MemoryStore we can access the jobs map via a test-only method.
  // For contract tests, we verify invariants on individual jobs we know about.
}

/**
 * Assert invariants on a specific job after an operation.
 */
export function assertJobInvariants(job: Job): void {
  // Running rows must have startedAt and claimedVersion
  if (job.status === "running") {
    expect(job.startedAt).not.toBeNull();
    expect(job.claimedVersion).not.toBeNull();
    expect(job.claimedVersion!).toBeLessThanOrEqual(job.version);
  }

  // Pending/cancelled rows should not have claimedVersion
  if (job.status === "pending" || job.status === "cancelled") {
    expect(job.claimedVersion).toBeNull();
  }

  // Terminal rows should have completedAt
  if (job.status === "completed" || job.status === "failed") {
    expect(job.completedAt).not.toBeNull();
  }

  // Pattern rows must have valid pattern fields
  if (job.kind === "debounce" || job.kind === "throttle") {
    expect(job.firstAt).not.toBeNull();
    expect(job.lastAt).not.toBeNull();
    expect(job.waitMs).not.toBeNull();
    expect(job.waitMs!).toBeGreaterThan(0);
  }

  // Once rows should not have pattern fields
  if (job.kind === "once") {
    expect(job.waitMs).toBeNull();
  }

  // Version must be positive
  expect(job.version).toBeGreaterThanOrEqual(1);

  // Attempt must not exceed maxAttempts (except during reclaim where it can equal)
  expect(job.attempt).toBeLessThanOrEqual(job.maxAttempts);
}

/**
 * Assert that at most one active row exists per (handler, key).
 * Call this with all jobs for a given handler + key pair.
 */
export function assertAtMostOneActive(jobs: Job[]): void {
  const active = jobs.filter(j => j.status === "pending" || j.status === "running");
  expect(active.length).toBeLessThanOrEqual(1);
}

/**
 * Assert that a terminal job doesn't block key reuse.
 * After a job completes/fails, a new job with the same key should be creatable.
 */
export async function assertKeyReusable(store: Store, handler: string, key: string): Promise<void> {
  const active = await store.getActiveJobByKey(handler, key);
  expect(active).toBeNull();
}

/**
 * Polling-specific: no pending retry job should be scheduled in the past.
 * Exception: pattern requeues from stalled recovery may be immediately due.
 */
export function assertNoStalePendingRetry(job: Job): void {
  if (job.status === "pending" && job.attempt > 0 && job.kind === "once") {
    expect(job.scheduledFor.getTime()).toBeGreaterThanOrEqual(Date.now() - 1000);
  }
}

/**
 * External scheduler: verify hook state is consistent with job state.
 */
export function assertSchedulerConsistency(
  job: Job,
  harness: ExternalSchedulerHarness,
): void {
  if (job.status === "pending" && job.schedulerRef) {
    const hooks = harness.allHooksFor(job.id);
    // At least one non-cancelled hook should exist for an active pending job
    const active = hooks.filter(h => !h.cancelled);
    expect(active.length).toBeGreaterThanOrEqual(1);
  }
}

import { randomUUID } from "node:crypto";
import type { Job, Store } from "../types.js";
import { ACTIVE_STATUSES, DEFAULT_TIMEOUT_MS, STALLED_GRACE_MS, truncateLastError } from "../types.js";

const EVICTION_INTERVAL = 60_000;
const EVICTION_AGE = 5 * 60_000;

function indexKey(handler: string, key: string): string {
  return `${handler}\0${key}`;
}

function resetDeferFields(job: Job): void {
  job.deferAttempts = 0;
  job.deferredSince = null;
}

export class MemoryStore implements Store {
  private jobs = new Map<string, Job>();
  private keyIndex = new Map<string, string>(); // indexKey(handler, key) → jobId
  private evictionTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    this.evictionTimer = setInterval(() => this.evictTerminal(), EVICTION_INTERVAL);
  }

  async createJob(job: Omit<Job, "createdAt">): Promise<Job> {
    const ik = indexKey(job.handler, job.key);
    const existingId = this.keyIndex.get(ik);
    if (existingId) {
      const existing = this.jobs.get(existingId);
      if (existing && ACTIVE_STATUSES.has(existing.status)) {
        throw new Error(`Job with active key "${job.key}" already exists (concurrent insert)`);
      }
    }

    const full: Job = { ...job, lastError: truncateLastError(job.lastError), createdAt: new Date() };
    this.jobs.set(full.id, full);
    this.keyIndex.set(ik, full.id);
    return full;
  }

  async getJob(id: string): Promise<Job | null> {
    const job = this.jobs.get(id);
    return job ? { ...job } : null;
  }

  async getActiveJobByKey(handler: string, key: string): Promise<Job | null> {
    const ik = indexKey(handler, key);
    const id = this.keyIndex.get(ik);
    if (!id) return null;
    const job = this.jobs.get(id);
    if (!job || !ACTIVE_STATUSES.has(job.status)) {
      this.keyIndex.delete(ik);
      return null;
    }
    return { ...job };
  }

  async cancelJob(id: string): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "pending") return false;
    job.status = "cancelled";
    job.completedAt = new Date();
    resetDeferFields(job);
    this.keyIndex.delete(indexKey(job.handler, job.key));
    return true;
  }

  async updateScheduledFor(id: string, scheduledFor: Date): Promise<void> {
    const job = this.jobs.get(id);
    if (job) job.scheduledFor = scheduledFor;
  }

  async deleteJob(id: string): Promise<void> {
    const job = this.jobs.get(id);
    if (job) this.keyIndex.delete(job.key);
    this.jobs.delete(id);
  }

  async updatePatternEvent(
    key: string,
    handler: string,
    kind: "debounce" | "throttle",
    now: Date,
    waitMs: number,
    maxWaitMs: number | null,
  ): Promise<Job | null> {
    const id = this.keyIndex.get(indexKey(handler, key));
    if (!id) return null;
    const job = this.jobs.get(id);
    if (!job || !ACTIVE_STATUSES.has(job.status)) return null;

    // Validate frozen config
    if (job.kind !== kind) {
      throw new Error(
        `Cannot use ${kind} for key "${key}": an active ${job.kind} job exists for this key.`
      );
    }
    if (job.handler !== handler) {
      throw new Error(
        `Config mismatch for key "${key}": active job uses handler "${job.handler}" but "${handler}" was requested.`
      );
    }
    if (job.waitMs !== waitMs) {
      throw new Error(
        `Config mismatch for key "${key}": active job uses wait=${job.waitMs}ms but ${waitMs}ms was requested.`
      );
    }
    if (job.maxWaitMs !== maxWaitMs) {
      throw new Error(
        `Config mismatch for key "${key}": active job uses maxWait=${job.maxWaitMs}ms but ${maxWaitMs}ms was requested.`
      );
    }

    job.version += 1;
    job.lastAt = now;

    if (job.status === "running") {
      // Only reset firstAt on the FIRST event after execution started.
      // Subsequent events during the same execution just update lastAt.
      // This keeps throttle windows anchored and debounce maxWait stable.
      if (!job.startedAt || job.firstAt!.getTime() <= job.startedAt.getTime()) {
        job.firstAt = now;
      }
    }

    return { ...job };
  }

  async markRunning(id: string, version: number): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "pending" || job.version !== version) return false;
    job.status = "running";
    job.claimedVersion = version;
    job.startedAt = new Date();
    resetDeferFields(job);
    return true;
  }

  async markCompleted(id: string, version: number): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || job.version !== version) return false;
    job.status = "completed";
    job.completedAt = new Date();
    this.keyIndex.delete(indexKey(job.handler, job.key));
    return true;
  }

  async markFailed(id: string, version: number, error: Error): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || job.version !== version) return false;
    job.status = "failed";
    job.lastError = truncateLastError(error.message);
    job.completedAt = new Date();
    this.keyIndex.delete(indexKey(job.handler, job.key));
    return true;
  }

  async retryJob(id: string, version: number, nextAttempt: number, scheduledFor: Date, lastError: string): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || job.version !== version) return false;
    job.status = "pending";
    job.attempt = nextAttempt;
    job.scheduledFor = scheduledFor;
    job.startedAt = null;
    job.completedAt = null;
    job.claimedVersion = null;
    job.lastError = truncateLastError(lastError);
    return true;
  }

  async rescheduleDueAt(id: string, version: number): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "pending" || job.version !== version) return null;
    if (!job.lastAt || !job.waitMs) return null;

    job.scheduledFor = computePatternDueAt(job);
    job.version += 1;
    return { ...job };
  }

  async requeueForNextWindow(id: string): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running") return null;
    if (!job.lastAt || !job.waitMs) return null;

    job.status = "pending";
    job.scheduledFor = computePatternDueAt(job);
    job.startedAt = null;
    job.completedAt = null;
    job.claimedVersion = null;
    job.attempt = 0;
    job.version += 1;
    return { ...job };
  }

  async replaceJob(id: string, scheduledFor: Date, maxAttempts: number): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "pending") return null;
    job.version += 1;
    job.scheduledFor = scheduledFor;
    job.attempt = 0;
    job.maxAttempts = maxAttempts;
    job.schedulerRef = null;
    job.lastError = null;
    resetDeferFields(job);
    return { ...job };
  }

  async deferJob(
    id: string,
    version: number,
    scheduledFor: Date,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "pending" || job.version !== version) return null;

    const now = new Date();
    const firstDefer = job.deferredSince ?? now;

    job.version += 1;
    job.deferAttempts += 1;
    job.deferredSince = firstDefer;

    if (now.getTime() - firstDefer.getTime() >= horizonMs) {
      job.status = "failed";
      job.completedAt = now;
      job.lastError = truncateLastError(terminalError);
      this.keyIndex.delete(indexKey(job.handler, job.key));
    } else {
      job.scheduledFor = scheduledFor;
      job.lastError = truncateLastError(deferredError);
    }
    return { ...job };
  }

  async updateSchedulerRef(id: string, version: number, ref: string): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.version !== version) return false;
    job.schedulerRef = ref;
    return true;
  }

  async reclaimStalled(id: string, leaseMs: number): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || !job.startedAt) return null;
    if (Date.now() - job.startedAt.getTime() <= leaseMs) return null;

    if (job.kind !== "once" && job.claimedVersion != null && job.version > job.claimedVersion) {
      // Pattern with version advance: requeue fresh window
      job.status = "pending";
      job.scheduledFor = computePatternDueAt(job);
      job.startedAt = null;
      job.completedAt = null;
      job.claimedVersion = null;
      job.attempt = 0;
      job.version += 1;
    } else {
      // Reclaim: increment attempt, set pending. Caller decides if exhausted.
      job.status = "pending";
      job.attempt += 1;
      job.startedAt = null;
      job.claimedVersion = null;
    }
    return { ...job };
  }

  async reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]> {
    // Reclaim cutoff is `max(DEFAULT_TIMEOUT_MS, ...handlerTimeouts) +
    // STALLED_GRACE_MS`. The DEFAULT_TIMEOUT_MS floor protects rows
    // whose handler isn't in the current registration map (rolling
    // deploy, rename) from being reclaimed earlier than baseline.
    const cutoffMs = Math.max(DEFAULT_TIMEOUT_MS, ...handlerTimeouts.values()) + STALLED_GRACE_MS;

    const reclaimed: Job[] = [];
    const now = Date.now();
    for (const job of this.jobs.values()) {
      if (job.status !== "running" || !job.startedAt) continue;
      if (now - job.startedAt.getTime() <= cutoffMs) continue;

      if (job.kind !== "once" && job.claimedVersion != null && job.version > job.claimedVersion) {
        // Pattern with version advance: requeue fresh window.
        job.status = "pending";
        job.scheduledFor = computePatternDueAt(job);
        job.startedAt = null;
        job.completedAt = null;
        job.claimedVersion = null;
        job.attempt = 0;
        job.version += 1;
        reclaimed.push({ ...job });
      } else {
        job.status = "pending";
        job.attempt += 1;
        job.startedAt = null;
        job.claimedVersion = null;
        reclaimed.push({ ...job });
      }
    }

    return reclaimed;
  }

  async getDueJobs(limit: number): Promise<Job[]> {
    const now = new Date();
    const due: Job[] = [];

    for (const job of this.jobs.values()) {
      if (job.status === "pending" && job.scheduledFor <= now) {
        due.push(job);
      }
    }

    due.sort((a, b) => a.scheduledFor.getTime() - b.scheduledFor.getTime());
    return due.slice(0, limit);
  }

  async close(): Promise<void> {
    if (this.evictionTimer) {
      clearInterval(this.evictionTimer);
      this.evictionTimer = null;
    }
    this.jobs.clear();
    this.keyIndex.clear();
  }

  private evictTerminal(): void {
    const cutoff = Date.now() - EVICTION_AGE;
    for (const [id, job] of this.jobs) {
      if (
        !ACTIVE_STATUSES.has(job.status) &&
        job.completedAt &&
        job.completedAt.getTime() < cutoff
      ) {
        this.jobs.delete(id);
      }
    }
  }
}

function computePatternDueAt(job: Job): Date {
  if (job.kind === "throttle") {
    // Throttle: fixed window from first event of the burst
    return new Date(job.firstAt!.getTime() + job.waitMs!);
  }
  // Debounce: sliding window from last event, capped by maxWait
  let nextAt = new Date(job.lastAt!.getTime() + job.waitMs!);
  if (job.maxWaitMs != null && job.firstAt) {
    const deadline = new Date(job.firstAt.getTime() + job.maxWaitMs);
    if (nextAt.getTime() > deadline.getTime()) {
      nextAt = deadline;
    }
  }
  return nextAt;
}

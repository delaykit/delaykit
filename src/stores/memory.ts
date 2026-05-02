import { randomUUID } from "node:crypto";
import type { ClaimBatch, DelayKitStats, FailureReason, Job, ListFailedOptions, ListFailedPage, Store } from "../types.js";
import { ACTIVE_STATUSES, ConcurrentInsertError, DEFAULT_TIMEOUT_MS, MAX_LIST_FAILED_LIMIT, STALLED_GRACE_MS, assertCappedLimit, assertPositiveLimit, decodeListFailedCursor, encodeListFailedCursor, isDebounceSettled, truncateLastError } from "../types.js";

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
    // unref() so a forgotten close() doesn't pin the event loop in
    // tests, REPLs, and CLI scripts. Optional-chained for runtimes
    // that don't expose unref on Timeout (the web spec doesn't).
    this.evictionTimer.unref?.();
  }

  async createJob(job: Omit<Job, "createdAt">): Promise<Job> {
    const ik = indexKey(job.handler, job.key);
    const existingId = this.keyIndex.get(ik);
    if (existingId) {
      const existing = this.jobs.get(existingId);
      if (existing && ACTIVE_STATUSES.has(existing.status)) {
        throw new ConcurrentInsertError(job.handler, job.key);
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
    resetDeferFields(job);
    this.keyIndex.delete(indexKey(job.handler, job.key));
    return true;
  }

  async markFailed(id: string, version: number, error: Error, reason: FailureReason): Promise<boolean> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || job.version !== version) return false;
    job.status = "failed";
    job.lastError = truncateLastError(error.message);
    job.failureReason = reason;
    job.completedAt = new Date();
    resetDeferFields(job);
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
    resetDeferFields(job);
    return true;
  }

  async rescheduleJob(id: string, version: number, scheduledFor: Date): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "running" || job.version !== version) return null;
    job.status = "pending";
    job.version += 1;
    job.attempt = 0;
    job.scheduledFor = scheduledFor;
    job.startedAt = null;
    job.completedAt = null;
    job.claimedVersion = null;
    job.lastError = null;
    job.failureReason = null;
    job.schedulerRef = null;
    resetDeferFields(job);
    return { ...job };
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
    resetDeferFields(job);
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
    job.failureReason = null;
    resetDeferFields(job);
    return { ...job };
  }

  async resetJobAt(id: string, version: number, scheduledFor: Date): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "failed" || job.version !== version) return null;
    const ik = indexKey(job.handler, job.key);
    const currentId = this.keyIndex.get(ik);
    if (currentId && currentId !== id) {
      const current = this.jobs.get(currentId);
      if (current && ACTIVE_STATUSES.has(current.status)) return null;
    }
    job.status = "pending";
    job.attempt = 0;
    job.version += 1;
    job.scheduledFor = scheduledFor;
    job.startedAt = null;
    job.completedAt = null;
    job.claimedVersion = null;
    job.lastError = null;
    job.failureReason = null;
    job.deferAttempts = 0;
    job.deferredSince = null;
    job.schedulerRef = null;
    this.keyIndex.set(ik, job.id);
    return { ...job };
  }

  async listFailed(opts: ListFailedOptions): Promise<ListFailedPage> {
    assertCappedLimit(opts.limit, MAX_LIST_FAILED_LIMIT);
    const cursor = opts.cursor ? decodeListFailedCursor(opts.cursor) : null;
    const sinceMs = opts.since?.getTime();
    const untilMs = opts.until?.getTime();

    const matches: Job[] = [];
    for (const job of this.jobs.values()) {
      if (job.status !== "failed" || !job.completedAt) continue;
      if (opts.handler && job.handler !== opts.handler) continue;
      if (opts.reason && job.failureReason !== opts.reason) continue;
      const ms = job.completedAt.getTime();
      if (sinceMs != null && ms < sinceMs) continue;
      if (untilMs != null && ms > untilMs) continue;
      if (cursor) {
        if (ms > cursor.completedAtMs) continue;
        if (ms === cursor.completedAtMs && job.id >= cursor.id) continue;
      }
      matches.push(job);
    }

    matches.sort((a, b) => {
      const diff = b.completedAt!.getTime() - a.completedAt!.getTime();
      return diff !== 0 ? diff : b.id.localeCompare(a.id);
    });

    const page = matches.slice(0, opts.limit);
    const last = page[page.length - 1];
    const more = matches.length > opts.limit;
    return {
      jobs: page.map((j) => ({ ...j })),
      cursor: more && last ? encodeListFailedCursor(last.completedAt!, last.id) : null,
    };
  }

  async resetJob(id: string): Promise<Job | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "failed") return null;
    // Guard: don't resurrect into a key slot already held by a newer active row.
    const ik = indexKey(job.handler, job.key);
    const currentId = this.keyIndex.get(ik);
    if (currentId && currentId !== id) {
      const current = this.jobs.get(currentId);
      if (current && ACTIVE_STATUSES.has(current.status)) return null;
    }
    job.status = "pending";
    job.attempt = 0;
    job.version += 1;
    job.scheduledFor = new Date();
    job.startedAt = null;
    job.completedAt = null;
    job.claimedVersion = null;
    job.lastError = null;
    job.failureReason = null;
    job.deferAttempts = 0;
    job.deferredSince = null;
    job.schedulerRef = null;
    this.keyIndex.set(indexKey(job.handler, job.key), job.id);
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
      job.failureReason = "defer_horizon";
      this.keyIndex.delete(indexKey(job.handler, job.key));
    } else {
      job.scheduledFor = scheduledFor;
      job.lastError = truncateLastError(deferredError);
    }
    return { ...job };
  }

  async noteMissingHandler(
    id: string,
    version: number,
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
      job.failureReason = "defer_horizon";
      this.keyIndex.delete(indexKey(job.handler, job.key));
    } else {
      // scheduled_for intentionally unchanged — capable replicas must
      // still see this row as due on their next claim cycle.
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
    resetDeferFields(job);
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
      } else {
        job.status = "pending";
        job.attempt += 1;
        job.startedAt = null;
        job.claimedVersion = null;
      }
      resetDeferFields(job);
      reclaimed.push({ ...job });
    }

    return reclaimed;
  }

  async unknownDueHandlers(knownHandlers: string[]): Promise<string[]> {
    const known = new Set(knownHandlers);
    const now = new Date();
    const unknown = new Set<string>();
    for (const job of this.jobs.values()) {
      if (job.status !== "pending") continue;
      if (job.scheduledFor > now) continue;
      if (known.has(job.handler)) continue;
      unknown.add(job.handler);
    }
    return Array.from(unknown);
  }

  async unknownDueJobs(knownHandlers: string[], limit: number): Promise<Job[]> {
    const known = new Set(knownHandlers);
    const now = new Date();
    const nowMs = now.getTime();
    const matches: Job[] = [];
    for (const job of this.jobs.values()) {
      if (job.status !== "pending") continue;
      if (job.scheduledFor > now) continue;
      if (known.has(job.handler)) continue;
      // Mirror claimDueJobs's settlement arm — un-settled debounce
      // rows aren't actually deliverable yet, so they shouldn't start
      // the missing-handler horizon clock.
      if (!isDebounceSettled(job, nowMs)) continue;
      matches.push(job);
    }
    // `deferred_since NULLS FIRST` ordering — see Postgres impl for
    // the rationale. Falls through to `scheduled_for ASC, id ASC`.
    matches.sort((a, b) => {
      const av = a.deferredSince?.getTime() ?? null;
      const bv = b.deferredSince?.getTime() ?? null;
      if (av === null && bv !== null) return -1;
      if (bv === null && av !== null) return 1;
      if (av !== null && bv !== null && av !== bv) return av - bv;
      const diff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
      return diff !== 0 ? diff : a.id.localeCompare(b.id);
    });
    return matches.slice(0, limit).map((job) => ({ ...job }));
  }

  async claimDueJobs(budget: number, handlerNames: string[]): Promise<ClaimBatch> {
    if (handlerNames.length === 0) return { toRun: [], rescheduled: [] };
    const allowed = new Set(handlerNames);
    const now = new Date();
    const due: Job[] = [];

    for (const job of this.jobs.values()) {
      if (job.status === "pending" && allowed.has(job.handler) && job.scheduledFor <= now) {
        due.push(job);
      }
    }

    due.sort((a, b) => {
      const diff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
      return diff !== 0 ? diff : a.id.localeCompare(b.id);
    });

    const toRun: Job[] = [];
    const rescheduled: Job[] = [];
    const nowMs = now.getTime();
    for (const job of due.slice(0, budget)) {
      if (!isDebounceSettled(job, nowMs)) {
        job.scheduledFor = computePatternDueAt(job);
        job.version += 1;
        rescheduled.push({ ...job });
      } else {
        job.status = "running";
        job.startedAt = now;
        job.claimedVersion = job.version;
        resetDeferFields(job);
        toRun.push({ ...job });
      }
    }
    return { toRun, rescheduled };
  }

  async pruneTerminal(olderThan: Date, limit?: number): Promise<number> {
    assertPositiveLimit(limit);
    const cutoff = olderThan.getTime();
    const candidates: Job[] = [];
    for (const job of this.jobs.values()) {
      if (
        !ACTIVE_STATUSES.has(job.status) &&
        job.completedAt &&
        job.completedAt.getTime() < cutoff
      ) {
        candidates.push(job);
      }
    }
    candidates.sort((a, b) => {
      const diff = a.completedAt!.getTime() - b.completedAt!.getTime();
      return diff !== 0 ? diff : a.id.localeCompare(b.id);
    });
    const toDelete = limit === undefined ? candidates : candidates.slice(0, limit);
    for (const job of toDelete) this.deleteTerminal(job);
    return toDelete.length;
  }

  async stats(): Promise<DelayKitStats> {
    const now = new Date();
    const nowMs = now.getTime();
    const cutoff24h = new Date(nowMs - 24 * 60 * 60 * 1_000);

    const byHandlerMap = new Map<string, {
      pending: number; duePending: number; running: number; deferred: number; failed24h: number;
    }>();

    const bucket = (handler: string) => {
      if (!byHandlerMap.has(handler)) {
        byHandlerMap.set(handler, { pending: 0, duePending: 0, running: 0, deferred: 0, failed24h: 0 });
      }
      return byHandlerMap.get(handler)!;
    };

    let pending = 0, duePending = 0, running = 0, deferred = 0, failed24h = 0;
    let oldestDuePending: DelayKitStats["oldestDuePending"] = null;
    let oldestRunning: DelayKitStats["oldestRunning"] = null;

    for (const job of this.jobs.values()) {
      if (job.status === "pending") {
        const b = bucket(job.handler);
        pending++;
        b.pending++;
        if (job.scheduledFor <= now && isDebounceSettled(job, nowMs)) {
          duePending++;
          b.duePending++;
          if (
            !oldestDuePending ||
            job.scheduledFor < oldestDuePending.scheduledFor ||
            (job.scheduledFor.getTime() === oldestDuePending.scheduledFor.getTime() && job.id < oldestDuePending.id)
          ) {
            oldestDuePending = { id: job.id, handler: job.handler, scheduledFor: new Date(job.scheduledFor) };
          }
        }
        if (job.deferredSince !== null) {
          deferred++;
          b.deferred++;
        }
      } else if (job.status === "running") {
        const b = bucket(job.handler);
        running++;
        b.running++;
        if (job.startedAt) {
          if (
            !oldestRunning ||
            job.startedAt < oldestRunning.startedAt ||
            (job.startedAt.getTime() === oldestRunning.startedAt.getTime() && job.id < oldestRunning.id)
          ) {
            oldestRunning = { id: job.id, handler: job.handler, startedAt: new Date(job.startedAt) };
          }
        }
      } else if (job.status === "failed" && job.completedAt && job.completedAt >= cutoff24h) {
        const b = bucket(job.handler);
        failed24h++;
        b.failed24h++;
      }
    }

    const byHandler = Array.from(byHandlerMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([handler, counts]) => ({ handler, ...counts }));

    return { pending, duePending, running, deferred, failed24h, oldestDuePending, oldestRunning, byHandler };
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
    for (const job of this.jobs.values()) {
      if (
        !ACTIVE_STATUSES.has(job.status) &&
        job.completedAt &&
        job.completedAt.getTime() < cutoff
      ) {
        this.deleteTerminal(job);
      }
    }
  }

  private deleteTerminal(job: Job): void {
    this.jobs.delete(job.id);
    const ik = indexKey(job.handler, job.key);
    if (this.keyIndex.get(ik) === job.id) this.keyIndex.delete(ik);
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

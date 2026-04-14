/**
 * Job status state machine:
 *
 * once:     pending → running → completed | failed
 * pattern:  pending → running → completed | failed | pending (requeue)
 * any:      pending → cancelled
 */
export type JobStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export const ACTIVE_STATUSES: ReadonlySet<JobStatus> = new Set(["pending", "running"]);

/**
 * Default per-handler timeout (30 seconds) when `HandlerConfig.timeout`
 * is omitted. Chosen as a conservative baseline for I/O-bound handlers
 * (HTTP calls, DB writes, email sends). Override per handler for work
 * that's materially shorter or longer.
 */
export const DEFAULT_TIMEOUT_MS = 30_000;

/**
 * Additional grace period on top of a handler's timeout before the
 * stalled-job sweep considers a `running` row reclaimable. Prevents
 * spurious reclaim of handlers that legitimately ran close to their
 * limit.
 */
export const STALLED_GRACE_MS = 5_000;

/**
 * Wall-clock ceiling on the handler-not-registered defer loop. When
 * exceeded, the row is flipped to `failed` instead of deferred again.
 */
export const DEFER_HORIZON_MS = 24 * 60 * 60 * 1000;

export const DEFER_INITIAL_MS = 5_000;
export const DEFER_MAX_MS = 5 * 60 * 1000;

/**
 * Default `maxDelay` applied to exponential backoff when the user
 * doesn't set one. Prevents `initialDelay * 2^attempts` from
 * scheduling retries hours or days apart at high attempt counts.
 * Fixed and linear backoff have no runaway case and receive no
 * implicit cap. Override per handler with `retry.maxDelay`.
 */
export const DEFAULT_RETRY_MAX_DELAY_MS = 60 * 60 * 1000;

/**
 * Maximum length of `Job.lastError` written by the store. Guards
 * against handlers that throw errors with huge serialized payloads
 * (e.g. `throw new Error(JSON.stringify(responseBody))` on a multi-MB
 * response) bloating DB rows. Character count, not bytes — UTF-8
 * expansion is bounded at ~4x, so on-disk size stays within ~8KB.
 */
export const MAX_LAST_ERROR_CHARS = 2048;

export const LAST_ERROR_TRUNCATION_MARKER = "... [truncated]";

const MAX_LAST_ERROR_PREFIX = MAX_LAST_ERROR_CHARS - LAST_ERROR_TRUNCATION_MARKER.length;

/** Truncate a `lastError` value to `MAX_LAST_ERROR_CHARS`, marker included. */
export function truncateLastError(value: string | null): string | null {
  if (value === null) return null;
  if (value.length <= MAX_LAST_ERROR_CHARS) return value;
  return value.slice(0, MAX_LAST_ERROR_PREFIX) + LAST_ERROR_TRUNCATION_MARKER;
}

export interface Job {
  id: string;
  kind: "once" | "debounce" | "throttle";
  handler: string;
  key: string;
  version: number;
  claimedVersion: number | null;
  status: JobStatus;
  scheduledFor: Date;
  startedAt: Date | null;
  completedAt: Date | null;
  attempt: number;
  maxAttempts: number;
  schedulerRef: string | null;
  lastError: string | null;
  createdAt: Date;
  // Pattern fields (null for kind='once')
  firstAt: Date | null;
  lastAt: Date | null;
  waitMs: number | null;
  maxWaitMs: number | null;
  /** Defer counter, independent of the user-facing `attempt` budget. */
  deferAttempts: number;
  /** First defer of the current miss streak; `null` when not in the defer loop. */
  deferredSince: Date | null;
  /**
   * Snapshot of the handler's retry config at schedule time. Preserves
   * the original backoff/jitter shape for the defer path when the
   * handler isn't registered on this instance. `null` when the handler
   * has no retry config.
   */
  retryConfig: SchedulerRetryConfig | null;
}

export interface ScheduleOptions {
  key: string;
  delay?: string;
  at?: Date;
  onDuplicate?: "skip" | "replace";
}

export interface DebounceOptions {
  key: string;
  wait: string;
  maxWait?: string;
}

export interface ThrottleOptions {
  key: string;
  wait: string;
}

export interface HandlerContext {
  key: string;
  job: Job;
  /**
   * Aborted when the handler's `timeout` fires. Pass it through to any
   * library that accepts an `AbortSignal` (e.g. `fetch`, `pg`) so the
   * handler can exit on abort and release its concurrency slot.
   * Handlers that ignore the signal hold their slot until they return
   * on their own; the `maxConcurrent` cap is not exceeded but
   * throughput is reduced.
   */
  signal: AbortSignal;
}

export type HandlerFn = (ctx: HandlerContext) => Promise<void>;

export interface RetryConfig {
  attempts: number;
  backoff: "exponential" | "linear" | "fixed";
  initialDelay: string;
  maxDelay?: string;
  jitter?: boolean;
}

export interface HandlerConfig {
  handler: HandlerFn;
  /**
   * Maximum handler duration as a duration string (e.g. `"10s"`,
   * `"2m"`). When the timer fires, `ctx.signal` is aborted and the
   * handler is treated as failed. Default: 30 seconds. See
   * {@link DEFAULT_TIMEOUT_MS}.
   */
  timeout?: string;
  retry?: RetryConfig;
  onFailure?: (ctx: { key: string; error: Error; attempts: number }) => Promise<void>;
}

// --- Store interface ---

export interface Store {
  // Job CRUD — id is caller-provided (pre-generated for scheduler-first flow)
  createJob(job: Omit<Job, "createdAt">): Promise<Job>;
  getJob(id: string): Promise<Job | null>;
  getActiveJobByKey(handler: string, key: string): Promise<Job | null>;
  deleteJob(id: string): Promise<void>;

  // Targeted mutations — no generic updateJob to prevent unchecked field overwrites.
  cancelJob(id: string): Promise<boolean>;
  updateScheduledFor(id: string, scheduledFor: Date): Promise<void>;

  // Pattern event on EXISTING window: bump version + lastAt, validate config.
  // Returns null if no active row for this key → caller creates new window.
  updatePatternEvent(
    key: string,
    handler: string,
    kind: "debounce" | "throttle",
    now: Date,
    waitMs: number,
    maxWaitMs: number | null,
  ): Promise<Job | null>;

  // Execution lifecycle
  markRunning(id: string, version: number): Promise<boolean>;
  markCompleted(id: string, version: number): Promise<boolean>;
  markFailed(id: string, version: number, error: Error): Promise<boolean>;
  retryJob(id: string, version: number, nextAttempt: number, scheduledFor: Date, lastError: string): Promise<boolean>;

  /**
   * CAS on `status='pending' AND version=$v`. Increments `deferAttempts`
   * and sets `deferredSince` on first defer. If `now() - deferredSince >=
   * horizonMs` the row flips to `failed` with `terminalError` written to
   * `lastError` (scheduledFor unchanged); otherwise it stays `pending`
   * with the supplied `scheduledFor` and `deferredError` in `lastError`.
   * Returns `null` if the CAS lost.
   */
  deferJob(
    id: string,
    version: number,
    scheduledFor: Date,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null>;

  // Pattern transitions — compute scheduledFor from row's own fields.
  // Return updated job for scheduler materialization, or null if CAS fails.
  rescheduleDueAt(id: string, version: number): Promise<Job | null>;
  requeueForNextWindow(id: string): Promise<Job | null>;

  // Replace (for schedule with onDuplicate: 'replace')
  replaceJob(id: string, scheduledFor: Date, maxAttempts: number): Promise<Job | null>;

  // Conditional schedulerRef update — only writes if the row's version still matches.
  // Prevents a stale delivery path from overwriting a newer ref.
  updateSchedulerRef(id: string, version: number, ref: string): Promise<boolean>;

  // Polling
  getDueJobs(limit: number): Promise<Job[]>;

  // Recovery — targeted single-job reclaim (inline on delivery)
  // Returns the reclaimed job if lease expired, null otherwise.
  reclaimStalled(id: string, leaseMs: number): Promise<Job | null>;

  // Recovery — bulk sweep (PollingScheduler timer)
  reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]>;

  // Lifecycle
  close(): Promise<void>;
}

export interface SchedulerRetryConfig {
  attempts: number;
  backoff: "exponential" | "linear" | "fixed";
  initialDelayMs: number;
  maxDelayMs: number;
  jitter: boolean;
}

// --- Scheduler interface ---

export interface ScheduleRequest {
  id: string;
  version: number;
  at: Date;
  handler: string;
  key?: string;
  retry?: SchedulerRetryConfig;
}

export interface SchedulerContext {
  store: Store;
  handlers: Map<string, { fn: (ctx: HandlerContext) => Promise<void>; timeoutMs: number }>;
  emit: EmitFn;
  /** See `DelayKitOptions.deferHorizon`. */
  deferHorizonMs: number;
}

export interface StopOptions {
  /**
   * Milliseconds to wait for in-flight handlers to finish before
   * returning. When omitted, `stop()` uses a default of
   * `max(registered handler timeouts) + STALLED_GRACE_MS` (falling
   * back to `DEFAULT_TIMEOUT_MS + STALLED_GRACE_MS` when no handlers
   * are registered). Pass `drainMs: 0` to skip the drain entirely;
   * in-flight handlers continue running but no caller awaits them.
   *
   * The computed default can exceed a host's shutdown grace period —
   * e.g., Vercel's 30s window with a handler whose `timeout: "5m"` is
   * declared. Pass an explicit `drainMs` when the platform bound is
   * tighter than the handler bound.
   */
  drainMs?: number;
}

export interface Scheduler {
  /** Maximum total attempts this scheduler supports. Omit for unlimited. */
  maxAttempts?: number;
  /** Called by DelayKit before start() with shared dependencies. */
  init?(ctx: SchedulerContext): void;
  schedule(req: ScheduleRequest): Promise<string | null>;
  cancel(schedulerRef: string): Promise<void>;
  start(): Promise<void>;
  stop(options?: StopOptions): Promise<void>;
  /** Signing key for webhook verification. Set by PosthookScheduler. */
  signingKey?: string;
  /** Verify a webhook delivery. Implemented by PosthookScheduler. */
  verifyDelivery?<T = Record<string, unknown>>(
    body: string,
    headers: Headers | Record<string, string | string[] | undefined>,
  ): { hookId: string; data: T };
}

// --- Lifecycle events ---

export interface JobScheduledEvent {
  type: "job:scheduled";
  job: Job;
  timestamp: Date;
}

export interface JobStartedEvent {
  type: "job:started";
  job: Job;
  timestamp: Date;
  attempt: number;
}

export interface JobCompletedEvent {
  type: "job:completed";
  job: Job;
  timestamp: Date;
  durationMs: number;
}

export interface JobFailedEvent {
  type: "job:failed";
  job: Job;
  timestamp: Date;
  error: Error;
  attempts: number;
  durationMs: number;
}

export interface JobRetryingEvent {
  type: "job:retrying";
  job: Job;
  timestamp: Date;
  error: Error;
  attempt: number;
  nextAttempt: number;
  scheduledFor: Date;
}

export interface JobCancelledEvent {
  type: "job:cancelled";
  job: Job;
  timestamp: Date;
}

export interface JobStalledEvent {
  type: "job:stalled";
  job: Job;
  timestamp: Date;
  stalledMs: number;
  reclaimed: boolean;
}

export interface JobEventMap {
  "job:scheduled": JobScheduledEvent;
  "job:started": JobStartedEvent;
  "job:completed": JobCompletedEvent;
  "job:failed": JobFailedEvent;
  "job:retrying": JobRetryingEvent;
  "job:cancelled": JobCancelledEvent;
  "job:stalled": JobStalledEvent;
}

export type JobEventType = keyof JobEventMap;
export type JobEvent = JobEventMap[JobEventType];
export type JobEventListener<E extends JobEventType> = (event: JobEventMap[E]) => void | Promise<void>;

/** Typed emit function. Synchronous; listener errors are caught internally. */
export type EmitFn = <E extends JobEventType>(event: JobEventMap[E]) => void;

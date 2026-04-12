/**
 * Job status state machine:
 *
 * once:     pending → running → completed | failed
 * pattern:  pending → running → completed | failed | pending (requeue)
 * any:      pending → cancelled
 */
export type JobStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export const ACTIVE_STATUSES: ReadonlySet<JobStatus> = new Set(["pending", "running"]);

export const DEFAULT_TIMEOUT_MS = 30_000;
export const STALLED_GRACE_MS = 5_000;

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
}

export interface Scheduler {
  /** Maximum total attempts this scheduler supports. Omit for unlimited. */
  maxAttempts?: number;
  /** Called by DelayKit before start() with shared dependencies. */
  init?(ctx: SchedulerContext): void;
  schedule(req: ScheduleRequest): Promise<string | null>;
  cancel(schedulerRef: string): Promise<void>;
  start(): Promise<void>;
  stop(): Promise<void>;
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

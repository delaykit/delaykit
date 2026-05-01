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
 * Maximum accepted distance between `dk.schedule({ at })` and now.
 * Dates further in the future are almost always a unit mistake
 * (seconds passed as ms, wrong year). Past Dates are accepted and
 * fire on the next poll — matches the "run ASAP" semantic for
 * absolute times that have already elapsed.
 *
 * Computed as `10 * 366 days` rather than `10 * 365` so that
 * scheduling exactly 10 calendar years out doesn't trip the guard
 * when the window contains leap days. A few days of slack is
 * immaterial for a bound that exists to catch unit mistakes.
 */
export const SCHEDULE_MAX_FUTURE_MS = 10 * 366 * 24 * 60 * 60 * 1000;

/**
 * Maximum length of `Job.lastError` written by the store. Guards
 * against handlers that throw errors with huge serialized payloads
 * (e.g. `throw new Error(JSON.stringify(responseBody))` on a multi-MB
 * response) bloating DB rows. Character count, not bytes — UTF-8
 * expansion is bounded at ~4x, so on-disk size stays within ~8KB.
 */
export const MAX_LAST_ERROR_CHARS = 2048;

export const LAST_ERROR_TRUNCATION_MARKER = "... [truncated]";

/**
 * Thrown by `Store.createJob` when a concurrent insert wins the
 * `(handler, key)` race for an active row. Stores normalize on this
 * type so callers (e.g. `dk.schedule`, `dk.debounce`, `dk.throttle`)
 * can `instanceof`-check rather than string-match an error message.
 */
export class ConcurrentInsertError extends Error {
  constructor(handler: string, key: string) {
    super(`Job with active key "${key}" already exists for handler "${handler}" (concurrent insert)`);
    this.name = "ConcurrentInsertError";
  }
}

const MAX_LAST_ERROR_PREFIX = MAX_LAST_ERROR_CHARS - LAST_ERROR_TRUNCATION_MARKER.length;

/** Truncate a `lastError` value to `MAX_LAST_ERROR_CHARS`, marker included. */
/** Shared precondition check for Store methods that accept an optional positive `limit`. */
export function assertPositiveLimit(limit: number | undefined): void {
  if (limit === undefined) return;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error(`limit must be a positive integer, got ${limit}`);
  }
}

/** Required, capped variant of `assertPositiveLimit` for paginated reads. */
export function assertCappedLimit(limit: number, max: number): void {
  if (!Number.isInteger(limit) || limit <= 0 || limit > max) {
    throw new Error(`limit must be a positive integer <= ${max}, got ${limit}`);
  }
}

export function truncateLastError(value: string | null): string | null {
  if (value === null) return null;
  if (value.length <= MAX_LAST_ERROR_CHARS) return value;
  return value.slice(0, MAX_LAST_ERROR_PREFIX) + LAST_ERROR_TRUNCATION_MARKER;
}

/** Coerce an unknown caught value into an Error. */
export function asError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/**
 * Opaque cursor for `listFailed`. Encodes `(completed_at_ms, id)` as
 * base64 of `${ms}:${id}`. The format is internal — callers must treat
 * the string as opaque so we can change it without a breaking change.
 */
export function encodeListFailedCursor(completedAt: Date, id: string): string {
  return Buffer.from(`${completedAt.getTime()}:${id}`, "utf8").toString("base64url");
}

export function decodeListFailedCursor(cursor: string): { completedAtMs: number; id: string } {
  const raw = Buffer.from(cursor, "base64url").toString("utf8");
  const sep = raw.indexOf(":");
  if (sep <= 0) throw new Error(`Invalid cursor: ${cursor}`);
  const ms = Number(raw.slice(0, sep));
  const id = raw.slice(sep + 1);
  if (!Number.isFinite(ms) || !id) throw new Error(`Invalid cursor: ${cursor}`);
  return { completedAtMs: ms, id };
}

/**
 * True when a debounce row's wait window has elapsed (or maxWait
 * exceeded). Non-debounce kinds always return `true` — throttle fires
 * unconditionally at its scheduled time, and `once` has no settlement
 * concept — so callers that branch on `kind` can rely on this as the
 * sole predicate.
 */
export function isDebounceSettled(job: Job, now: number): boolean {
  if (job.kind !== "debounce") return true;
  const waitMs = job.waitMs ?? 0;
  const settled = job.lastAt != null && (now - job.lastAt.getTime()) >= waitMs;
  const maxWaitExceeded = job.maxWaitMs != null && job.firstAt != null &&
    (now - job.firstAt.getTime()) >= job.maxWaitMs;
  return settled || maxWaitExceeded;
}

/**
 * Discriminator on `JobFailedEvent` and persisted on `delaykit.jobs.failure_reason`.
 * `null` only on legacy rows from before the column was added.
 *
 * - `handler_error` — handler threw past max attempts.
 * - `timeout` — handler exceeded its in-flight timeout budget.
 * - `stalled` — process died (or lease expired); reclaimed past max attempts.
 * - `defer_horizon` — handler not registered for the defer horizon.
 * - `materialization_error` — scheduler wake materialization failed during schedule/retry.
 */
export type FailureReason =
  | "handler_error"
  | "timeout"
  | "stalled"
  | "defer_horizon"
  | "materialization_error";

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
  failureReason: FailureReason | null;
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

/**
 * Result of an atomic `Store.claimDueJobs` call. Splits the batch into
 * rows that are ready to run and debounce rows whose `scheduled_for`
 * was advanced because they weren't settled yet.
 */
export interface ClaimBatch {
  toRun: Job[];
  rescheduled: Job[];
}

/** Hard cap on `listFailed.limit` and `retryFailed.limit`. */
export const MAX_LIST_FAILED_LIMIT = 1000;

export interface ListFailedOptions {
  handler?: string;
  reason?: FailureReason;
  since?: Date;
  until?: Date;
  limit: number;
  cursor?: string;
}

export interface ListFailedPage {
  jobs: Job[];
  cursor: string | null;
}

export type RetryFailedOptions =
  | { ids: string[]; spreadMs?: number }
  | (Omit<ListFailedOptions, "cursor"> & { spreadMs?: number });

export interface RetryFailedResult {
  retried: number;
  skipped: number;
  spreadMs: number;
  hasMore: boolean;
}

export interface DelayKitStats {
  /** All jobs with status='pending'. */
  pending: number;
  /** Subset of pending where scheduledFor <= now(). Actual backlog; use for stuck-job alerts. */
  duePending: number;
  running: number;
  /** Subset of pending in the missing-handler defer loop (deferredSince IS NOT NULL). */
  deferred: number;
  /** Jobs with status='failed' and completedAt within the last 24 hours. */
  failed24h: number;
  oldestDuePending: { id: string; handler: string; scheduledFor: Date } | null;
  oldestRunning: { id: string; handler: string; startedAt: Date } | null;
  byHandler: Array<{
    handler: string;
    pending: number;
    duePending: number;
    running: number;
    deferred: number;
    failed24h: number;
  }>;
}

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
  /**
   * Terminal-failure CAS on `status='running' AND version=$v`. Writes
   * `failure_reason` to the row alongside `last_error` so operators can
   * query historical reasons after the event fires.
   */
  markFailed(id: string, version: number, error: Error, reason: FailureReason): Promise<boolean>;
  retryJob(id: string, version: number, nextAttempt: number, scheduledFor: Date, lastError: string): Promise<boolean>;

  /**
   * CAS on `status='pending' AND version=$v`. Increments `deferAttempts`
   * and sets `deferredSince` on first defer. If `now() - deferredSince >=
   * horizonMs` the row flips to `failed` with `terminalError` written to
   * `lastError` and `failure_reason='defer_horizon'` (scheduledFor unchanged);
   * otherwise it stays `pending` with the supplied `scheduledFor` and
   * `deferredError` in `lastError`. Returns `null` if the CAS lost.
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

  /**
   * Return distinct handler names of due-now pending rows whose
   * handler is **not** in `knownHandlers`. Used by the scheduler to
   * warn operators about rows this replica can't process — rows that
   * would otherwise sit pending indefinitely if no replica in the
   * cluster has the handler. Rare-path observability, not a hot-path
   * query.
   */
  unknownDueHandlers(knownHandlers: string[]): Promise<string[]>;

  /**
   * Atomically claim up to `budget` due-now rows whose handler is in
   * `handlerNames`. Handler availability is replica-local, so rows
   * whose handler isn't registered on this replica are never claimed
   * — they stay pending, available for replicas that can run them.
   *
   * Returns two sets in a single round-trip:
   *
   * - `toRun`: settled rows flipped to `running`, ready to execute.
   *   Concurrent pollers claim disjoint sets via `FOR UPDATE SKIP LOCKED`.
   *   Ordering: `scheduled_for ASC, id ASC`.
   * - `rescheduled`: un-settled debounce rows whose `scheduled_for`
   *   was atomically advanced to the next settlement time. Still
   *   `pending`; caller materializes a new wake for each.
   *
   * Throttle and `once` rows always go to `toRun`; only debounce rows
   * can route to `rescheduled`.
   */
  claimDueJobs(budget: number, handlerNames: string[]): Promise<ClaimBatch>;

  // Recovery — targeted single-job reclaim (inline on delivery)
  // Returns the reclaimed job if lease expired, null otherwise.
  reclaimStalled(id: string, leaseMs: number): Promise<Job | null>;

  // Recovery — bulk sweep (PollingScheduler timer)
  reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]>;

  /**
   * Delete terminal rows (`completed` / `failed` / `cancelled`) whose
   * `completedAt < olderThan`. Returns the number of rows deleted.
   *
   * When `limit` is provided, deletes oldest-first in batches so a
   * single prune doesn't lock a large table. `limit` must be a
   * positive integer.
   */
  pruneTerminal(olderThan: Date, limit?: number): Promise<number>;

  /**
   * Reset a `failed` job to `pending` with a fresh attempt budget.
   * Sets `attempt=0`, bumps `version`, `scheduledFor=now()`, clears
   * `lastError`, `failureReason`, `deferAttempts`, `deferredSince`,
   * `schedulerRef`, and execution timestamps. Pattern fields (`firstAt`,
   * `lastAt`, `waitMs`, `maxWaitMs`) and `retryConfig` are preserved.
   * Returns null if the job doesn't exist, isn't in `failed` status, or if
   * the active `(handler, key)` slot is already occupied by a newer row.
   */
  resetJob(id: string): Promise<Job | null>;

  /**
   * Version-guarded variant of `resetJob` for bulk redrive. CAS on
   * `(id, version, status='failed')`; sets `scheduledFor` to the supplied
   * value (instead of `now()`) so callers can spread retried rows over a
   * window. Returns null if the CAS lost (manual retry, prune, or version
   * advanced concurrently). Otherwise identical to `resetJob`.
   */
  resetJobAt(id: string, version: number, scheduledFor: Date): Promise<Job | null>;

  /**
   * Page through `failed` rows for triage and bulk redrive. Newest-first
   * via `(completed_at DESC, id DESC)` so cursor pagination is stable
   * under concurrent writes. `cursor` is opaque — pass back what the
   * previous call returned. `limit` is required and capped at 1000.
   */
  listFailed(opts: ListFailedOptions): Promise<ListFailedPage>;

  stats(): Promise<DelayKitStats>;

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

/**
 * JSON can't represent `Infinity`, which `SchedulerRetryConfig.maxDelayMs`
 * allows. Encode as `null` on write; rehydrate on read. Returns the
 * canonical object form — Postgres binds it directly as JSONB; SQLite
 * call sites `JSON.stringify` it.
 */
export function serializeRetryConfig(
  config: SchedulerRetryConfig,
): Omit<SchedulerRetryConfig, "maxDelayMs"> & { maxDelayMs: number | null } {
  return {
    attempts: config.attempts,
    backoff: config.backoff,
    initialDelayMs: config.initialDelayMs,
    maxDelayMs: Number.isFinite(config.maxDelayMs) ? config.maxDelayMs : null,
    jitter: config.jitter,
  };
}

/** Inverse of `serializeRetryConfig`. Returns `null` for missing or malformed input. */
export function parseRetryConfig(raw: unknown): SchedulerRetryConfig | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Partial<SchedulerRetryConfig> & { maxDelayMs?: number | null };
  if (typeof r.attempts !== "number" || typeof r.backoff !== "string") return null;
  return {
    attempts: r.attempts,
    backoff: r.backoff as SchedulerRetryConfig["backoff"],
    initialDelayMs: r.initialDelayMs ?? 1_000,
    maxDelayMs: r.maxDelayMs == null ? Infinity : r.maxDelayMs,
    jitter: r.jitter ?? false,
  };
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

  /**
   * Whether to close the store after the scheduler drains. Default
   * `false` — the consumer manages store lifecycle, and post-stop
   * cleanup operations (`cancel`, `unschedule`) remain available.
   *
   * Pass `true` to hide the order between `scheduler.stop()` and
   * `store.close()` inside the library. Useful when the store is
   * dedicated to this `DelayKit` instance and shutdown is terminal —
   * e.g., a single Bun server with one SQLite file. Don't pass
   * `true` when the store or its connection pool is shared with
   * other consumers, or when you need post-stop cleanup ops.
   *
   * Store implementations are expected to make `close()` idempotent
   * so calling it again from the consumer is harmless.
   */
  closeStore?: boolean;
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
  reason: FailureReason;
}

export interface JobDeferredEvent {
  type: "job:deferred";
  job: Job;
  timestamp: Date;
  /** Number of times this job has been deferred so far (including this one). */
  deferAttempts: number;
  /** When the job will next be retried. */
  nextAttemptAt: Date;
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

/**
 * Pattern handler (debounce/throttle) ran an attempt while new events
 * arrived for the same key. The just-finished execution's outcome is
 * captured in `outcome`; the row is now `pending` for the next window.
 *
 * Without this event, operators using `job:completed` / `job:failed` /
 * `job:retrying` for metrics would undercount the corresponding outcomes
 * whenever a pattern handler was concurrent with its own events.
 *
 * Only fires for `kind: "debounce" | "throttle"`.
 */
export interface JobRequeuedEvent {
  type: "job:requeued";
  /** The row after requeue: `pending`, with new `scheduledFor` for the next window. */
  job: Job;
  timestamp: Date;
  /** What happened on the just-finished attempt before the row was requeued. */
  outcome: "succeeded" | "failed_with_retries" | "failed_exhausted";
  /** Set when `outcome` is one of the failed cases. */
  error?: Error;
  /** Total attempts made on the just-finished window (including the final one). */
  attempts: number;
  /** Wall-clock duration of the just-finished execution. */
  durationMs: number;
}

export interface JobEventMap {
  "job:scheduled": JobScheduledEvent;
  "job:started": JobStartedEvent;
  "job:completed": JobCompletedEvent;
  "job:failed": JobFailedEvent;
  "job:retrying": JobRetryingEvent;
  "job:cancelled": JobCancelledEvent;
  "job:stalled": JobStalledEvent;
  "job:deferred": JobDeferredEvent;
  "job:requeued": JobRequeuedEvent;
}

export type JobEventType = keyof JobEventMap;
export type JobEvent = JobEventMap[JobEventType];
export type JobEventListener<E extends JobEventType> = (event: JobEventMap[E]) => void | Promise<void>;

/** Typed emit function. Synchronous; listener errors are caught internally. */
export type EmitFn = <E extends JobEventType>(event: JobEventMap[E]) => void;

import { randomUUID } from "node:crypto";
import { delayToDate, parseDuration } from "./duration.js";
import { executeJob, executeClaimed } from "./executor.js";
import type { HandlerEntry } from "./executor.js";
import type {
  DebounceOptions,
  DelayKitStats,
  ListFailedOptions,
  ListFailedPage,
  RetryFailedOptions,
  RetryFailedResult,
  ThrottleOptions,
  HandlerFn,
  HandlerConfig,
  Job,
  Scheduler,
  ScheduleOptions,
  ScheduleResult,
  StopOptions,
  Store,
  JobEventType,
  JobEventListener,
  SchedulerRetryConfig,
} from "./types.js";
import {
  CLOCK_DRIFT_MS,
  ConcurrentInsertError,
  DEFAULT_RETRY_MAX_DELAY_MS,
  DEFAULT_TIMEOUT_MS,
  DEFER_HORIZON_MS,
  MAX_LIST_FAILED_LIMIT,
  SCHEDULE_MAX_FUTURE_MS,
  STALLED_GRACE_MS,
  UNKNOWN_DUE_BUDGET,
  asError,
} from "./types.js";
import type { PollingHandlerEntry } from "./schedulers/polling.js";
import { handleResult, materializeRescheduledWakes, applyMissingHandlerHorizon } from "./result-handler.js";
import type { ResultHandlerDeps } from "./result-handler.js";
import { JobEventEmitter, cloneJobForEvent, emitJobFailed, emitStalled, warnUnknownDueHandlers } from "./emitter.js";

/**
 * Resolve the effective `maxDelay` for a retry config. The default
 * only applies to exponential backoff — fixed and linear don't have a
 * runaway case, and capping them silently would shorten retries that
 * the caller explicitly configured (e.g. `initialDelay: "2h"` on
 * fixed backoff).
 */
function defaultMaxDelayMs(backoff: "exponential" | "linear" | "fixed", maxDelay: string | undefined): number {
  if (maxDelay) return parseDuration(maxDelay);
  return backoff === "exponential" ? DEFAULT_RETRY_MAX_DELAY_MS : Infinity;
}

function jsonResponse(status: number, body: object): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/**
 * Internal, normalized form of a registered handler. Durations are
 * parsed once at registration time. `retry` is present only when
 * `attempts > 1`, so `scheduler.schedule({ retry })` receives
 * `undefined` for no-retry handlers (no-op delivery semantics).
 */
type NormalizedHandler = {
  fn: HandlerFn;
  timeoutMs: number;
  retry?: SchedulerRetryConfig;
  onFailure?: NonNullable<HandlerConfig["onFailure"]>;
};

/**
 * Compute when a debounce window will settle if no further events arrive.
 * Mirrors the settlement logic in stores/memory.ts:computePatternDueAt and
 * executor.ts settlement check — the trailing edge fires at lastAt+waitMs,
 * unless maxWait clamps it earlier (firstAt+maxWaitMs).
 */
function computeDebounceSettlesAt(
  firstAt: Date,
  lastAt: Date,
  waitMs: number,
  maxWaitMs: number | null,
): Date {
  const trailing = lastAt.getTime() + waitMs;
  if (maxWaitMs == null) return new Date(trailing);
  const deadline = firstAt.getTime() + maxWaitMs;
  return new Date(Math.min(trailing, deadline));
}

export interface DelayKitOptions {
  store: Store;
  scheduler: Scheduler;
  /**
   * Maximum wall-clock time a row may stay in the missing-handler
   * defer loop before flipping to `failed`. Default: `"24h"`.
   *
   * Both delivery models maintain the same horizon clock, but with
   * different mechanics:
   *
   * - **Wake path** (`createHandler()` webhook delivery): when a wake
   *   arrives for a row whose handler isn't registered, `deferJob`
   *   advances `scheduled_for` with exponential backoff (5s → 5min
   *   cap), sets `deferredSince` on the first miss, and materializes a
   *   replacement wake. Once `now - deferredSince >= horizonMs`, the
   *   row flips to `failed` and `job:failed` fires with
   *   `reason: "defer_horizon"`.
   *
   * - **Poll path** (`PollingScheduler` / `dk.poll`): handler
   *   availability is replica-local, so unknown-handler rows are
   *   filtered out of `claimDueJobs`. The sweep cycle (and `dk.poll`
   *   for serverless) records the horizon clock via
   *   `Store.noteMissingHandler` *without moving `scheduled_for`*, so
   *   capable replicas in mixed-handler deployments still see the row
   *   as due and can claim it on their next cycle. Termination at
   *   horizon is identical: `failed` with `reason: "defer_horizon"`.
   *
   * `unknownDueHandlers` continues to log a console warning each
   * sweep that finds orphan rows, as a fast operator signal that
   * complements the slower horizon-based termination.
   */
  deferHorizon?: string;
}

type LifecycleState = "idle" | "started" | "stopping" | "closed";

export class DelayKit {
  private store: Store;
  private scheduler: Scheduler;
  private deferHorizonMs: number;
  private handlers = new Map<string, NormalizedHandler>();
  private state: LifecycleState = "idle";
  private stopPromise: Promise<void> | null = null;
  private emitter = new JobEventEmitter();

  constructor(options: DelayKitOptions) {
    this.store = options.store;
    this.scheduler = options.scheduler;
    this.deferHorizonMs = options.deferHorizon
      ? parseDuration(options.deferHorizon)
      : DEFER_HORIZON_MS;
  }

  on<E extends JobEventType>(event: E, listener: JobEventListener<E>): () => void {
    return this.emitter.on(event, listener);
  }

  handle(name: string, handlerOrConfig: HandlerFn | HandlerConfig): void {
    if (this.state !== "idle") {
      throw new Error(
        `Cannot register handler "${name}" after DelayKit has been started. Register all handlers before start(), poll(), or createHandler().`
      );
    }
    if (this.handlers.has(name)) {
      throw new Error(`Handler "${name}" is already registered.`);
    }
    if (!name || /[^a-zA-Z0-9_-]/.test(name)) {
      throw new Error(
        `Invalid handler name "${name}". Use only letters, numbers, hyphens, and underscores.`
      );
    }

    if (typeof handlerOrConfig === "function") {
      this.handlers.set(name, {
        fn: handlerOrConfig,
        timeoutMs: DEFAULT_TIMEOUT_MS,
      });
      return;
    }

    const config = handlerOrConfig;
    if (config.retry) {
      const attempts = config.retry.attempts;
      if (!Number.isInteger(attempts) || attempts < 1) {
        throw new Error(
          `Handler "${name}" has invalid retry.attempts: ${attempts}. Must be a positive integer (1 = no retry, N = N total attempts).`,
        );
      }
    }

    let retry: SchedulerRetryConfig | undefined;
    if (config.retry && config.retry.attempts > 1) {
      const r = config.retry;
      const backoff = r.backoff ?? "fixed";
      retry = {
        attempts: r.attempts,
        backoff,
        initialDelayMs: r.initialDelay ? parseDuration(r.initialDelay) : 1_000,
        maxDelayMs: defaultMaxDelayMs(backoff, r.maxDelay),
        jitter: r.jitter ?? false,
      };
    }

    this.handlers.set(name, {
      fn: config.handler,
      timeoutMs: config.timeout ? parseDuration(config.timeout) : DEFAULT_TIMEOUT_MS,
      retry,
      onFailure: config.onFailure,
    });
  }

  /**
   * Schedule a one-time job for `(handler, key)` at `delay` or `at`.
   *
   * **Idempotent.** If an active row exists for the same `(handler, key)`,
   * the existing row is returned with `created: false` and a
   * `skippedReason` describing why no new row was created. To reschedule
   * the *current run* from inside a handler, use
   * `ctx.reschedule({ delay, at })` — `dk.schedule` cannot replace a
   * row that is already running.
   */
  async schedule(handler: string, options: ScheduleOptions): Promise<ScheduleResult> {
    this.ensureSchedulable("schedule");
    this.validateHandler(handler);
    this.validateScheduleOptions(options);
    if (options.at !== undefined) this.validateScheduleAt(options.at);

    const scheduledFor = options.at ?? delayToDate(options.delay!);
    const onDuplicate = options.onDuplicate ?? "skip";

    const insertOnce = async (): Promise<ScheduleResult> => {
      const job = await this.createNewJob({
        kind: "once",
        handler,
        key: options.key,
        scheduledFor,
      });
      this.emitScheduled(job);
      return { job, created: true };
    };

    const existing = await this.store.getActiveJobByKey(handler, options.key);
    if (existing) {
      return this.handleExistingOnce(existing, handler, options.key, scheduledFor, onDuplicate);
    }

    try {
      return await insertOnce();
    } catch (err) {
      if (!(err instanceof ConcurrentInsertError)) throw err;
      // Lost the unique-constraint race. Re-read and treat as a duplicate.
      const winner = await this.store.getActiveJobByKey(handler, options.key);
      if (winner) {
        return this.handleExistingOnce(winner, handler, options.key, scheduledFor, onDuplicate);
      }
      // Winner was cancelled or completed between the race and re-read — retry the insert.
      return insertOnce();
    }
  }

  /**
   * Schedule a trailing-edge debounced execution. Each call extends the
   * window by `wait` from now. If no further calls arrive within the
   * window, the handler runs once.
   *
   * @returns `settlesAt` — the moment the debounce will settle (and the
   *   handler run) if no further `debounce()` calls are made on this key.
   *   Each subsequent call returns a later `settlesAt`. When `maxWait`
   *   is set, `settlesAt` may be clamped earlier than `now + wait` if
   *   the burst would otherwise exceed the maxWait deadline.
   */
  async debounce(
    handler: string,
    options: DebounceOptions,
  ): Promise<{ settlesAt: Date }> {
    this.ensureSchedulable("debounce");
    this.validateHandler(handler);
    this.validatePatternOptions("debounce", options);

    const waitMs = parseDuration(options.wait);
    const maxWaitMs = options.maxWait ? parseDuration(options.maxWait) : null;
    const now = new Date();

    // Try to update existing window (no new hook needed)
    const updated = await this.store.updatePatternEvent(
      options.key, handler, "debounce", now, waitMs, maxWaitMs,
    );
    if (updated) {
      // updatePatternEvent set lastAt = now and preserved firstAt (unless the
      // previous handler is running, in which case it was reset to now).
      return {
        settlesAt: computeDebounceSettlesAt(updated.firstAt!, now, waitMs, maxWaitMs),
      };
    }

    // New window: firstAt = lastAt = now
    const settlesAt = computeDebounceSettlesAt(now, now, waitMs, maxWaitMs);

    try {
      const job = await this.createNewJob({
        kind: "debounce",
        handler,
        key: options.key,
        scheduledFor: settlesAt,
        pattern: { firstAt: now, lastAt: now, waitMs, maxWaitMs },
      });
      this.emitScheduled(job);
      return { settlesAt };
    } catch (err) {
      if (err instanceof ConcurrentInsertError) {
        // Another call won the insert — our hook is stale (harmless)
        // Retry as update on the winner
        const winner = await this.store.updatePatternEvent(
          options.key, handler, "debounce", now, waitMs, maxWaitMs,
        );
        return {
          settlesAt: winner
            ? computeDebounceSettlesAt(winner.firstAt!, now, waitMs, maxWaitMs)
            : settlesAt,
        };
      }
      throw err;
    }
  }

  async throttle(handler: string, options: ThrottleOptions): Promise<void> {
    this.ensureSchedulable("throttle");
    this.validateHandler(handler);
    this.validatePatternOptions("throttle", options);

    const waitMs = parseDuration(options.wait);
    const now = new Date();

    // Try to update existing window
    const updated = await this.store.updatePatternEvent(
      options.key, handler, "throttle", now, waitMs, null,
    );
    if (updated) return;

    // New window: scheduler-first, then insert
    const scheduledFor = new Date(now.getTime() + waitMs);

    try {
      const job = await this.createNewJob({
        kind: "throttle",
        handler,
        key: options.key,
        scheduledFor,
        pattern: { firstAt: now, lastAt: now, waitMs, maxWaitMs: null },
      });
      this.emitScheduled(job);
    } catch (err) {
      if (err instanceof ConcurrentInsertError) {
        await this.store.updatePatternEvent(
          options.key, handler, "throttle", now, waitMs, null,
        );
        return;
      }
      throw err;
    }
  }

  async cancel(id: string): Promise<boolean> {
    const job = await this.store.getJob(id);
    if (!job || job.status !== "pending") {
      return false;
    }

    const cancelled = await this.store.cancelJob(id);
    if (!cancelled) return false;

    const now = new Date();
    this.emitter.emit({
      type: "job:cancelled",
      job: { ...cloneJobForEvent(job), status: "cancelled", completedAt: new Date(now.getTime()) },
      timestamp: now,
    });

    if (job.schedulerRef) {
      try {
        await this.scheduler.cancel(job.schedulerRef);
      } catch {
        // Logical cancellation succeeded; physical cleanup is best-effort
      }
    }

    return true;
  }

  async getJob(id: string): Promise<Job | null> {
    return this.store.getJob(id);
  }

  /**
   * Look up the active job for a handler + key.
   *
   * Returns null for terminal jobs — a fired, failed, or cancelled job
   * is no longer the *active* job for its key, since the key may have
   * been reused by a fresh schedule. Use `getJob(id)` if you have the
   * job ID and want a specific row regardless of status.
   */
  async getActiveJobByKey(handler: string, key: string): Promise<Job | null> {
    return this.store.getActiveJobByKey(handler, key);
  }

  async stats(): Promise<DelayKitStats> {
    if (this.state === "closed") {
      throw new Error("Cannot stats: DelayKit has stopped. Instantiate a new DelayKit.");
    }
    return this.store.stats();
  }

  async unschedule(handler: string, key: string): Promise<boolean> {
    const job = await this.store.getActiveJobByKey(handler, key);
    if (!job) return false;
    if (job.status !== "pending") return false;
    return this.cancel(job.id);
  }

  async start(): Promise<void> {
    if (this.state === "started") return;
    if (this.state !== "idle") {
      throw new Error(
        `Cannot start: DelayKit has stopped. Instantiate a new DelayKit.`
      );
    }

    this.scheduler.init?.({
      store: this.store,
      handlers: this.buildPollingHandlers(),
      emit: this.emitter.emit,
      deferHorizonMs: this.deferHorizonMs,
    });

    await this.scheduler.start();
    this.state = "started";
  }

  /**
   * Best-effort, terminal shutdown. The instance cannot be reused after `stop()`.
   *
   * Drains in-flight handlers (see `StopOptions.drainMs`), then closes
   * the store unless `closeStore: false` is passed. Idempotent —
   * concurrent or repeated calls await the same in-flight shutdown.
   */
  async stop(options?: StopOptions): Promise<void> {
    if (this.state === "idle" || this.state === "closed") return;
    if (this.stopPromise) return this.stopPromise;

    // Resolve drain before flipping state — parseDuration on a bad
    // handler timeout throws here, leaving state untouched.
    const drainOptions = this.resolveDrainOptions(options);
    const closeStore = options?.closeStore === true;
    this.state = "stopping";
    this.stopPromise = (async () => {
      try {
        await this.scheduler.stop(drainOptions);
        if (closeStore) await this.store.close();
      } finally {
        this.state = "closed";
        this.stopPromise = null;
      }
    })();
    return this.stopPromise;
  }

  private resolveDrainOptions(options?: StopOptions): StopOptions {
    if (options?.drainMs !== undefined) return options;
    return { ...(options ?? {}), drainMs: this.computeDefaultDrainMs() };
  }

  private computeDefaultDrainMs(): number {
    let max = DEFAULT_TIMEOUT_MS;
    for (const entry of this.buildHandlers().values()) {
      if (entry.timeoutMs > max) max = entry.timeoutMs;
    }
    return max + STALLED_GRACE_MS;
  }

  /**
   * Run one poll cycle: find due jobs and execute them concurrently.
   * Use this from a Vercel cron route instead of start():
   *
   * ```ts
   * // app/api/delaykit/poll/route.ts
   * export async function GET() {
   *   await dk().poll({ batchSize: 10, timeout: "8s" });
   *   return Response.json({ ok: true });
   * }
   * ```
   *
   * ```json
   * // vercel.json
   * { "crons": [{ "path": "/api/delaykit/poll", "schedule": "* * * * *" }] }
   * ```
   *
   * @param options.batchSize - Jobs per batch, run concurrently. Keeps
   *   processing batches until no more due jobs or timeout. Default: 10.
   * @param options.timeout - Hard deadline for the poll cycle. If the timeout
   *   elapses, poll() returns. Jobs still running are left in 'running' state
   *   and recovered by stalled job recovery on the next cycle.
   *   Example: "8s" on Vercel Hobby (10s function limit).
   */
  async poll(options?: { batchSize?: number; timeout?: string }): Promise<void> {
    this.enterStarted("poll");
    const handlers = this.buildPollingHandlers();
    const batchSize = options?.batchSize ?? 10;

    // Reclaim stalled jobs from previous cycles (e.g., timeout killed the function)
    const timeouts = new Map<string, number>();
    for (const [name, entry] of handlers) {
      timeouts.set(name, entry.timeoutMs);
    }
    const reclaimed = await this.store.reclaimStalledJobs(timeouts);
    for (const job of reclaimed) {
      const timeout = timeouts.get(job.handler) ?? DEFAULT_TIMEOUT_MS;
      emitStalled(this.emitter.emit, job, timeout + STALLED_GRACE_MS);
    }

    const emit = this.emitter.emit;
    const deadline = options?.timeout
      ? Date.now() + parseDuration(options.timeout)
      : null;

    const deps: ResultHandlerDeps = {
      store: this.store,
      handlers,
      schedule: this.scheduler.schedule.bind(this.scheduler),
      cancel: this.scheduler.cancel.bind(this.scheduler),
      emit,
      deferHorizonMs: this.deferHorizonMs,
    };

    const handlerNames = Array.from(handlers.keys());

    await warnUnknownDueHandlers(this.store, handlerNames);

    while (true) {
      if (deadline && Date.now() >= deadline) break;
      const { done } = await this.runOneCycle(deps, handlerNames, batchSize, deadline);
      if (done) break;
    }

    // Track the missing-handler horizon for due rows whose handler
    // isn't registered on any reachable replica. Runs *after* the
    // claim loop so capable handlers in the same poll get first crack
    // at the rows; the note pass only touches what's actually
    // orphaned. `noteMissingHandler` does not move `scheduled_for`.
    if (!deadline || Date.now() < deadline) {
      const orphans = await this.store.unknownDueJobs(handlerNames, UNKNOWN_DUE_BUDGET);
      for (const job of orphans) {
        await applyMissingHandlerHorizon(job, deps);
      }
    }
  }

  /**
   * Creates a webhook route handler for PosthookScheduler delivery.
   * Mount this as a POST handler in your Next.js app:
   *
   * ```ts
   * // app/api/delaykit/route.ts
   * export const runtime = 'nodejs';
   * export const POST = dk().createHandler();
   * ```
   */
  createHandler(): (req: Request) => Promise<Response> {
    this.enterStarted("createHandler");
    const store = this.store;
    const scheduler = this.scheduler;
    const handlers = this.buildPollingHandlers();
    const emit = this.emitter.emit;
    const deferHorizonMs = this.deferHorizonMs;

    return async (req: Request): Promise<Response> => {
      const validation = await this.validateDelivery(req);
      if ("reject" in validation) return validation.reject;
      const { job } = validation;

      const result = await executeJob(
        { jobId: job.id, version: job.version },
        store,
        handlers,
        emit,
      );

      // PosthookScheduler owns retry timing; handleResult turns the
      // executor outcome into the wire response.
      const outcome = await handleResult(result, {
        store,
        handlers,
        schedule: scheduler.schedule.bind(scheduler),
        cancel: scheduler.cancel.bind(scheduler),
        externalRetries: true,
        emit,
        deferHorizonMs,
      });

      return outcome === "retry"
        ? jsonResponse(500, { status: "retry" })
        : jsonResponse(200, { status: "ok" });
    };
  }

  // --- Private ---

  /**
   * Entry-only check: a call that passes the guard before `stop()`
   * flips state runs to completion, possibly after `stop()` returns.
   * Orphan rows from that race are swept by the next instance's poll
   * or stalled reclaim.
   */
  private ensureSchedulable(op: string): void {
    if (this.state === "stopping") {
      throw new Error(`Cannot ${op}: DelayKit is stopping.`);
    }
    if (this.state === "closed") {
      throw new Error(`Cannot ${op}: DelayKit has stopped. Instantiate a new DelayKit.`);
    }
  }

  private enterStarted(op: string): void {
    this.ensureSchedulable(op);
    if (this.state === "idle") this.state = "started";
  }

  private emitScheduled(job: Job): void {
    this.emitter.emit({
      type: "job:scheduled",
      job: cloneJobForEvent(job),
      timestamp: new Date(),
    });
  }

  async listFailed(opts: ListFailedOptions): Promise<ListFailedPage> {
    if (this.state === "closed") throw new Error("DelayKit is closed");
    return this.store.listFailed(opts);
  }

  async retryFailed(opts: RetryFailedOptions): Promise<RetryFailedResult> {
    this.ensureSchedulable("retryFailed");

    const explicitSpread = opts.spreadMs;
    if (explicitSpread != null && (!Number.isFinite(explicitSpread) || explicitSpread < 0)) {
      throw new Error(`spreadMs must be a non-negative finite number, got ${explicitSpread}`);
    }

    let jobs: Job[];
    let hasMore = false;
    let skipped = 0;

    if ("ids" in opts) {
      if (opts.ids.length > MAX_LIST_FAILED_LIMIT) {
        throw new Error(`ids exceeds cap of ${MAX_LIST_FAILED_LIMIT}, got ${opts.ids.length}`);
      }
      const fetched = await Promise.all(opts.ids.map((id) => this.store.getJob(id)));
      jobs = fetched.filter((j): j is Job => j != null && j.status === "failed");
      skipped = fetched.length - jobs.length;
    } else {
      if (opts.handler == null && opts.reason == null && opts.since == null) {
        throw new Error("retryFailed filter requires at least one of handler, reason, or since");
      }
      const page = await this.store.listFailed({
        handler: opts.handler,
        reason: opts.reason,
        since: opts.since,
        until: opts.until,
        limit: opts.limit,
      });
      jobs = page.jobs;
      hasMore = page.cursor != null;
    }

    const N = jobs.length;
    const spreadMs = explicitSpread ?? Math.min(N * 100, 60_000);

    let retried = 0;
    const baseMs = Date.now();
    for (let i = 0; i < N; i++) {
      const job = jobs[i];
      const offset = N <= 1 || spreadMs === 0 ? 0 : Math.floor((i / N) * spreadMs);
      const scheduledFor = new Date(baseMs + offset);

      const reset = await this.store.resetJobAt(job.id, job.version, scheduledFor);
      if (!reset) { skipped++; continue; }

      try {
        await this.materializeWakeup(reset.id, reset.version, reset.scheduledFor, reset.handler, reset.key, reset.retryConfig ?? undefined);
      } catch (err) {
        // Bulk redrive is best-effort per row: re-fail this one, count it
        // skipped, and continue. Single-job retryJob throws instead.
        await this.failMaterialization(reset, asError(err));
        skipped++;
        continue;
      }

      this.emitScheduled(reset);
      retried++;
    }

    return { retried, skipped, spreadMs, hasMore };
  }

  async retryJob(id: string): Promise<Job | null> {
    this.ensureSchedulable("retryJob");
    const job = await this.store.resetJob(id);
    if (!job) return null;
    try {
      await this.materializeWakeup(job.id, job.version, job.scheduledFor, job.handler, job.key, job.retryConfig ?? undefined);
    } catch (err) {
      await this.failMaterialization(job, asError(err));
      throw err;
    }
    this.emitScheduled(job);
    return job;
  }

  private async createNewJob(args: {
    kind: "once" | "debounce" | "throttle";
    handler: string;
    key: string;
    scheduledFor: Date;
    pattern?: { firstAt: Date; lastAt: Date; waitMs: number; maxWaitMs: number | null };
  }): Promise<Job> {
    const id = randomUUID();
    const ref = await this.scheduler.schedule({
      id,
      version: 1,
      at: args.scheduledFor,
      handler: args.handler,
      key: args.key,
      retry: this.getRetryConfig(args.handler),
    });
    return this.store.createJob({
      id,
      kind: args.kind,
      handler: args.handler,
      key: args.key,
      version: 1,
      claimedVersion: null,
      status: "pending",
      scheduledFor: args.scheduledFor,
      startedAt: null,
      completedAt: null,
      attempt: 0,
      maxAttempts: this.getMaxAttempts(args.handler),
      schedulerRef: ref,
      lastError: null,
      failureReason: null,
      firstAt: args.pattern?.firstAt ?? null,
      lastAt: args.pattern?.lastAt ?? null,
      waitMs: args.pattern?.waitMs ?? null,
      maxWaitMs: args.pattern?.maxWaitMs ?? null,
      deferAttempts: 0,
      deferredSince: null,
      retryConfig: this.getRetryConfig(args.handler) ?? null,
    });
  }

  private async handleExistingOnce(
    existing: Job,
    handler: string,
    key: string,
    scheduledFor: Date,
    onDuplicate: "skip" | "replace",
  ): Promise<ScheduleResult> {
    if (existing.kind !== "once") {
      throw new Error(
        `Cannot schedule key "${key}": a ${existing.kind} pattern is active for this key.`
      );
    }

    if (onDuplicate === "skip") {
      return {
        created: false,
        job: existing,
        skippedReason: existing.status === "running" ? "running" : "pending",
      };
    }

    if (existing.status === "running") {
      return { created: false, job: existing, skippedReason: "running" };
    }

    const replaced = await this.store.replaceJob(
      existing.id,
      scheduledFor,
      this.getMaxAttempts(handler),
    );

    if (!replaced) {
      const current = await this.store.getActiveJobByKey(handler, key);
      return { created: false, job: current ?? existing, skippedReason: "race_lost" };
    }

    // Materialize new wake first, then cancel old. If cancel fails,
    // the stale hook is harmless — schedulerRef guard rejects it.
    // If we cancelled first and materialize failed, the job would be stranded.
    try {
      await this.materializeWakeup(replaced.id, replaced.version, scheduledFor, handler, key);
    } catch (err) {
      await this.failMaterialization(replaced, asError(err));
      throw err;
    }

    if (existing.schedulerRef) {
      try {
        await this.scheduler.cancel(existing.schedulerRef);
      } catch {
        // Best-effort — old delivery rejected by schedulerRef guard
      }
    }

    this.emitScheduled(replaced);
    return { job: replaced, created: true };
  }

  private async materializeWakeup(jobId: string, version: number, scheduledFor: Date, handler: string, key?: string, retryOverride?: SchedulerRetryConfig): Promise<void> {
    const retry = retryOverride ?? this.getRetryConfig(handler);
    const ref = await this.scheduler.schedule({ id: jobId, version, at: scheduledFor, handler, key, retry });
    if (!ref) return;
    const stored = await this.store.updateSchedulerRef(jobId, version, ref);
    if (!stored) {
      try { await this.scheduler.cancel(ref); } catch { /* best-effort */ }
    }
  }

  /**
   * Verify and gate a webhook delivery before it can run a handler.
   * Returns the row to execute on success; otherwise returns a
   * pre-built `Response` the caller should send back unchanged.
   */
  private async runOneCycle(
    deps: ResultHandlerDeps,
    handlerNames: string[],
    batchSize: number,
    deadline: number | null,
  ): Promise<{ done: boolean }> {
    const { toRun, rescheduled } = await this.store.claimDueJobs(batchSize, handlerNames);
    if (toRun.length === 0 && rescheduled.length === 0) return { done: true };

    const { handlers, emit } = deps;

    const runPromise = Promise.all(
      toRun.map(async (job) => {
        try {
          const result = await executeClaimed(job, this.store, handlers, emit);
          await handleResult(result, deps);
        } catch (err) {
          console.error(`[delaykit] Error processing job ${job.id}:`, err);
        }
      }),
    );
    const reschedulePromise = materializeRescheduledWakes(rescheduled, deps).catch((err) => {
      console.error(`[delaykit] materializeRescheduledWakes error:`, err);
    });
    const batch = Promise.all([runPromise, reschedulePromise]);
    // If the timeout below wins the race, `batch` is left orphaned.
    // runPromise/reschedulePromise already swallow their errors, but
    // defend against future regressions of that contract.
    batch.catch(() => {});

    if (deadline) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) return { done: true };
      await Promise.race([
        batch,
        new Promise<void>((resolve) => setTimeout(resolve, remaining)),
      ]);
    } else {
      await batch;
    }

    return { done: toRun.length + rescheduled.length < batchSize };
  }

  private async validateDelivery(
    req: Request,
  ): Promise<{ job: Job } | { reject: Response }> {
    // Once stop() has begun, bounce deliveries with 500 so the
    // external scheduler redelivers to a healthy instance. 200
    // would strand the row; starting new handler work during
    // drain would either extend it or be cut by process exit.
    if (this.state === "stopping" || this.state === "closed") {
      return { reject: jsonResponse(500, { status: "retry" }) };
    }

    if (!this.scheduler.verifyDelivery) {
      return { reject: jsonResponse(500, { error: "Scheduler does not support webhook delivery" }) };
    }

    let jobId: string;
    let hookId: string;
    try {
      const body = await req.text();
      const delivery = this.scheduler.verifyDelivery<{ jobId: string }>(body, req.headers);
      if (typeof delivery.data?.jobId !== "string") {
        throw new Error("Delivery payload missing jobId");
      }
      jobId = delivery.data.jobId;
      hookId = delivery.hookId;
    } catch (err) {
      const message = err instanceof Error ? err.message : "Verification failed";
      return { reject: jsonResponse(401, { error: message }) };
    }

    // Load current row — Posthook hooks carry only jobId, not version.
    const job = await this.store.getJob(jobId);
    if (!job || !["pending", "running"].includes(job.status)) {
      return { reject: jsonResponse(200, { status: "ok" }) };
    }

    // Primary guard: artifact identity.
    // If the row has a schedulerRef and it doesn't match this hook's ID,
    // this is a stale artifact from a previous schedule/replace/reschedule.
    // A current hook should still exist for this job — safe to ignore.
    if (job.schedulerRef && hookId !== job.schedulerRef) {
      return { reject: jsonResponse(200, { status: "ok" }) };
    }

    // Secondary guard: timing.
    // If this IS the current artifact but scheduledFor hasn't arrived yet,
    // return 500 so the scheduler retries later. Returning 200 would strand
    // the job if no other wake is coming.
    if (job.kind === "once" && job.scheduledFor.getTime() > Date.now() + CLOCK_DRIFT_MS) {
      return { reject: jsonResponse(500, { status: "retry" }) };
    }

    return { job };
  }

  private async failMaterialization(job: Job, error: Error): Promise<void> {
    await this.store.markRunning(job.id, job.version);
    const failed = await this.store.markFailed(job.id, job.version, error, "materialization_error");
    if (failed) {
      emitJobFailed(this.emitter.emit, {
        job, error, reason: "materialization_error", attempts: job.attempt, durationMs: 0,
      });
    }
  }

  buildHandlers(): Map<string, HandlerEntry> {
    const entries = new Map<string, HandlerEntry>();
    for (const [name, h] of this.handlers) {
      entries.set(name, { fn: h.fn, timeoutMs: h.timeoutMs });
    }
    return entries;
  }

  private buildPollingHandlers(): Map<string, PollingHandlerEntry> {
    const entries = new Map<string, PollingHandlerEntry>();
    for (const [name, h] of this.handlers) {
      const retry = h.retry
        ? {
            maxAttempts: h.retry.attempts,
            initialDelayMs: h.retry.initialDelayMs,
            maxDelayMs: h.retry.maxDelayMs,
            backoff: h.retry.backoff,
            jitter: h.retry.jitter,
            onFailure: h.onFailure,
          }
        : {
            maxAttempts: 1,
            initialDelayMs: 1_000,
            maxDelayMs: Infinity,
            backoff: "fixed" as const,
            jitter: false,
            onFailure: h.onFailure,
          };
      entries.set(name, { fn: h.fn, timeoutMs: h.timeoutMs, retry });
    }
    return entries;
  }

  private getMaxAttempts(handler: string): number {
    const attempts = this.handlers.get(handler)?.retry?.attempts ?? 1;
    return this.scheduler.maxAttempts ? Math.min(attempts, this.scheduler.maxAttempts) : attempts;
  }

  private getRetryConfig(handler: string): SchedulerRetryConfig | undefined {
    return this.handlers.get(handler)?.retry;
  }

  private validateHandler(name: string): void {
    if (!this.handlers.has(name)) {
      throw new Error(
        `No handler registered for "${name}". Call dk.handle("${name}", ...) before scheduling.`
      );
    }
  }

  private validatePatternOptions(
    kind: "debounce" | "throttle",
    options: { key: string; wait: string },
  ): void {
    if (!options.key || !options.key.trim()) {
      throw new Error(`Key is required for ${kind}.`);
    }
    if (!options.wait) {
      const example = kind === "debounce" ? "5m" : "2m";
      throw new Error(`Wait is required for ${kind} (e.g., "${example}").`);
    }
  }

  private validateScheduleOptions(options: ScheduleOptions): void {
    if (!options.key || !options.key.trim()) {
      throw new Error(
        "Key is required. DelayKit is reference-based — the key identifies the business object this job acts on."
      );
    }
    if (!options.delay && !options.at) {
      throw new Error('Either "delay" (e.g., "24h") or "at" (Date) is required.');
    }
    if (options.delay && options.at) {
      throw new Error('Provide either "delay" or "at", not both.');
    }
  }

  private validateScheduleAt(at: Date): void {
    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new Error(`Invalid "at" Date: ${String(at)}.`);
    }
    if (at.getTime() - Date.now() > SCHEDULE_MAX_FUTURE_MS) {
      throw new Error(
        `"at" is more than 10 years in the future — likely a unit mistake (seconds vs ms, or wrong year).`,
      );
    }
  }
}

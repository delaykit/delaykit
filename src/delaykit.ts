import { randomUUID } from "node:crypto";
import { delayToDate, parseDuration } from "./duration.js";
import { executeJob, executeClaimed } from "./executor.js";
import type { HandlerEntry } from "./executor.js";
import type {
  DebounceOptions,
  DelayKitStats,
  ThrottleOptions,
  HandlerFn,
  HandlerConfig,
  Job,
  Scheduler,
  ScheduleOptions,
  StopOptions,
  Store,
  JobEventType,
  JobEventListener,
  SchedulerRetryConfig,
} from "./types.js";
import {
  ConcurrentInsertError,
  DEFAULT_RETRY_MAX_DELAY_MS,
  DEFAULT_TIMEOUT_MS,
  DEFER_HORIZON_MS,
  SCHEDULE_MAX_FUTURE_MS,
  STALLED_GRACE_MS,
} from "./types.js";
import type { PollingHandlerEntry } from "./schedulers/polling.js";
import { handleResult, materializeRescheduledWakes } from "./result-handler.js";
import { JobEventEmitter, emitStalled, warnUnknownDueHandlers } from "./emitter.js";

/** Grace window for early delivery — absorbs clock drift between Posthook and the app. */
const CLOCK_DRIFT_MS = 5_000;

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
   * Wake-path (Posthook delivery) horizon: maximum wall-clock time a
   * row may stay in the missing-handler defer loop before flipping to
   * `failed`. Default: `"24h"`.
   *
   * Applies only to the wake path (`createHandler()` webhook
   * delivery). When a wake arrives for a row whose handler isn't
   * registered, `deferJob` advances `scheduled_for` with exponential
   * backoff (5s → 5min cap) and sets `deferredSince` on the first
   * miss; once `now - deferredSince >= horizonMs`, the row flips to
   * `failed` and `job:failed` fires.
   *
   * The poll path (`PollingScheduler` / `dk.poll`) has no automatic
   * horizon: rows whose handler isn't registered on this replica are
   * filtered out of the claim candidates entirely (handler
   * availability is replica-local). They stay pending until a
   * replica with the handler claims them. If no replica has the
   * handler, `PollingScheduler.sweepStalled` logs a warning via
   * `unknownDueHandlers`; operators monitor and resolve manually.
   */
  deferHorizon?: string;
}

type LifecycleState = "idle" | "started" | "stopping" | "closed";

export class DelayKit {
  private store: Store;
  private scheduler: Scheduler;
  private deferHorizonMs: number;
  private handlerConfigs = new Map<string, HandlerConfig | HandlerFn>();
  private retryConfigCache = new Map<string, SchedulerRetryConfig>();
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
    if (this.handlerConfigs.has(name)) {
      throw new Error(`Handler "${name}" is already registered.`);
    }
    if (!name || /[^a-zA-Z0-9_-]/.test(name)) {
      throw new Error(
        `Invalid handler name "${name}". Use only letters, numbers, hyphens, and underscores.`
      );
    }
    if (typeof handlerOrConfig !== "function" && handlerOrConfig.retry) {
      const attempts = handlerOrConfig.retry.attempts;
      if (!Number.isInteger(attempts) || attempts < 1) {
        throw new Error(
          `Handler "${name}" has invalid retry.attempts: ${attempts}. Must be a positive integer (1 = no retry, N = N total attempts).`,
        );
      }
    }
    this.handlerConfigs.set(name, handlerOrConfig);

    // Pre-compute retry config (avoids parseDuration on every schedule call)
    if (typeof handlerOrConfig !== "function" && handlerOrConfig.retry && handlerOrConfig.retry.attempts > 1) {
      const r = handlerOrConfig.retry;
      const backoff = r.backoff ?? "fixed";
      this.retryConfigCache.set(name, {
        attempts: r.attempts,
        backoff,
        initialDelayMs: r.initialDelay ? parseDuration(r.initialDelay) : 1_000,
        maxDelayMs: defaultMaxDelayMs(backoff, r.maxDelay),
        jitter: r.jitter ?? false,
      });
    }
  }

  async schedule(handler: string, options: ScheduleOptions): Promise<{ job: Job; created: boolean }> {
    this.ensureSchedulable("schedule");
    this.validateHandler(handler);
    this.validateScheduleOptions(options);
    if (options.at !== undefined) this.validateScheduleAt(options.at);

    const scheduledFor = options.at ?? delayToDate(options.delay!);
    const onDuplicate = options.onDuplicate ?? "skip";

    for (let attempt = 0; attempt < 2; attempt++) {
      const existing = await this.store.getActiveJobByKey(handler, options.key);

      if (existing) {
        if (existing.kind !== "once") {
          throw new Error(
            `Cannot schedule key "${options.key}": a ${existing.kind} pattern is active for this key.`
          );
        }

        if (onDuplicate === "skip") {
          return { job: existing, created: false };
        }

        if (existing.status === "running") {
          return { job: existing, created: false };
        }

        const replaced = await this.store.replaceJob(
          existing.id,
          scheduledFor,
          this.getMaxAttempts(handler),
        );

        if (!replaced) {
          const current = await this.store.getActiveJobByKey(handler, options.key);
          return { job: current ?? existing, created: false };
        }

        // Materialize new wake first, then cancel old. If cancel fails,
        // the stale hook is harmless — schedulerRef guard rejects it.
        // If we cancelled first and materialize failed, the job would be stranded.
        try {
          await this.materializeWakeup(replaced.id, replaced.version, scheduledFor, handler, options.key);
        } catch (err) {
          await this.store.markRunning(replaced.id, replaced.version);
          await this.store.markFailed(replaced.id, replaced.version, err instanceof Error ? err : new Error(String(err)));
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

      // No existing job — scheduler-first, then insert
      const id = randomUUID();
      const ref = await this.scheduler.schedule({ id, version: 1, at: scheduledFor, handler, key: options.key, retry: this.getRetryConfig(handler) });

      try {
        const job = await this.store.createJob({
          id,
          kind: "once",
          handler,
          key: options.key,
          version: 1,
          claimedVersion: null,
          status: "pending",
          scheduledFor,
          startedAt: null,
          completedAt: null,
          attempt: 0,
          maxAttempts: this.getMaxAttempts(handler),
          schedulerRef: ref,
          lastError: null,
          firstAt: null,
          lastAt: null,
          waitMs: null,
          maxWaitMs: null,
          deferAttempts: 0,
          deferredSince: null,
          retryConfig: this.getRetryConfig(handler) ?? null,
        });
        this.emitScheduled(job);
        return { job, created: true };
      } catch (err) {
        if (err instanceof ConcurrentInsertError && attempt === 0) {
          continue; // Retry — loop re-reads and applies full validation
        }
        throw err;
      }
    }

    // Should not reach here, but satisfy TypeScript
    throw new Error(`Failed to schedule key "${options.key}" after retry`);
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
    if (!options.key || !options.key.trim()) throw new Error("Key is required for debounce.");
    if (!options.wait) throw new Error('Wait is required for debounce (e.g., "5m").');

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

    const id = randomUUID();
    const ref = await this.scheduler.schedule({ id, version: 1, at: settlesAt, handler, key: options.key, retry: this.getRetryConfig(handler) });

    try {
      const job = await this.store.createJob({
        id,
        kind: "debounce",
        handler,
        key: options.key,
        version: 1,
        claimedVersion: null,
        status: "pending",
        scheduledFor: settlesAt,
        startedAt: null,
        completedAt: null,
        attempt: 0,
        maxAttempts: this.getMaxAttempts(handler),
        schedulerRef: ref,
        lastError: null,
        firstAt: now,
        lastAt: now,
        waitMs,
        maxWaitMs,
        deferAttempts: 0,
        deferredSince: null,
        retryConfig: this.getRetryConfig(handler) ?? null,
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
    if (!options.key || !options.key.trim()) throw new Error("Key is required for throttle.");
    if (!options.wait) throw new Error('Wait is required for throttle (e.g., "2m").');

    const waitMs = parseDuration(options.wait);
    const now = new Date();

    // Try to update existing window
    const updated = await this.store.updatePatternEvent(
      options.key, handler, "throttle", now, waitMs, null,
    );
    if (updated) return;

    // New window: scheduler-first, then insert
    const id = randomUUID();
    const scheduledFor = new Date(now.getTime() + waitMs);
    const ref = await this.scheduler.schedule({ id, version: 1, at: scheduledFor, handler, key: options.key, retry: this.getRetryConfig(handler) });

    try {
      const job = await this.store.createJob({
        id,
        kind: "throttle",
        handler,
        key: options.key,
        version: 1,
        claimedVersion: null,
        status: "pending",
        scheduledFor,
        startedAt: null,
        completedAt: null,
        attempt: 0,
        maxAttempts: this.getMaxAttempts(handler),
        schedulerRef: ref,
        lastError: null,
        firstAt: now,
        lastAt: now,
        waitMs,
        maxWaitMs: null,
        deferAttempts: 0,
        deferredSince: null,
        retryConfig: this.getRetryConfig(handler) ?? null,
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
      job: { ...job, status: "cancelled", completedAt: now },
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

  async getJobByKey(handler: string, key: string): Promise<Job | null> {
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

  /** Best-effort, terminal shutdown. The instance cannot be reused after `stop()`. */
  async stop(options?: StopOptions): Promise<void> {
    if (this.state === "idle" || this.state === "closed") return;
    if (this.stopPromise) return this.stopPromise;

    // Resolve drain before flipping state — parseDuration on a bad
    // handler timeout throws here, leaving state untouched.
    const drainOptions = this.resolveDrainOptions(options);
    this.state = "stopping";
    this.stopPromise = this.scheduler.stop(drainOptions).finally(() => {
      this.state = "closed";
      this.stopPromise = null;
    });
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

    await warnUnknownDueHandlers(this.store, Array.from(handlers.keys()));

    const emit = this.emitter.emit;
    const deadline = options?.timeout
      ? Date.now() + parseDuration(options.timeout)
      : null;

    const deps = {
      store: this.store,
      handlers,
      schedule: this.scheduler.schedule.bind(this.scheduler),
      cancel: this.scheduler.cancel.bind(this.scheduler),
      emit,
      deferHorizonMs: this.deferHorizonMs,
    };

    const handlerNames = Array.from(handlers.keys());

    while (true) {
      if (deadline && Date.now() >= deadline) break;

      const { toRun, rescheduled } = await this.store.claimDueJobs(batchSize, handlerNames);
      if (toRun.length === 0 && rescheduled.length === 0) break;

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

      if (deadline) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) break;
        // If the timeout wins the race, `batch` is left orphaned.
        // runPromise/reschedulePromise already swallow their errors,
        // but defend against future regressions of that contract by
        // suppressing the orphan tail explicitly.
        batch.catch(() => {});
        await Promise.race([
          batch,
          new Promise<void>((resolve) => setTimeout(resolve, remaining)),
        ]);
      } else {
        await batch;
      }

      if (toRun.length + rescheduled.length < batchSize) break;
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
      // Once stop() has begun, bounce deliveries with 500 so the
      // external scheduler redelivers to a healthy instance. 200
      // would strand the row; starting new handler work during
      // drain would either extend it or be cut by process exit.
      if (this.state === "stopping" || this.state === "closed") {
        return new Response(
          JSON.stringify({ status: "retry" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }

      // Verify the delivery
      if (!scheduler.verifyDelivery) {
        return new Response(
          JSON.stringify({ error: "Scheduler does not support webhook delivery" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }

      let jobId: string;
      let hookId: string;

      try {
        const body = await req.text();
        const delivery = scheduler.verifyDelivery<{ jobId: string }>(
          body,
          req.headers,
        );
        jobId = delivery.data.jobId;
        hookId = delivery.hookId;
      } catch (err) {
        const message = err instanceof Error ? err.message : "Verification failed";
        return new Response(
          JSON.stringify({ error: message }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        );
      }

      // Load current row — Posthook hooks carry only jobId, not version.
      const job = await store.getJob(jobId);
      if (!job || !["pending", "running"].includes(job.status)) {
        return new Response(
          JSON.stringify({ status: "ok" }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      // Primary guard: artifact identity.
      // If the row has a schedulerRef and it doesn't match this hook's ID,
      // this is a stale artifact from a previous schedule/replace/reschedule.
      // A current hook should still exist for this job — safe to ignore.
      if (job.schedulerRef && hookId !== job.schedulerRef) {
        return new Response(
          JSON.stringify({ status: "ok" }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      // Secondary guard: timing.
      // If this IS the current artifact but scheduledFor hasn't arrived yet,
      // return 500 so the scheduler retries later. Returning 200 would strand
      // the job if no other wake is coming.
      if (job.kind === "once" && job.scheduledFor.getTime() > Date.now() + CLOCK_DRIFT_MS) {
        return new Response(
          JSON.stringify({ status: "retry" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }

      // Execute the job using its current version
      const result = await executeJob(
        { jobId, version: job.version },
        store,
        handlers,
        emit,
      );

      // Handle the result — PosthookScheduler owns retry timing
      const outcome = await handleResult(result, {
        store,
        handlers,
        schedule: scheduler.schedule.bind(scheduler),
        cancel: scheduler.cancel.bind(scheduler),
        externalRetries: true,
        emit,
        deferHorizonMs,
      });

      if (outcome === "retry") {
        return new Response(
          JSON.stringify({ status: "retry" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        JSON.stringify({ status: "ok" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
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
      job: { ...job },
      timestamp: new Date(),
    });
  }

  async retryJob(id: string): Promise<Job | null> {
    this.ensureSchedulable("retryJob");
    const job = await this.store.resetJob(id);
    if (!job) return null;
    try {
      await this.materializeWakeup(job.id, job.version, job.scheduledFor, job.handler, job.key, job.retryConfig ?? undefined);
    } catch (err) {
      await this.store.markRunning(job.id, job.version);
      await this.store.markFailed(job.id, job.version, err instanceof Error ? err : new Error(String(err)));
      throw err;
    }
    this.emitScheduled(job);
    return job;
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

  buildHandlers(): Map<string, HandlerEntry> {
    const entries = new Map<string, HandlerEntry>();
    for (const [name, config] of this.handlerConfigs) {
      if (typeof config === "function") {
        entries.set(name, { fn: config, timeoutMs: DEFAULT_TIMEOUT_MS });
      } else {
        entries.set(name, {
          fn: config.handler,
          timeoutMs: config.timeout ? parseDuration(config.timeout) : DEFAULT_TIMEOUT_MS,
        });
      }
    }
    return entries;
  }

  private buildPollingHandlers(): Map<string, PollingHandlerEntry> {
    const entries = new Map<string, PollingHandlerEntry>();
    for (const [name, config] of this.handlerConfigs) {
      if (typeof config === "function") {
        entries.set(name, {
          fn: config,
          timeoutMs: DEFAULT_TIMEOUT_MS,
          retry: {
            maxAttempts: 1,
            initialDelayMs: 1_000,
            maxDelayMs: Infinity,
            backoff: "fixed",
            jitter: false,
          },
        });
      } else {
        const retry = config.retry;
        const backoff = retry?.backoff ?? "fixed";
        entries.set(name, {
          fn: config.handler,
          timeoutMs: config.timeout ? parseDuration(config.timeout) : DEFAULT_TIMEOUT_MS,
          retry: {
            maxAttempts: retry?.attempts ?? 1,
            initialDelayMs: retry?.initialDelay ? parseDuration(retry.initialDelay) : 1_000,
            maxDelayMs: defaultMaxDelayMs(backoff, retry?.maxDelay),
            backoff,
            jitter: retry?.jitter ?? false,
            onFailure: config.onFailure,
          },
        });
      }
    }
    return entries;
  }

  private getMaxAttempts(handler: string): number {
    const config = this.handlerConfigs.get(handler);
    if (!config || typeof config === "function") return 1;
    const attempts = config.retry?.attempts ?? 1;
    return this.scheduler.maxAttempts ? Math.min(attempts, this.scheduler.maxAttempts) : attempts;
  }

  private getRetryConfig(handler: string): SchedulerRetryConfig | undefined {
    return this.retryConfigCache.get(handler);
  }

  private validateHandler(name: string): void {
    if (!this.handlerConfigs.has(name)) {
      throw new Error(
        `No handler registered for "${name}". Call dk.handle("${name}", ...) before scheduling.`
      );
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

import { randomUUID } from "node:crypto";
import { delayToDate, parseDuration } from "./duration.js";
import { executeJob } from "./executor.js";
import type { HandlerEntry } from "./executor.js";
import type {
  DebounceOptions,
  ThrottleOptions,
  HandlerFn,
  HandlerConfig,
  Job,
  Scheduler,
  ScheduleOptions,
  Store,
  JobEventType,
  JobEventListener,
  SchedulerRetryConfig,
} from "./types.js";
import { DEFAULT_TIMEOUT_MS, STALLED_GRACE_MS } from "./types.js";
import type { PollingHandlerEntry } from "./schedulers/polling.js";
import { handleResult } from "./result-handler.js";
import { JobEventEmitter, emitStalled } from "./emitter.js";

/** Grace window for early delivery — absorbs clock drift between Posthook and the app. */
const CLOCK_DRIFT_MS = 5_000;

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
}

export class DelayKit {
  private store: Store;
  private scheduler: Scheduler;
  private handlerConfigs = new Map<string, HandlerConfig | HandlerFn>();
  private retryConfigCache = new Map<string, SchedulerRetryConfig>();
  private started = false;
  private emitter = new JobEventEmitter();

  constructor(options: DelayKitOptions) {
    this.store = options.store;
    this.scheduler = options.scheduler;
  }

  on<E extends JobEventType>(event: E, listener: JobEventListener<E>): () => void {
    return this.emitter.on(event, listener);
  }

  handle(name: string, handlerOrConfig: HandlerFn | HandlerConfig): void {
    if (this.started) {
      throw new Error(
        `Cannot register handler "${name}" after start() or createHandler(). Register all handlers before starting.`
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
    this.handlerConfigs.set(name, handlerOrConfig);

    // Pre-compute retry config (avoids parseDuration on every schedule call)
    if (typeof handlerOrConfig !== "function" && handlerOrConfig.retry && handlerOrConfig.retry.attempts > 1) {
      const r = handlerOrConfig.retry;
      this.retryConfigCache.set(name, {
        attempts: r.attempts,
        backoff: r.backoff ?? "fixed",
        initialDelayMs: r.initialDelay ? parseDuration(r.initialDelay) : 1_000,
        maxDelayMs: r.maxDelay ? parseDuration(r.maxDelay) : Infinity,
        jitter: r.jitter ?? false,
      });
    }
  }

  async schedule(handler: string, options: ScheduleOptions): Promise<{ job: Job; created: boolean }> {
    this.validateHandler(handler);
    this.validateScheduleOptions(options);

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
        });
        this.emitScheduled(job);
        return { job, created: true };
      } catch (err: any) {
        if (err.message?.includes("concurrent insert") && attempt === 0) {
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
    this.validateHandler(handler);
    if (!options.key) throw new Error("Key is required for debounce.");
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
      });
      this.emitScheduled(job);
      return { settlesAt };
    } catch (err: any) {
      if (err.message?.includes("concurrent insert")) {
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
    this.validateHandler(handler);
    if (!options.key) throw new Error("Key is required for throttle.");
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
      });
      this.emitScheduled(job);
    } catch (err: any) {
      if (err.message?.includes("concurrent insert")) {
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

  async unschedule(handler: string, key: string): Promise<boolean> {
    const job = await this.store.getActiveJobByKey(handler, key);
    if (!job) return false;
    if (job.status !== "pending") return false;
    return this.cancel(job.id);
  }

  async start(): Promise<void> {
    if (this.started) return;

    this.scheduler.init?.({
      store: this.store,
      handlers: this.buildPollingHandlers(),
      emit: this.emitter.emit,
    });

    await this.scheduler.start();
    this.started = true;
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    await this.scheduler.stop();
    this.started = false;
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
    this.started = true; // freeze handler registration
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

    // Process due jobs in batches. Loop until no more due jobs or deadline reached.
    while (true) {
      if (deadline && Date.now() >= deadline) break;

      const dueJobs = await this.store.getDueJobs(batchSize);
      if (dueJobs.length === 0) break;

      const batch = Promise.all(
        dueJobs.map(async (job) => {
          try {
            const result = await executeJob(
              { jobId: job.id, version: job.version },
              this.store,
              handlers,
              emit,
            );
            await handleResult(result, {
              store: this.store,
              handlers,
              schedule: this.scheduler.schedule.bind(this.scheduler),
              cancel: this.scheduler.cancel.bind(this.scheduler),
              emit,
            });
          } catch (err) {
            console.error(`[delaykit] Error processing job ${job.id}:`, err);
          }
        }),
      );

      if (deadline) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) break;
        await Promise.race([
          batch,
          new Promise<void>((resolve) => setTimeout(resolve, remaining)),
        ]);
      } else {
        await batch;
      }

      // If we got fewer than limit, there are no more due jobs
      if (dueJobs.length < batchSize) break;
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
    this.started = true; // freeze handler registration
    const store = this.store;
    const scheduler = this.scheduler;
    const handlers = this.buildPollingHandlers();
    const emit = this.emitter.emit;

    return async (req: Request): Promise<Response> => {
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

  private emitScheduled(job: Job): void {
    this.emitter.emit({
      type: "job:scheduled",
      job: { ...job },
      timestamp: new Date(),
    });
  }

  private async materializeWakeup(jobId: string, version: number, scheduledFor: Date, handler: string, key?: string): Promise<void> {
    const ref = await this.scheduler.schedule({ id: jobId, version, at: scheduledFor, handler, key, retry: this.getRetryConfig(handler) });
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
        entries.set(name, {
          fn: config.handler,
          timeoutMs: config.timeout ? parseDuration(config.timeout) : DEFAULT_TIMEOUT_MS,
          retry: {
            maxAttempts: retry?.attempts ?? 1,
            initialDelayMs: retry?.initialDelay ? parseDuration(retry.initialDelay) : 1_000,
            maxDelayMs: retry?.maxDelay ? parseDuration(retry.maxDelay) : Infinity,
            backoff: retry?.backoff ?? "fixed",
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
    if (!options.key) {
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
}

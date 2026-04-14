import { executeJob } from "../executor.js";
import type { HandlerEntry, TriggerPayload } from "../executor.js";
import type { Scheduler, SchedulerContext, Store, StopOptions, EmitFn } from "../types.js";
import { DEFAULT_TIMEOUT_MS, DEFER_HORIZON_MS, STALLED_GRACE_MS } from "../types.js";
import { emitStalled } from "../emitter.js";
import { handleResult, calculateRetryDelay } from "../result-handler.js";

export interface RetryConfig {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoff: "exponential" | "linear" | "fixed";
  jitter: boolean;
  onFailure?: (ctx: { key: string; error: Error; attempts: number }) => Promise<void>;
}

export interface PollingHandlerEntry extends HandlerEntry {
  retry: RetryConfig;
}

export interface PollingSchedulerOptions {
  /** Polling interval in milliseconds. Default: 1000 (1 second). */
  interval?: number;
  /** Stalled job check interval in milliseconds. Default: 30000 (30 seconds). */
  stalledCheckInterval?: number;
  /**
   * Maximum number of handlers running concurrently. When the in-flight
   * count reaches this cap, `poll()` skips the DB fetch until running
   * handlers settle; excess due jobs stay `pending` in the store and
   * are claimed on subsequent polls.
   *
   * Default: 10. Raise for I/O-bound handlers (DB/HTTP). Lower for
   * CPU-bound work that blocks the event loop.
   */
  maxConcurrent?: number;
}

/** Upper bound on the backoff delay applied after a poll or stalled-sweep error. */
const BACKOFF_MAX_MS = 30_000;

/**
 * Long-running polling scheduler.
 *
 * Supported topology: **single instance per store**. Running two or
 * more instances against the same Postgres (or other) store is not
 * yet supported — `getDueJobs` is a non-locking read, so concurrent
 * pollers race on `markRunning` and can degrade throughput. Leader
 * election is on the post-v1 roadmap. For v1, the two production
 * paths are (a) one long-running `PollingScheduler` per app, and
 * (b) `dk.poll()` invoked from Vercel cron (single-cycle), or use
 * `PosthookScheduler` for managed delivery.
 */
export class PollingScheduler implements Scheduler {
  private interval: number;
  private stalledCheckInterval: number;
  private maxConcurrent: number;
  private inFlight = 0;
  private pollInProgress = false;
  private sweepInProgress = false;
  private pollBackoffAttempts = 0;
  private stalledBackoffAttempts = 0;
  private store: Store | null = null;
  private handlers: Map<string, PollingHandlerEntry> | null = null;
  private _emit: EmitFn | null = null;
  private deferHorizonMs = DEFER_HORIZON_MS;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stalledTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private stopping: Promise<void> | null = null;

  constructor(options?: PollingSchedulerOptions) {
    this.interval = options?.interval ?? 1_000;
    this.stalledCheckInterval = options?.stalledCheckInterval ?? 30_000;
    this.maxConcurrent = options?.maxConcurrent ?? 10;
  }

  init(ctx: SchedulerContext): void {
    this.store = ctx.store;
    this.handlers = ctx.handlers as Map<string, PollingHandlerEntry>;
    this._emit = ctx.emit;
    this.deferHorizonMs = ctx.deferHorizonMs;
  }

  async schedule(_req: unknown): Promise<string | null> {
    return null;
  }

  async cancel(_schedulerRef: string): Promise<void> {}

  async start(): Promise<void> {
    if (this.running) return;
    this.detectServerless();
    this.running = true;
    this.stopping = null;
    this.pollBackoffAttempts = 0;
    this.stalledBackoffAttempts = 0;
    this.scheduleNextPoll();
    this.scheduleNextStalledCheck();
  }

  async stop(options?: StopOptions): Promise<void> {
    // Concurrent stop() calls share the first shutdown's promise.
    // A second stop() — even with different options — awaits the
    // same drain instead of racing it.
    if (this.stopping) return this.stopping;
    this.stopping = this.runStop(options);
    return this.stopping;
  }

  private async runStop(options?: StopOptions): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.stalledTimer) {
      clearTimeout(this.stalledTimer);
      this.stalledTimer = null;
    }

    const drainMs = options?.drainMs ?? 0;
    if (drainMs <= 0) return;

    // Wait for in-flight handlers AND any in-progress poll/sweep
    // whose awaited store calls haven't yet incremented `inFlight`.
    const busy = (): boolean =>
      this.inFlight > 0 || this.pollInProgress || this.sweepInProgress;

    if (!busy()) return;

    const deadline = Date.now() + drainMs;
    while (busy() && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }

    if (this.inFlight > 0) {
      console.warn(
        `[delaykit] PollingScheduler.stop drain timeout — ${this.inFlight} handlers still in flight`,
      );
    }
  }

  private scheduleNextPoll(): void {
    this.scheduleLoop(
      "poll",
      () => this.nextDelayWithBackoff(this.interval, this.pollBackoffAttempts),
      () => this.poll(),
      (t) => { this.timer = t; },
    );
  }

  private scheduleNextStalledCheck(): void {
    this.scheduleLoop(
      "stalled sweep",
      () => this.nextDelayWithBackoff(this.stalledCheckInterval, this.stalledBackoffAttempts),
      () => this.sweepStalled(),
      (t) => { this.stalledTimer = t; },
    );
  }

  /**
   * Base interval when healthy; exponential backoff after errors.
   *
   * Floored at `baseMs` so a slow-cadence poll (e.g. `interval: 60_000`)
   * never retries faster than its configured rate during an outage.
   * Capped at `BACKOFF_MAX_MS` so fast-cadence polls don't grow
   * without bound. The shift is clamped at 32 so the intermediate
   * `baseMs * 2**attempts` can't overflow to `Infinity` during a
   * long outage.
   */
  private nextDelayWithBackoff(baseMs: number, attempts: number): number {
    if (attempts === 0) return baseMs;
    const shift = Math.min(attempts, 32);
    return Math.max(baseMs, Math.min(BACKOFF_MAX_MS, baseMs * 2 ** shift));
  }

  private scheduleLoop(
    label: string,
    getDelay: () => number,
    work: () => Promise<void>,
    storeTimer: (t: ReturnType<typeof setTimeout>) => void,
  ): void {
    if (!this.running) return;
    storeTimer(setTimeout(async () => {
      try {
        await work();
      } catch (err) {
        console.error(`[delaykit] PollingScheduler ${label} loop error:`, err);
      }
      this.scheduleLoop(label, getDelay, work, storeTimer);
    }, getDelay()));
  }

  private async poll(): Promise<void> {
    if (!this.handlers || !this.store) return;

    const budget = this.maxConcurrent - this.inFlight;
    if (budget <= 0) return;

    // stop()'s drain watches this flag: without it, a drain called
    // during an awaited getDueJobs resolves before handlers dispatch.
    this.pollInProgress = true;
    try {
      const dueJobs = await this.store.getDueJobs(budget);
      this.pollBackoffAttempts = 0;
      for (const job of dueJobs) {
        this.inFlight++;
        this.handleJob({ jobId: job.id, version: job.version }).then(
          this.onJobSettled,
          this.onJobError,
        );
      }
    } catch (err) {
      this.pollBackoffAttempts++;
      const nextDelay = this.nextDelayWithBackoff(this.interval, this.pollBackoffAttempts);
      console.error(
        `[delaykit] PollingScheduler poll error: ${err instanceof Error ? err.message : String(err)}; next attempt in ${nextDelay}ms`,
      );
    } finally {
      this.pollInProgress = false;
    }
  }

  private readonly onJobSettled = (): void => {
    this.inFlight--;
  };

  private readonly onJobError = (err: unknown): void => {
    this.inFlight--;
    console.error("[delaykit] Unhandled error processing job:", err);
  };

  private async sweepStalled(): Promise<void> {
    if (!this.handlers || !this.store) return;

    this.sweepInProgress = true;
    try {
      const timeouts = new Map<string, number>();
      for (const [name, entry] of this.handlers) {
        timeouts.set(name, entry.timeoutMs);
      }
      const reclaimed = await this.store.reclaimStalledJobs(timeouts);
      this.stalledBackoffAttempts = 0;

      for (const job of reclaimed) {
        const entry = this.handlers.get(job.handler);
        if (!entry) continue;

        const timeout = timeouts.get(job.handler) ?? DEFAULT_TIMEOUT_MS;
        emitStalled(this._emit ?? undefined, job, timeout + STALLED_GRACE_MS);

        if (job.attempt >= entry.retry.maxAttempts) {
          // Exhausted: mark failed + call onFailure
          await this.store.markRunning(job.id, job.version);
          await this.store.markFailed(job.id, job.version,
            new Error("Job stalled (process crash or timeout)"));
          if (entry.retry.onFailure) {
            try {
              await entry.retry.onFailure({
                key: job.key,
                error: new Error("Job stalled (process crash or timeout)"),
                attempts: job.attempt,
              });
            } catch (e) {
              console.error(`[delaykit] onFailure threw for stalled job ${job.id}:`, e);
            }
          }
        } else if (job.attempt > 0) {
          // Retry reclaim: apply backoff delay.
          // Pattern requeues (attempt=0) already have correct scheduledFor from the store.
          const delay = calculateRetryDelay(entry.retry, job.attempt - 1);
          const nextAt = new Date(Date.now() + delay);
          await this.store.updateScheduledFor(job.id, nextAt);
        }
      }
    } catch (err) {
      this.stalledBackoffAttempts++;
      const nextDelay = this.nextDelayWithBackoff(this.stalledCheckInterval, this.stalledBackoffAttempts);
      console.error(
        `[delaykit] PollingScheduler stalled sweep error: ${err instanceof Error ? err.message : String(err)}; next attempt in ${nextDelay}ms`,
      );
    } finally {
      this.sweepInProgress = false;
    }
  }

  private async handleJob(trigger: TriggerPayload): Promise<void> {
    if (!this.handlers || !this.store) return;

    // timeoutMode: "await" — defer slot release until the handler
    // actually finishes, so the maxConcurrent cap is not exceeded
    // when a handler ignores its abort signal.
    const result = await executeJob(
      trigger,
      this.store,
      this.handlers,
      this._emit ?? undefined,
      { timeoutMode: "await" },
    );
    await handleResult(result, {
      store: this.store,
      handlers: this.handlers,
      schedule: this.schedule.bind(this),
      emit: this._emit ?? undefined,
      deferHorizonMs: this.deferHorizonMs,
    });
  }

  private detectServerless(): void {
    const isVercel = typeof process !== "undefined" && process.env.VERCEL === "1";
    const isLambda =
      typeof process !== "undefined" && !!process.env.AWS_LAMBDA_FUNCTION_NAME;

    if (isVercel || isLambda) {
      throw new Error(
        [
          "PollingScheduler cannot run in serverless environments (no long-running process).",
          "",
          "Options:",
          "  1. Use PosthookScheduler for managed scheduling: https://delaykit.dev/vercel",
          "  2. Run PollingScheduler on a separate VPS with triggerUrl: https://delaykit.dev/split-deploy",
        ].join("\n")
      );
    }
  }
}

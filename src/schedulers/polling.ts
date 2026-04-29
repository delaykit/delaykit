import { executeClaimed } from "../executor.js";
import type { HandlerEntry } from "../executor.js";
import type { Scheduler, SchedulerContext, Store, StopOptions, EmitFn } from "../types.js";
import { DEFAULT_TIMEOUT_MS, DEFER_HORIZON_MS, STALLED_GRACE_MS } from "../types.js";
import { emitStalled, warnUnknownDueHandlers } from "../emitter.js";
import { handleResult, calculateRetryDelay, materializeRescheduledWakes, claimTerminalStall } from "../result-handler.js";

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
   * Maximum number of handlers running concurrently **per instance**.
   * When the in-flight count reaches this cap, `poll()` skips the DB
   * fetch until running handlers settle; excess due jobs stay
   * `pending` in the store and are claimed on subsequent polls.
   *
   * The cluster-wide ceiling is `N × maxConcurrent` for N instances —
   * `claimDueJobs` uses `FOR UPDATE SKIP LOCKED`, so concurrent
   * pollers claim disjoint sets. For a strict global cap, run one
   * instance.
   *
   * Default: 10. Raise for I/O-bound handlers (DB/HTTP). Lower for
   * CPU-bound work that blocks the event loop.
   */
  maxConcurrent?: number;
}

/** Upper bound on the backoff delay. Applied after jitter so the ceiling holds at the positive extreme. */
const BACKOFF_MAX_MS = 30_000;

/**
 * Pure delay calculation for a single backoff iteration.
 *
 * `rand` is the caller's pre-sampled Math.random() value. Passing it in
 * makes the function pure (testable without mocking Math.random).
 *
 * Floored at `baseMs` so a slow-cadence interval never retries faster
 * than its configured rate. Capped at `BACKOFF_MAX_MS` after jitter.
 * The shift is clamped at 32 to prevent `baseMs * 2**attempts` from
 * overflowing to `Infinity` during a long outage.
 */
export function computeBackoffDelay(baseMs: number, attempts: number, rand: number): number {
  if (attempts === 0) return baseMs;
  const shift = Math.min(attempts, 32);
  const delay = Math.max(baseMs, Math.min(BACKOFF_MAX_MS, baseMs * 2 ** shift));
  const jitter = delay * 0.25 * (rand * 2 - 1);
  return Math.max(baseMs, Math.min(BACKOFF_MAX_MS, delay + jitter));
}

class LoopBackoff {
  attempts = 0;
  onSuccess(): void { this.attempts = 0; }
  onFailure(): void { this.attempts++; }
  reset(): void { this.attempts = 0; }
  nextDelay(baseMs: number): number {
    return computeBackoffDelay(baseMs, this.attempts, Math.random());
  }
}

/**
 * Long-running polling scheduler.
 *
 * Multi-instance: claiming uses `Store.claimDueJobs`, which is backed
 * by `FOR UPDATE SKIP LOCKED` in Postgres. Concurrent pollers claim
 * disjoint sets, so running multiple instances against one store
 * scales throughput linearly. `maxConcurrent` becomes per-instance —
 * the cluster ceiling is `N × maxConcurrent`. For a strict global
 * cap, run one instance.
 */
export class PollingScheduler implements Scheduler {
  private interval: number;
  private stalledCheckInterval: number;
  private maxConcurrent: number;
  private inFlight = 0;
  private pollInProgress = false;
  private sweepInProgress = false;
  private pollBackoff = new LoopBackoff();
  private stalledBackoff = new LoopBackoff();
  private store: Store | null = null;
  private handlers: Map<string, PollingHandlerEntry> | null = null;
  private _emit: EmitFn | null = null;
  private deferHorizonMs = DEFER_HORIZON_MS;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stalledTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private stopping: Promise<void> | null = null;

  /**
   * Defaults: `interval: 1000ms`, `stalledCheckInterval: 30000ms`,
   * `maxConcurrent: 10`. Bare `new PollingScheduler()` is the right
   * starting point for most apps; see `PollingSchedulerOptions` for tuning.
   */
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
    this.pollBackoff.reset();
    this.stalledBackoff.reset();
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
    this.scheduleLoop("poll", this.interval, this.pollBackoff, () => this.poll(), (t) => { this.timer = t; });
  }

  private scheduleNextStalledCheck(): void {
    this.scheduleLoop("stalled sweep", this.stalledCheckInterval, this.stalledBackoff, () => this.sweepStalled(), (t) => { this.stalledTimer = t; });
  }

  /**
   * Owns the full loop lifecycle: call work, update backoff state,
   * sample jitter once, log on error, schedule next iteration.
   *
   * `delay` is the pre-sampled delay for THIS timer installation. On
   * the first call it is omitted (base interval, no jitter). Recursive
   * calls always pass the value sampled after the previous work() so
   * the logged and scheduled delays always agree.
   */
  private scheduleLoop(
    label: string,
    baseMs: number,
    backoff: LoopBackoff,
    work: () => Promise<void>,
    storeTimer: (t: ReturnType<typeof setTimeout>) => void,
    delay?: number,
  ): void {
    if (!this.running) return;
    const d = delay ?? backoff.nextDelay(baseMs);
    storeTimer(setTimeout(async () => {
      let failure: unknown = null;
      try {
        await work();
        backoff.onSuccess();
      } catch (err) {
        failure = err;
        backoff.onFailure();
      }
      // Sample jitter once and reuse for both the log and the next
      // timer so the reported delay always matches what is scheduled.
      const nextDelay = backoff.nextDelay(baseMs);
      if (failure !== null) {
        console.error(
          `[delaykit] PollingScheduler ${label} error: ${failure instanceof Error ? failure.message : String(failure)}; next attempt in ${nextDelay}ms`,
        );
      }
      this.scheduleLoop(label, baseMs, backoff, work, storeTimer, nextDelay);
    }, d));
  }

  private async poll(): Promise<void> {
    if (!this.handlers || !this.store) return;

    const budget = this.maxConcurrent - this.inFlight;
    if (budget <= 0) return;

    // stop()'s drain watches this flag: without it, a drain called
    // during an awaited claimDueJobs resolves before handlers dispatch.
    this.pollInProgress = true;
    try {
      const batch = await this.store.claimDueJobs(budget, Array.from(this.handlers.keys()));

      // Dispatch ready jobs FIRST so their `inFlight` increments are
      // immediate — a slow `scheduler.schedule()` in the materialize
      // path must not delay handler dispatch, drain bookkeeping, or
      // the next poll's budget calculation.
      for (const job of batch.toRun) {
        this.inFlight++;
        this.handleJob(job).then(
          this.onJobSettled,
          this.onJobError,
        );
      }

      if (batch.rescheduled.length > 0) {
        try {
          await materializeRescheduledWakes(batch.rescheduled, this.resultDeps());
        } catch (rescheduleErr) {
          console.error(
            `[delaykit] materializeRescheduledWakes error: ${rescheduleErr instanceof Error ? rescheduleErr.message : String(rescheduleErr)}`,
          );
        }
      }
    } finally {
      this.pollInProgress = false;
    }
  }

  private resultDeps() {
    return {
      store: this.store!,
      handlers: this.handlers!,
      schedule: this.schedule.bind(this),
      emit: this._emit ?? undefined,
      deferHorizonMs: this.deferHorizonMs,
    };
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

      for (const job of reclaimed) {
        const entry = this.handlers.get(job.handler);
        if (!entry) continue;

        const timeout = timeouts.get(job.handler) ?? DEFAULT_TIMEOUT_MS;
        emitStalled(this._emit ?? undefined, job, timeout + STALLED_GRACE_MS);

        if (job.attempt >= entry.retry.maxAttempts) {
          const stalledError = await claimTerminalStall(this.store, job.id, job.version);
          if (!stalledError) continue;
          await handleResult(
            { status: "stalled_terminal", error: stalledError, job, startedAt: Date.now() },
            { store: this.store, handlers: this.handlers, schedule: this.schedule.bind(this), emit: this._emit ?? undefined },
          );
        } else if (job.attempt > 0) {
          // Retry reclaim: apply backoff delay.
          // Pattern requeues (attempt=0) already have correct scheduledFor from the store.
          const delay = calculateRetryDelay(entry.retry, job.attempt - 1);
          const nextAt = new Date(Date.now() + delay);
          await this.store.updateScheduledFor(job.id, nextAt);
        }
      }

      await warnUnknownDueHandlers(this.store, Array.from(this.handlers.keys()));
    } finally {
      this.sweepInProgress = false;
    }
  }

  private async handleJob(claimed: import("../types.js").Job): Promise<void> {
    if (!this.handlers || !this.store) return;

    // timeoutMode: "await" — defer slot release until the handler
    // actually finishes, so the maxConcurrent cap is not exceeded
    // when a handler ignores its abort signal.
    const result = await executeClaimed(
      claimed,
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

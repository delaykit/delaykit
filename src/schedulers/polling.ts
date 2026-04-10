import { executeJob } from "../executor.js";
import type { HandlerEntry, TriggerPayload } from "../executor.js";
import type { Scheduler, SchedulerContext, Store, EmitFn } from "../types.js";
import { DEFAULT_TIMEOUT_MS, STALLED_GRACE_MS } from "../types.js";
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
}

export class PollingScheduler implements Scheduler {
  private interval: number;
  private stalledCheckInterval: number;
  private store: Store | null = null;
  private handlers: Map<string, PollingHandlerEntry> | null = null;
  private _emit: EmitFn | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stalledTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  constructor(options?: PollingSchedulerOptions) {
    this.interval = options?.interval ?? 1_000;
    this.stalledCheckInterval = options?.stalledCheckInterval ?? 30_000;
  }

  init(ctx: SchedulerContext): void {
    this.store = ctx.store;
    this.handlers = ctx.handlers as Map<string, PollingHandlerEntry>;
    this._emit = ctx.emit;
  }

  async schedule(_req: unknown): Promise<string | null> {
    return null;
  }

  async cancel(_schedulerRef: string): Promise<void> {}

  async start(): Promise<void> {
    if (this.running) return;
    this.detectServerless();
    this.running = true;
    this.scheduleNextPoll();
    this.scheduleNextStalledCheck();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.stalledTimer) {
      clearTimeout(this.stalledTimer);
      this.stalledTimer = null;
    }
  }

  private scheduleNextPoll(): void {
    if (!this.running) return;
    this.timer = setTimeout(async () => {
      await this.poll();
      this.scheduleNextPoll();
    }, this.interval);
  }

  private async poll(): Promise<void> {
    if (!this.handlers || !this.store) return;

    try {
      const dueJobs = await this.store.getDueJobs(100);
      for (const job of dueJobs) {
        const trigger: TriggerPayload = { jobId: job.id, version: job.version };
        this.handleJob(trigger).catch((err) => {
          console.error(`[delaykit] Unhandled error processing job ${job.id}:`, err);
        });
      }
    } catch (err) {
      console.error("[delaykit] PollingScheduler poll error:", err);
    }
  }

  private scheduleNextStalledCheck(): void {
    if (!this.running) return;
    this.stalledTimer = setTimeout(async () => {
      await this.sweepStalled();
      this.scheduleNextStalledCheck();
    }, this.stalledCheckInterval);
  }

  private async sweepStalled(): Promise<void> {
    if (!this.handlers || !this.store) return;

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
      console.error("[delaykit] PollingScheduler stalled sweep error:", err);
    }
  }

  private async handleJob(trigger: TriggerPayload): Promise<void> {
    if (!this.handlers || !this.store) return;

    const result = await executeJob(trigger, this.store, this.handlers, this._emit ?? undefined);
    await handleResult(result, {
      store: this.store,
      handlers: this.handlers,
      schedule: this.schedule.bind(this),
      emit: this._emit ?? undefined,
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

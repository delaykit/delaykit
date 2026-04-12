import type { HandlerContext, Job, Store, EmitFn } from "./types.js";
import { DEFAULT_TIMEOUT_MS, STALLED_GRACE_MS } from "./types.js";
import { emitStalled } from "./emitter.js";

export interface HandlerEntry {
  fn: (ctx: HandlerContext) => Promise<void>;
  timeoutMs: number;
}

export type ExecutionResult =
  | { status: "completed"; job: Job; startedAt: number }
  | { status: "handler_succeeded"; job: Job; startedAt: number }
  | { status: "handler_error"; error: Error; job: Job; startedAt: number }
  | { status: "needs_reschedule"; job: Job }
  | { status: "skipped" };

export interface TriggerPayload {
  jobId: string;
  version: number;
}

export interface ExecuteOptions {
  /**
   * How the per-handler timeout interacts with handler completion:
   *
   * - `"race"` (default): reject as soon as the timer fires. The
   *   handler's underlying promise is left running. Use for
   *   request-scoped callers (`dk.poll()`, `createHandler()`) that
   *   must return a response before the platform deadline.
   * - `"await"`: abort the signal on timer fire but keep awaiting
   *   `fn(ctx)`; throw the timeout error after it actually settles.
   *   Use for callers that need backpressure (PollingScheduler) so
   *   the caller can defer "I'm done with this slot" until the work
   *   really stops.
   */
  timeoutMode?: "race" | "await";
}

/**
 * Claims a job and runs the handler. Returns a decision signal
 * for the scheduler to act on.
 *
 * For kind='once': marks completed on success, returns "completed".
 * For patterns: returns "handler_succeeded" — the scheduler handles
 *   the markCompleted/requeueForNextWindow decision.
 * For debounce not settled: returns "needs_reschedule" — the scheduler
 *   does scheduler-first + rescheduleDueAt.
 * On failure: returns "handler_error" — the scheduler decides retry vs terminal.
 */
export async function executeJob(
  trigger: TriggerPayload,
  store: Store,
  handlers: Map<string, HandlerEntry>,
  emit?: EmitFn,
  options?: ExecuteOptions,
): Promise<ExecutionResult> {
  let job = await store.getJob(trigger.jobId);
  if (!job) return { status: "skipped" };

  if (job.version !== trigger.version) return { status: "skipped" };

  // Inline stalled recovery: if the row is running with an expired lease,
  // reclaim it before the pending check. This handles the case where a
  // previous process died mid-handler and a redelivery arrives.
  if (job.status === "running" && job.startedAt) {
    const entry = handlers.get(job.handler);
    const leaseMs = (entry?.timeoutMs ?? DEFAULT_TIMEOUT_MS) + STALLED_GRACE_MS;
    const stalledMs = Date.now() - job.startedAt.getTime();
    const reclaimed = await store.reclaimStalled(job.id, leaseMs);
    if (reclaimed) {
      emitStalled(emit, job, stalledMs);

      // Check if retry budget is exhausted after reclaim
      if (reclaimed.attempt >= reclaimed.maxAttempts) {
        await store.markRunning(reclaimed.id, reclaimed.version);
        const error = new Error("Job stalled (process crash or timeout)");
        await store.markFailed(reclaimed.id, reclaimed.version, error);
        return { status: "handler_error", error, job: reclaimed, startedAt: Date.now() };
      }
      job = reclaimed;
    }
  }

  if (job.status !== "pending") return { status: "skipped" };

  // Debounce settlement check (before claiming)
  if (job.kind === "debounce") {
    const now = Date.now();
    const settled = job.lastAt != null && (now - job.lastAt.getTime()) >= (job.waitMs ?? 0);
    const maxWaitExceeded = job.maxWaitMs != null && job.firstAt != null &&
      (now - job.firstAt.getTime()) >= job.maxWaitMs;

    if (!settled && !maxWaitExceeded) {
      return { status: "needs_reschedule", job };
    }
  }

  const entry = handlers.get(job.handler);
  if (!entry) {
    console.error(`[delaykit] No handler registered for "${job.handler}". Job ${job.id} marked failed.`);
    await store.markRunning(job.id, job.version);
    await store.markFailed(job.id, job.version, new Error(`No handler registered for "${job.handler}"`));
    return { status: "handler_error", error: new Error(`No handler registered for "${job.handler}"`), job, startedAt: Date.now() };
  }

  const claimed = await store.markRunning(job.id, job.version);
  if (!claimed) return { status: "skipped" };

  const startedAt = Date.now();
  const startedDate = new Date(startedAt);

  emit?.({
    type: "job:started",
    job: { ...job, status: "running", claimedVersion: job.version, startedAt: startedDate },
    timestamp: startedDate,
    attempt: job.attempt,
  });

  const ac = new AbortController();
  const ctx: HandlerContext = { key: job.key, job, signal: ac.signal };

  try {
    await executeWithTimeout(entry.fn, ctx, ac, entry.timeoutMs, options?.timeoutMode ?? "race");

    if (job.kind === "once") {
      await store.markCompleted(job.id, job.version);
      return { status: "completed", job, startedAt };
    }

    // Pattern: return decision signal for scheduler to handle
    return { status: "handler_succeeded", job, startedAt };
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    return { status: "handler_error", error, job, startedAt };
  }
}

function timeoutError(handler: string, timeoutMs: number): Error {
  return new Error(`Handler "${handler}" timed out after ${timeoutMs}ms`);
}

function executeWithTimeout(
  fn: (ctx: HandlerContext) => Promise<void>,
  ctx: HandlerContext,
  ac: AbortController,
  timeoutMs: number,
  mode: "race" | "await",
): Promise<void> {
  if (mode === "await") return runAwaitMode(fn, ctx, ac, timeoutMs);
  return runRaceMode(fn, ctx, ac, timeoutMs);
}

async function runAwaitMode(
  fn: (ctx: HandlerContext) => Promise<void>,
  ctx: HandlerContext,
  ac: AbortController,
  timeoutMs: number,
): Promise<void> {
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    ac.abort();
  }, timeoutMs);

  let handlerError: unknown;
  let handlerThrew = false;
  try {
    await fn(ctx);
  } catch (err) {
    handlerError = err;
    handlerThrew = true;
  } finally {
    clearTimeout(timer);
  }

  if (timedOut) throw timeoutError(ctx.job.handler, timeoutMs);
  if (handlerThrew) throw handlerError;
}

function runRaceMode(
  fn: (ctx: HandlerContext) => Promise<void>,
  ctx: HandlerContext,
  ac: AbortController,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      ac.abort();
      reject(timeoutError(ctx.job.handler, timeoutMs));
    }, timeoutMs);

    fn(ctx)
      .then(resolve, reject)
      .finally(() => clearTimeout(timer));
  });
}

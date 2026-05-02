import type { HandlerContext, Job, RescheduleOptions, Store, EmitFn } from "./types.js";
import { DEFAULT_TIMEOUT_MS, SCHEDULE_MAX_FUTURE_MS, STALLED_GRACE_MS, asError, isDebounceSettled } from "./types.js";
import { parseDuration } from "./duration.js";
import { cloneJobForEvent, emitStalled } from "./emitter.js";
import { claimTerminalStall } from "./result-handler.js";

export interface HandlerEntry {
  fn: (ctx: HandlerContext) => Promise<void>;
  timeoutMs: number;
}

export type ExecutionResult =
  | { status: "completed"; job: Job; startedAt: number }
  | { status: "handler_succeeded"; job: Job; startedAt: number }
  | { status: "handler_rescheduled"; job: Job; scheduledFor: Date; startedAt: number }
  | { status: "handler_error"; error: Error; job: Job; startedAt: number; isTimeout: boolean }
  | { status: "stalled_terminal"; error: Error; job: Job; startedAt: number }
  | { status: "needs_reschedule"; job: Job }
  | { status: "handler_not_registered"; job: Job }
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
 * Wake-delivery entry point. Loads the row, validates, claims, runs.
 * Used by `createHandler()` (PosthookScheduler webhook delivery).
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

      if (reclaimed.attempt >= reclaimed.maxAttempts) {
        const error = await claimTerminalStall(store, reclaimed.id, reclaimed.version);
        if (!error) return { status: "skipped" };
        return { status: "stalled_terminal", error, job: reclaimed, startedAt: Date.now() };
      }
      job = reclaimed;
    }
  }

  if (job.status !== "pending") return { status: "skipped" };

  if (job.kind === "debounce" && !isDebounceSettled(job, Date.now())) {
    return { status: "needs_reschedule", job };
  }

  const entry = handlers.get(job.handler);
  if (!entry) {
    return { status: "handler_not_registered", job };
  }

  const claimed = await store.markRunning(job.id, job.version);
  if (!claimed) return { status: "skipped" };

  // markRunning flipped the DB row; reflect the same on our local
  // snapshot so runClaimedRow's handler ctx carries running state.
  const running: Job = {
    ...job,
    status: "running",
    claimedVersion: job.version,
    startedAt: new Date(),
  };
  return runClaimedRow(running, entry, store, emit, options);
}

/**
 * Poll-path entry point. `claimDueJobs` already filtered candidates
 * by registered handler names, did the settlement check in-query,
 * and flipped status to `running`. Just emit and run.
 */
export async function executeClaimed(
  claimed: Job,
  store: Store,
  handlers: Map<string, HandlerEntry>,
  emit?: EmitFn,
  options?: ExecuteOptions,
): Promise<ExecutionResult> {
  const entry = handlers.get(claimed.handler);
  if (!entry) {
    // Unreachable under normal operation (the claim query filters by
    // handlerNames) — but guard defensively if a handler is
    // de-registered between claim and dispatch.
    return { status: "handler_not_registered", job: claimed };
  }
  return runClaimedRow(claimed, entry, store, emit, options);
}

/**
 * Shared "run already-claimed row" path. Emits `job:started`, builds
 * the handler context, runs the handler under a timeout, and returns
 * the appropriate `ExecutionResult`. Row arrives in `running` state
 * from either `markRunning` (wake path) or `claimDueJobs` (poll path).
 */
async function runClaimedRow(
  job: Job,
  entry: HandlerEntry,
  store: Store,
  emit: EmitFn | undefined,
  options: ExecuteOptions | undefined,
): Promise<ExecutionResult> {
  const startedAt = job.startedAt ? job.startedAt.getTime() : Date.now();
  const startedDate = new Date(startedAt);

  emit?.({
    type: "job:started",
    job: cloneJobForEvent(job),
    timestamp: startedDate,
    attempt: job.attempt,
  });

  const ac = new AbortController();
  let rescheduleIntent: Date | null = null;
  const reschedule = (options: RescheduleOptions): void => {
    if (job.kind !== "once") {
      throw new Error(
        `ctx.reschedule is only supported on kind="once" handlers; "${job.handler}" is a ${job.kind} pattern. Pattern handlers requeue automatically via their wait/maxWait window.`,
      );
    }
    rescheduleIntent = resolveRescheduleAt(options);
  };
  const ctx: HandlerContext = {
    key: job.key,
    job: cloneJobForEvent(job),
    signal: ac.signal,
    reschedule,
  };

  try {
    await executeWithTimeout(entry.fn, ctx, ac, entry.timeoutMs, options?.timeoutMode ?? "race");

    if (rescheduleIntent !== null) {
      return { status: "handler_rescheduled", job, scheduledFor: rescheduleIntent, startedAt };
    }

    if (job.kind === "once") {
      await store.markCompleted(job.id, job.version);
      return { status: "completed", job, startedAt };
    }

    return { status: "handler_succeeded", job, startedAt };
  } catch (err) {
    return { status: "handler_error", error: asError(err), job, startedAt, isTimeout: ac.signal.aborted };
  }
}

function resolveRescheduleAt(options: RescheduleOptions): Date {
  const hasDelay = "delay" in options && options.delay !== undefined;
  const hasAt = "at" in options && options.at !== undefined;
  if (!hasDelay && !hasAt) {
    throw new Error('ctx.reschedule requires either "delay" (e.g., "2m") or "at" (Date).');
  }
  if (hasDelay && hasAt) {
    throw new Error('ctx.reschedule: provide either "delay" or "at", not both.');
  }
  if (hasAt) {
    const at = options.at as Date;
    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new Error(`ctx.reschedule: invalid "at" Date: ${String(at)}.`);
    }
    if (at.getTime() - Date.now() > SCHEDULE_MAX_FUTURE_MS) {
      throw new Error(
        'ctx.reschedule: "at" is more than 10 years in the future — likely a unit mistake (seconds vs ms, or wrong year).',
      );
    }
    return at;
  }
  return new Date(Date.now() + parseDuration(options.delay as string));
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

    // After timer wins the race the outer promise is settled, so any
    // later resolve/reject from fn(ctx) is a no-op. The trailing
    // .catch keeps that no-op silent — Node's unhandledRejection
    // warning has been observed firing on the orphan tail in some
    // runtimes despite the handler being attached.
    fn(ctx)
      .then(resolve, reject)
      .catch(() => {})
      .finally(() => clearTimeout(timer));
  });
}

import type { ExecutionResult } from "./executor.js";
import {
  DEFER_HORIZON_MS, DEFER_INITIAL_MS, DEFER_MAX_MS,
  type Store, type Job, type EmitFn, type ScheduleRequest, type SchedulerRetryConfig,
} from "./types.js";
import type { PollingHandlerEntry, RetryConfig } from "./schedulers/polling.js";

export interface ResultHandlerDeps {
  store: Store;
  handlers: Map<string, PollingHandlerEntry>;
  schedule: (req: ScheduleRequest) => Promise<string | null>;
  cancel?: (schedulerRef: string) => Promise<void>;
  /** When true, the external scheduler owns retry timing (PosthookScheduler).
   *  Handler failures just return "retry" without calling retryJob(). */
  externalRetries?: boolean;
  emit?: EmitFn;
  /** Defer horizon in ms. Falls back to the default 24h if not provided. */
  deferHorizonMs?: number;
}

/**
 * Shared post-execution result handling for both PollingScheduler and webhook delivery.
 *
 * Returns:
 * - "ok" — completed, requeued, or skipped (caller returns 200)
 * - "retry" — handler failed with retries remaining (caller returns 500 for Posthook retry)
 */
export async function handleResult(
  result: ExecutionResult,
  deps: ResultHandlerDeps,
): Promise<"ok" | "retry"> {
  if (result.status === "skipped") {
    return "ok";
  }

  if (result.status === "completed") {
    const now = Date.now();
    deps.emit?.({
      type: "job:completed",
      job: { ...result.job, status: "completed" },
      timestamp: new Date(now),
      durationMs: now - result.startedAt,
    });
    return "ok";
  }

  if (result.status === "stalled_terminal") {
    const now = Date.now();
    deps.emit?.({
      type: "job:failed",
      job: { ...result.job, status: "failed" },
      timestamp: new Date(now),
      error: result.error,
      attempts: result.job.attempt,
      durationMs: now - result.startedAt,
      reason: "timeout",
    });
    const entry = deps.handlers.get(result.job.handler);
    if (entry?.retry.onFailure) {
      try {
        await entry.retry.onFailure({ key: result.job.key, error: result.error, attempts: result.job.attempt });
      } catch (e) {
        console.error(`[delaykit] onFailure handler threw for job ${result.job.id}:`, e);
      }
    }
    return "ok";
  }

  if (result.status === "needs_reschedule") {
    // Only the wake path returns this variant — the poll path's
    // settlement check is fused into `claimDueJobs` so an un-settled
    // debounce row never reaches the executor.
    const updated = await deps.store.rescheduleDueAt(result.job.id, result.job.version);
    if (updated) {
      await materializeWake(updated, retryFromEntry(updated.handler, deps.handlers), deps);
    }
    return "ok";
  }

  if (result.status === "handler_not_registered") {
    // Primarily the wake path — the poll path filters unregistered
    // handlers out of the claim candidates. Also a defensive fallback
    // if a handler is de-registered between claim and dispatch.
    const nextAttempts = result.job.deferAttempts + 1;
    const scheduledFor = new Date(Date.now() + computeDeferBackoffMs(nextAttempts));
    const deferredError = `Handler "${result.job.handler}" is not registered at delivery time`;
    const terminalError = `Handler "${result.job.handler}" was not registered for the defer horizon; job flipped to failed. Register the handler, or cancel the job manually.`;
    const horizonMs = deps.deferHorizonMs ?? DEFER_HORIZON_MS;

    const updated = await deps.store.deferJob(
      result.job.id,
      result.job.version,
      scheduledFor,
      deferredError,
      terminalError,
      horizonMs,
    );
    if (!updated) return "ok";

    if (updated.status === "failed") {
      deps.emit?.({
        type: "job:failed",
        job: { ...updated },
        timestamp: new Date(),
        error: new Error(updated.lastError!),
        attempts: updated.attempt,
        durationMs: 0,
        reason: "defer_horizon",
      });
      return "ok";
    }

    deps.emit?.({
      type: "job:deferred",
      job: { ...updated },
      timestamp: new Date(),
      deferAttempts: updated.deferAttempts,
      nextAttemptAt: updated.scheduledFor,
    });
    console.error(
      `[delaykit] Handler "${updated.handler}" not registered — deferring job ${updated.id} (attempt ${updated.deferAttempts}) until ${scheduledFor.toISOString()}`,
    );
    await materializeWake(updated, updated.retryConfig ?? undefined, deps);
    return "ok";
  }

  if (result.status === "handler_succeeded") {
    const completed = await deps.store.markCompleted(result.job.id, result.job.version);
    if (completed) {
      const now = Date.now();
      deps.emit?.({
        type: "job:completed",
        job: { ...result.job, status: "completed" },
        timestamp: new Date(now),
        durationMs: now - result.startedAt,
      });
    } else if (result.job.kind !== "once") {
      const requeued = await deps.store.requeueForNextWindow(result.job.id);
      if (requeued) {
        await materializeWake(requeued, retryFromEntry(requeued.handler, deps.handlers), deps);
      }
    }
    return "ok";
  }

  if (result.status === "handler_error") {
    const entry = deps.handlers.get(result.job.handler);
    if (!entry) {
      await deps.store.markFailed(result.job.id, result.job.version, result.error);
      return "ok";
    }

    if (result.job.attempt + 1 < entry.retry.maxAttempts) {
      const scheduledFor = deps.externalRetries
        ? new Date()
        : new Date(Date.now() + calculateRetryDelay(entry.retry, result.job.attempt));

      const retried = await deps.store.retryJob(
        result.job.id, result.job.version,
        result.job.attempt + 1,
        scheduledFor,
        result.error.message,
      );
      if (retried) {
        deps.emit?.({
          type: "job:retrying",
          job: { ...result.job },
          timestamp: new Date(),
          error: result.error,
          attempt: result.job.attempt,
          nextAttempt: result.job.attempt + 1,
          scheduledFor,
        });
      } else if (result.job.kind !== "once") {
        const requeued = await deps.store.requeueForNextWindow(result.job.id);
        if (requeued) {
          await materializeWake(requeued, retryFromEntry(requeued.handler, deps.handlers), deps);
        }
        return "ok";
      }
      return deps.externalRetries ? "retry" : "ok";
    }

    // Exhausted: terminal failure.
    //
    // `markFailed` CAS can lose for two distinct reasons:
    //   (a) pattern-event race — a new event bumped the version via
    //       updatePatternEvent while the handler was running. The
    //       OLD window did fail; a NEW window is starting.
    //       `requeueForNextWindow` succeeds (status is still
    //       `running`). We still owe `onFailure` for the old window.
    //   (b) stalled-reclaim race — sweepStalled already reclaimed and
    //       marked this row failed, so `onFailure` has ALREADY fired
    //       there. `requeueForNextWindow` fails (status is `failed`).
    //       Calling `onFailure` again would double-fire alerting or
    //       cleanup the user wired up.
    //
    // Gate `onFailure` on either winning the CAS ourselves (we own
    // the terminal transition) or successfully requeueing (case a).
    const failed = await deps.store.markFailed(result.job.id, result.job.version, result.error);
    let fireOnFailure = failed;

    if (failed) {
      const now = Date.now();
      deps.emit?.({
        type: "job:failed",
        job: { ...result.job, status: "failed" },
        timestamp: new Date(now),
        error: result.error,
        attempts: result.job.attempt + 1,
        durationMs: now - result.startedAt,
        reason: result.isTimeout ? "timeout" : "handler_error",
      });
    } else if (result.job.kind !== "once") {
      const requeued = await deps.store.requeueForNextWindow(result.job.id);
      if (requeued) {
        await materializeWake(requeued, retryFromEntry(requeued.handler, deps.handlers), deps);
        fireOnFailure = true;
      }
    }

    if (fireOnFailure && entry.retry.onFailure) {
      try {
        await entry.retry.onFailure({
          key: result.job.key,
          error: result.error,
          attempts: result.job.attempt + 1,
        });
      } catch (e) {
        console.error(`[delaykit] onFailure handler threw for job ${result.job.id}:`, e);
      }
    }
    return "ok";
  }

  return "ok";
}

/**
 * Fire `materializeWake` for each un-settled debounce row returned by
 * `claimDueJobs` in its `rescheduled` bucket. PollingScheduler's
 * `schedule()` is a no-op (returns null), so for pure poll deployments
 * this iterates but does no external work. For external-scheduler
 * deployments (`dk.poll()` + Posthook), it creates the replacement
 * hook at the advanced `scheduled_for`.
 */
export async function materializeRescheduledWakes(
  rescheduled: Job[],
  deps: ResultHandlerDeps,
): Promise<void> {
  await Promise.all(
    rescheduled.map((job) =>
      materializeWake(job, retryFromEntry(job.handler, deps.handlers), deps),
    ),
  );
}

/**
 * Schedule an external wake and store the ref (version-guarded).
 * If the version advanced while we were creating the hook (e.g., a new
 * pattern event arrived during the network round-trip), the CAS fails.
 * Cancel the orphaned hook so it doesn't deliver and get silently ignored.
 */
async function materializeWake(
  job: Job,
  retry: SchedulerRetryConfig | undefined,
  deps: ResultHandlerDeps,
): Promise<void> {
  const ref = await deps.schedule({
    id: job.id, version: job.version, at: job.scheduledFor,
    handler: job.handler, key: job.key, retry,
  });
  if (!ref) return;
  const stored = await deps.store.updateSchedulerRef(job.id, job.version, ref);
  if (!stored && deps.cancel) {
    try { await deps.cancel(ref); } catch { /* best-effort cleanup */ }
  }
}

/**
 * Own the pending→running→failed CAS chain for an exhausted stalled job.
 * Returns the stall Error on success, null if another actor won the race.
 */
export async function claimTerminalStall(
  store: Store,
  id: string,
  version: number,
): Promise<Error | null> {
  const running = await store.markRunning(id, version);
  if (!running) return null;
  const error = new Error("Job stalled (process crash or timeout)");
  const failed = await store.markFailed(id, version, error);
  if (!failed) return null;
  return error;
}

/** Retry config from the registered handler's config (normal paths). */
function retryFromEntry(
  handler: string,
  handlers: Map<string, PollingHandlerEntry>,
): SchedulerRetryConfig | undefined {
  const entry = handlers.get(handler);
  if (entry && entry.retry.maxAttempts > 1) {
    return { ...entry.retry, attempts: entry.retry.maxAttempts };
  }
  return undefined;
}

export function computeDeferBackoffMs(attempts: number): number {
  if (attempts <= 0) return DEFER_INITIAL_MS;
  // Clamp the shift so a long defer streak can't overflow to Infinity.
  const shift = Math.min(attempts - 1, 32);
  return Math.min(DEFER_MAX_MS, DEFER_INITIAL_MS * 2 ** shift);
}

export function calculateRetryDelay(retry: RetryConfig, attempt: number): number {
  let delay: number;
  switch (retry.backoff) {
    case "exponential":
      delay = retry.initialDelayMs * Math.pow(2, attempt);
      break;
    case "linear":
      delay = retry.initialDelayMs * (attempt + 1);
      break;
    case "fixed":
      delay = retry.initialDelayMs;
      break;
  }

  delay = Math.min(delay, retry.maxDelayMs);

  if (retry.jitter) {
    const jitterRange = delay * 0.25;
    delay += (Math.random() * 2 - 1) * jitterRange;
  }

  return Math.max(delay, 0);
}

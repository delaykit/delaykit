import type { ExecutionResult } from "./executor.js";
import type { Store, Job, EmitFn, ScheduleRequest } from "./types.js";
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

  if (result.status === "needs_reschedule") {
    const updated = await deps.store.rescheduleDueAt(result.job.id, result.job.version);
    if (updated) await materializeWake(updated, deps);
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
      if (requeued) await materializeWake(requeued, deps);
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
        if (requeued) await materializeWake(requeued, deps);
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
      });
    } else if (result.job.kind !== "once") {
      const requeued = await deps.store.requeueForNextWindow(result.job.id);
      if (requeued) {
        await materializeWake(requeued, deps);
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
 * Schedule an external wake and store the ref (version-guarded).
 * If the version advanced while we were creating the hook (e.g., a new
 * pattern event arrived during the network round-trip), the CAS fails.
 * Cancel the orphaned hook so it doesn't deliver and get silently ignored.
 */
async function materializeWake(job: Job, deps: ResultHandlerDeps): Promise<void> {
  const entry = deps.handlers.get(job.handler);
  const retry = entry && entry.retry.maxAttempts > 1
    ? { ...entry.retry, attempts: entry.retry.maxAttempts }
    : undefined;
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

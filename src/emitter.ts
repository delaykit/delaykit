import type { FailureReason, Job, JobEventMap, JobEventType, JobEventListener, EmitFn } from "./types.js";

export class JobEventEmitter {
  private listeners = new Map<JobEventType, Set<JobEventListener<any>>>();

  /** Bound emit function — safe to pass as a dependency. Single instance, no closure per access. */
  readonly emit: EmitFn = (event) => {
    const set = this.listeners.get(event.type);
    if (!set || set.size === 0) return;
    // Snapshot before iterating so a listener that calls unsubscribe()
    // on itself or another listener mid-dispatch doesn't change the
    // iteration order (Set's mid-mutation behavior is well-defined
    // but surprising).
    for (const listener of Array.from(set)) {
      try {
        const result: void | Promise<void> = listener(event);
        if (result instanceof Promise) {
          result.catch((err: unknown) => {
            console.error(`[delaykit] Async event listener error for "${event.type}":`, err);
          });
        }
      } catch (err) {
        console.error(`[delaykit] Event listener error for "${event.type}":`, err);
      }
    }
  };

  on<E extends JobEventType>(event: E, listener: JobEventListener<E>): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener);
    return () => { set!.delete(listener); };
  }
}

export function cloneJobForEvent(job: Job): Job {
  return {
    ...job,
    scheduledFor: new Date(job.scheduledFor.getTime()),
    startedAt: cloneDate(job.startedAt),
    completedAt: cloneDate(job.completedAt),
    createdAt: new Date(job.createdAt.getTime()),
    firstAt: cloneDate(job.firstAt),
    lastAt: cloneDate(job.lastAt),
    deferredSince: cloneDate(job.deferredSince),
    retryConfig: job.retryConfig ? { ...job.retryConfig } : null,
  };
}

export function cloneErrorForEvent(error: Error): Error {
  const cloned = new Error(error.message);
  cloned.name = error.name;
  cloned.stack = error.stack;

  const source = error as Error & Record<string, unknown>;
  const target = cloned as Error & Record<string, unknown>;
  if ("cause" in source) target.cause = source.cause;
  for (const key of Object.keys(source)) {
    target[key] = source[key];
  }
  return cloned;
}

export function emitStalled(emit: EmitFn | undefined, job: Job, stalledMs: number): void {
  emit?.({
    type: "job:stalled",
    job: cloneJobForEvent(job),
    timestamp: new Date(),
    stalledMs,
    reclaimed: true,
  });
}

export function emitJobFailed(
  emit: EmitFn | undefined,
  args: { job: Job; error: Error; reason: FailureReason; attempts: number; durationMs: number },
): void {
  const now = new Date();
  emit?.({
    type: "job:failed",
    job: { ...cloneJobForEvent(args.job), status: "failed", failureReason: args.reason },
    timestamp: now,
    error: cloneErrorForEvent(args.error),
    attempts: args.attempts,
    durationMs: args.durationMs,
    reason: args.reason,
  });
}

export function emitJobRequeued(
  emit: EmitFn | undefined,
  args: {
    job: Job;
    outcome: "succeeded" | "failed_with_retries" | "failed_exhausted";
    error?: Error;
    attempts: number;
    durationMs: number;
  },
): void {
  emit?.({
    type: "job:requeued",
    job: cloneJobForEvent(args.job),
    timestamp: new Date(),
    outcome: args.outcome,
    error: args.error ? cloneErrorForEvent(args.error) : undefined,
    attempts: args.attempts,
    durationMs: args.durationMs,
  });
}

function cloneDate(date: Date | null): Date | null {
  return date ? new Date(date.getTime()) : null;
}

/**
 * Log a warning when due rows exist for handlers that aren't registered
 * on this replica. Called by both `PollingScheduler.sweepStalled` and
 * `dk.poll()` so serverless and long-running deployments surface the
 * same operator signal.
 */
export async function warnUnknownDueHandlers(
  store: { unknownDueHandlers(known: string[]): Promise<string[]> },
  handlerNames: string[],
): Promise<void> {
  try {
    const unknown = await store.unknownDueHandlers(handlerNames);
    if (unknown.length > 0) {
      console.warn(
        `[delaykit] Due rows exist for handlers not registered on this replica: ${unknown.join(", ")}. If no replica has these handlers registered, rows will sit pending.`,
      );
    }
  } catch (err) {
    console.error(
      `[delaykit] unknownDueHandlers error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

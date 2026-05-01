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

export function emitStalled(emit: EmitFn | undefined, job: Job, stalledMs: number): void {
  emit?.({
    type: "job:stalled",
    job: { ...job },
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
    job: { ...args.job, status: "failed", failureReason: args.reason },
    timestamp: now,
    error: args.error,
    attempts: args.attempts,
    durationMs: args.durationMs,
    reason: args.reason,
  });
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

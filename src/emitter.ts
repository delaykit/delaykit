import type { Job, JobEventMap, JobEventType, JobEventListener, EmitFn } from "./types.js";

export class JobEventEmitter {
  private listeners = new Map<JobEventType, Set<JobEventListener<any>>>();

  /** Bound emit function — safe to pass as a dependency. Single instance, no closure per access. */
  readonly emit: EmitFn = (event) => {
    const set = this.listeners.get(event.type);
    if (!set || set.size === 0) return;
    for (const listener of set) {
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

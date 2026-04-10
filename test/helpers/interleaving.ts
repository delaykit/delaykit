import type { Store } from "../../src/types.js";

/**
 * A barrier that blocks until released. Used to force specific
 * orderings between concurrent operations.
 */
export class Barrier {
  private _resolve!: () => void;
  private _promise: Promise<void>;
  private _released = false;

  constructor() {
    this._promise = new Promise<void>((resolve) => {
      this._resolve = resolve;
    });
  }

  /** Block until release() is called. */
  async wait(): Promise<void> {
    if (this._released) return;
    await this._promise;
  }

  /** Unblock all waiters. */
  release(): void {
    this._released = true;
    this._resolve();
  }

  get released(): boolean {
    return this._released;
  }
}

/**
 * Intercept a store method with a barrier. The method will block
 * at the barrier before executing, letting tests control timing.
 *
 * Returns a cleanup function to restore the original method.
 *
 * @example
 * ```ts
 * const barrier = new Barrier();
 * const restore = interceptBefore(store, "createJob", barrier);
 *
 * // This call will block at the barrier
 * const promise = store.createJob({ ... });
 *
 * // Do something else while createJob is blocked
 * await otherOperation();
 *
 * // Let createJob proceed
 * barrier.release();
 * await promise;
 *
 * restore();
 * ```
 */
export function interceptBefore(
  store: Store,
  method: keyof Store,
  barrier: Barrier,
): () => void {
  const original = (store as any)[method].bind(store);
  (store as any)[method] = async (...args: any[]) => {
    await barrier.wait();
    return original(...args);
  };
  return () => {
    (store as any)[method] = original;
  };
}

/**
 * Intercept a store method with a callback that runs AFTER the method.
 * Useful for injecting side effects between two sequential operations.
 */
export function interceptAfter(
  store: Store,
  method: keyof Store,
  callback: () => Promise<void>,
): () => void {
  const original = (store as any)[method].bind(store);
  (store as any)[method] = async (...args: any[]) => {
    const result = await original(...args);
    await callback();
    return result;
  };
  return () => {
    (store as any)[method] = original;
  };
}

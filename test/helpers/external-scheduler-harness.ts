import type { Scheduler, ScheduleRequest } from "../../src/types.js";

export interface ScheduledHook {
  ref: string;
  id: string;
  version: number;
  at: Date;
  key?: string;
  cancelled: boolean;
}

/**
 * Deterministic external scheduler for testing.
 * Models real Posthook-like scheduled artifacts:
 * - Hooks are created on schedule(), tracked with stable refs
 * - cancel() marks hooks cancelled (not deleted — old refs are preserved)
 * - Tests can deliver hooks via deliver(), deliverEarly(), etc.
 *
 * The harness wraps a createHandler() function to simulate webhook delivery.
 */
export class ExternalSchedulerHarness implements Scheduler {
  private hooks = new Map<string, ScheduledHook>();
  private counter = 0;
  private handler: ((req: Request) => Promise<Response>) | null = null;

  /** Set the handler function (from dk.createHandler()) */
  setHandler(handler: (req: Request) => Promise<Response>): void {
    this.handler = handler;
  }

  // --- Scheduler interface ---

  async schedule(req: ScheduleRequest): Promise<string | null> {
    this.counter++;
    const ref = `hook_${this.counter}`;
    this.hooks.set(ref, { ref, id: req.id, version: req.version, at: req.at, key: req.key, cancelled: false });
    return ref;
  }

  async cancel(schedulerRef: string): Promise<void> {
    const hook = this.hooks.get(schedulerRef);
    if (hook) hook.cancelled = true;
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}

  // Needed for createHandler to work
  readonly signingKey = "test_harness_key";

  verifyDelivery<T = Record<string, unknown>>(
    body: string,
    _headers: Headers | Record<string, string | string[] | undefined>,
  ): { hookId: string; data: T } {
    const parsed = JSON.parse(body);
    return { hookId: parsed.hookId, data: parsed.data as T };
  }

  // --- Test delivery methods ---

  /** Deliver a hook normally (at its scheduled time). */
  async deliver(ref: string): Promise<Response> {
    const hook = this.getHookOrThrow(ref);
    return this.sendDelivery(hook);
  }

  /** Deliver a hook before its scheduledFor time. */
  async deliverEarly(ref: string): Promise<Response> {
    const hook = this.getHookOrThrow(ref);
    return this.sendDelivery(hook);
  }

  /** Deliver the same hook twice (duplicate delivery). */
  async deliverTwice(ref: string): Promise<[Response, Response]> {
    const hook = this.getHookOrThrow(ref);
    const res1 = await this.sendDelivery(hook);
    const res2 = await this.sendDelivery(hook);
    return [res1, res2];
  }

  /** Deliver hooks in a specific order. */
  async deliverInOrder(refs: string[]): Promise<Response[]> {
    const results: Response[] = [];
    for (const ref of refs) {
      const hook = this.getHookOrThrow(ref);
      results.push(await this.sendDelivery(hook));
    }
    return results;
  }

  // --- Inspection ---

  /** Get all non-cancelled hooks. */
  activeHooks(): ScheduledHook[] {
    return [...this.hooks.values()].filter(h => !h.cancelled);
  }

  /** Get the most recent hook for a job id. */
  hookFor(jobId: string): ScheduledHook | undefined {
    return [...this.hooks.values()]
      .filter(h => h.id === jobId)
      .at(-1);
  }

  /** Get all hooks (including cancelled) for a job id. */
  allHooksFor(jobId: string): ScheduledHook[] {
    return [...this.hooks.values()].filter(h => h.id === jobId);
  }

  /** Was this ref cancelled? */
  wasCancelled(ref: string): boolean {
    return this.hooks.get(ref)?.cancelled ?? false;
  }

  /** Reset all state. */
  reset(): void {
    this.hooks.clear();
    this.counter = 0;
  }

  // --- Internal ---

  private getHookOrThrow(ref: string): ScheduledHook {
    const hook = this.hooks.get(ref);
    if (!hook) throw new Error(`No hook with ref "${ref}"`);
    return hook;
  }

  private async sendDelivery(hook: ScheduledHook): Promise<Response> {
    if (!this.handler) {
      throw new Error("No handler set. Call setHandler(dk.createHandler()) first.");
    }

    const body = JSON.stringify({
      hookId: hook.ref,
      data: { jobId: hook.id, key: hook.key },
    });

    const req = new Request("http://localhost/api/delaykit", {
      method: "POST",
      body,
      headers: {
        "Content-Type": "application/json",
        "Posthook-Id": hook.ref,
        "Posthook-Timestamp": String(Math.floor(Date.now() / 1000)),
        "Posthook-Signature": "v1,test_harness",
      },
    });

    return this.handler(req);
  }
}

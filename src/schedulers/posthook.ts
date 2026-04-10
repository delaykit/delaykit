import Posthook from "@posthook/node";
import type { Scheduler, ScheduleRequest, SchedulerRetryConfig } from "../types.js";

export interface PosthookSchedulerOptions {
  /** Posthook API key. */
  apiKey: string;
  /** Posthook signing key for webhook verification. */
  signingKey: string;
  /** Base path where createHandler() is mounted (e.g., '/api/delaykit').
   *  Handler name is appended automatically: '/api/delaykit/send-reminder'. */
  basePath: string;
  /** Override the Posthook API base URL (for development). */
  baseURL?: string;
}

export class PosthookScheduler implements Scheduler {
  private client: Posthook;
  private basePath: string;
  readonly signingKey: string;

  constructor(options: PosthookSchedulerOptions) {
    this.client = new Posthook(options.apiKey, {
      signingKey: options.signingKey,
      ...(options.baseURL ? { baseURL: options.baseURL } : {}),
    });
    this.basePath = options.basePath.replace(/\/+$/, ""); // strip trailing slash
    this.signingKey = options.signingKey;
  }

  /** Maximum total attempts Posthook supports (1 initial + 15 retries). */
  readonly maxAttempts = 16;

  async schedule(req: ScheduleRequest): Promise<string | null> {
    const hook = await this.client.hooks.schedule({
      path: `${this.basePath}/${encodeURIComponent(req.handler)}`,
      postAt: req.at.toISOString(),
      data: { jobId: req.id, ...(req.key ? { key: req.key } : {}) },
      ...(req.retry && req.retry.attempts > 1 ? { retryOverride: toPosthookRetry(req.retry) } : {}),
    });
    return hook.id;
  }

  async cancel(schedulerRef: string): Promise<void> {
    await this.client.hooks.delete(schedulerRef);
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}

  verifyDelivery<T = Record<string, unknown>>(
    body: string,
    headers: Headers | Record<string, string | string[] | undefined>,
  ): { hookId: string; data: T } {
    const delivery = this.client.signatures.parseDelivery<T>(body, headers);
    return { hookId: delivery.hookId, data: delivery.data };
  }
}

function toPosthookRetry(retry: SchedulerRetryConfig) {
  const strategy = retry.backoff === "exponential" ? "exponential" as const : "fixed" as const;
  const delaySecs = Math.min(Math.max(5, Math.ceil(retry.initialDelayMs / 1000)), 60);
  const minRetries = Math.min(retry.attempts - 1, 15);

  return {
    minRetries,
    delaySecs,
    strategy,
    jitter: retry.jitter,
    ...(strategy === "exponential" ? {
      maxDelaySecs: Math.max(60, Math.min(Math.ceil(retry.maxDelayMs / 1000), 3600)),
    } : {}),
  };
}

import type { Job } from "./types.js";

/**
 * Trailing-edge settle time for a debounce window. `firstAt +
 * maxWaitMs` clamps the result if a long burst would otherwise push
 * `lastAt + waitMs` past the maxWait deadline.
 */
export function computeDebounceSettlesAt(
  firstAt: Date,
  lastAt: Date,
  waitMs: number,
  maxWaitMs: number | null,
): Date {
  const trailing = lastAt.getTime() + waitMs;
  if (maxWaitMs == null) return new Date(trailing);
  const deadline = firstAt.getTime() + maxWaitMs;
  return new Date(Math.min(trailing, deadline));
}

/**
 * Next `scheduledFor` for a pattern row after an event, a stalled-job
 * reclaim, or a requeue. Throttle fires at a fixed `firstAt + waitMs`
 * window; debounce uses `computeDebounceSettlesAt`.
 *
 * The SQL stores encode the equivalent formula in their NEXT_WINDOW
 * fragments. Store-contract tests enforce parity across stores.
 */
export function computePatternDueAt(job: Job): Date {
  if (job.kind === "throttle") {
    return new Date(job.firstAt!.getTime() + job.waitMs!);
  }
  return computeDebounceSettlesAt(job.firstAt!, job.lastAt!, job.waitMs!, job.maxWaitMs);
}

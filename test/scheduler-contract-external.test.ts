/**
 * Scheduler contract: ExternalSchedulerHarness transport.
 *
 * Uses fake timers for setTimeout inside handlers, plus deterministic
 * hook delivery via the harness. advance() delivers all hooks whose
 * scheduledFor <= simulated now, modeling real Posthook delivery.
 */

import { vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { ExternalSchedulerHarness } from "./helpers/external-scheduler-harness.js";
import { schedulerContractSuite } from "./scheduler-contract.js";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

schedulerContractSuite("ExternalSchedulerHarness", () => {
  // Capabilities: delivery blocks until handler completes (no async handler interleaving),
  // and no stalled sweep (Posthook handles recovery via redelivery).

  const store = new MemoryStore();
  const harness = new ExternalSchedulerHarness();
  const dk = new DelayKit({ store, scheduler: harness });

  let handlerSet = false;

  return {
    dk,
    store,
    advance: async (ms: number) => {
      // Lazy init: createHandler() freezes the handler map, so wait until
      // the test has registered its handlers before calling it.
      if (!handlerSet) {
        harness.setHandler(dk.createHandler());
        handlerSet = true;
      }

      // Advance fake timers first (for setTimeout inside handlers)
      await vi.advanceTimersByTimeAsync(ms);

      // Deliver all hooks whose scheduledFor <= Date.now() (fake timer time).
      // Iterate until no more due hooks (delivering a hook may schedule new ones).
      const now = Date.now();
      const delivered = new Set<string>();
      let progress = true;

      while (progress) {
        progress = false;
        for (const hook of harness.activeHooks()) {
          if (!delivered.has(hook.ref) && hook.at.getTime() <= now) {
            delivered.add(hook.ref);
            await harness.deliver(hook.ref);
            progress = true;
          }
        }
      }
    },
    teardown: async () => {
      harness.reset();
      await store.close();
    },
  };
}, { asyncHandlers: false, stalledRecovery: false });

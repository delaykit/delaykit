/**
 * Scheduler contract: PollingScheduler transport.
 *
 * Uses fake timers to advance time deterministically.
 */

import { vi, beforeEach, afterEach } from "vitest";
import { DelayKit } from "../src/delaykit.js";
import { MemoryStore } from "../src/stores/memory.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { schedulerContractSuite } from "./scheduler-contract.js";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

schedulerContractSuite("PollingScheduler", () => {
  const store = new MemoryStore();
  const scheduler = new PollingScheduler({ interval: 50, stalledCheckInterval: 200 });
  const dk = new DelayKit({ store, scheduler });
  let started = false;

  return {
    dk,
    store,
    advance: async (ms: number) => {
      // Lazy start: handlers must be registered before start() freezes the map
      if (!started) {
        await dk.start();
        started = true;
      }
      await vi.advanceTimersByTimeAsync(ms);
    },
    teardown: () => dk.stop(),
  };
});

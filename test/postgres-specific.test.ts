/**
 * Postgres-specific tests beyond the shared store contract.
 *
 * Covers: migration idempotency, SQL uniqueness constraint enforcement.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { PostgresStore } from "../src/stores/postgres.js";
import { makeJob } from "./helpers/job-factory.js";

const TEST_URL = "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

let store: PostgresStore;

beforeAll(async () => {
  store = await PostgresStore.connect(TEST_URL);
});

afterAll(async () => {
  await store.close();
});

beforeEach(async () => {
  await (store as any).sql`DELETE FROM delaykit.jobs`;
});

describe("PostgresStore: migrations", () => {
  it("creates schema and tables on connect (idempotent)", async () => {
    // Second connect should succeed without error — migration is idempotent
    const second = await PostgresStore.connect(TEST_URL);
    await second.close();
  });

  it("re-run migration is fast (tables already exist)", async () => {
    const before = Date.now();
    const third = await PostgresStore.connect(TEST_URL);
    const elapsed = Date.now() - before;
    await third.close();
    expect(elapsed).toBeLessThan(1000);
  });
});

describe("PostgresStore: SQL uniqueness constraint", () => {
  it("unique partial index rejects two active rows for same handler + key", async () => {
    await store.createJob(makeJob({ key: "uniq:1" }));
    await expect(
      store.createJob(makeJob({ key: "uniq:1" })),
    ).rejects.toThrow();
  });

  it("allows same key with different handlers", async () => {
    await store.createJob(makeJob({ key: "cross:1", handler: "handler-a" }));
    const second = await store.createJob(makeJob({ key: "cross:1", handler: "handler-b" }));
    expect(second.key).toBe("cross:1");
    expect(second.handler).toBe("handler-b");
  });

  it("uniqueness allows same key after terminal state", async () => {
    const job = await store.createJob(makeJob({ key: "uniq:2" }));
    await store.markRunning(job.id, job.version);
    await store.markCompleted(job.id, job.version);

    // Terminal row doesn't block insert
    const second = await store.createJob(makeJob({ key: "uniq:2" }));
    expect(second.id).not.toBe(job.id);
  });

  it("concurrent inserts: one succeeds, other gets constraint error", async () => {
    // Simulate by inserting rapidly — the unique constraint catches duplicates
    const key = "uniq:concurrent";
    const results = await Promise.allSettled([
      store.createJob(makeJob({ key })),
      store.createJob(makeJob({ key })),
    ]);

    const fulfilled = results.filter(r => r.status === "fulfilled");
    const rejected = results.filter(r => r.status === "rejected");

    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);
  });
});

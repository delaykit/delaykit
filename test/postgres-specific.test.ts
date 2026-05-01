/**
 * Postgres-specific tests beyond the shared store contract.
 *
 * Covers: migration idempotency, SQL uniqueness constraint enforcement.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { PostgresStore, runPostgresMigrations } from "../src/stores/postgres.js";
import { LATEST_POSTGRES_MIGRATION_VERSION } from "../src/stores/postgres-migrations.js";
import { makeJob } from "./helpers/job-factory.js";
import {
  TEST_URL,
  dropPostgresSchema,
  truncatePostgresJobs,
} from "./helpers/postgres-fixture.js";

let store: PostgresStore;

beforeAll(async () => {
  store = await PostgresStore.connect(TEST_URL);
});

afterAll(async () => {
  await store.close();
});

beforeEach(async () => {
  await truncatePostgresJobs(store);
});

async function dropSchema() {
  await dropPostgresSchema(store);
}

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

describe("PostgresStore: runPostgresMigrations + build-time migration pattern", () => {
  it("runPostgresMigrations(url) applies the schema without keeping the store open", async () => {
    // Fresh database scope — drop everything to prove a cold start.
    await dropSchema();

    await runPostgresMigrations(TEST_URL);

    // After runPostgresMigrations returns, a connect() with runMigrations: false
    // should find the schema caught up.
    const verified = await PostgresStore.connect(TEST_URL, { runMigrations: false });
    await verified.close();

    // Recreate schema for subsequent tests in this file.
    await dropSchema();
    await runPostgresMigrations(TEST_URL);
  });

  it("runPostgresMigrations(sql) uses an existing client and leaves it open", async () => {
    // Fresh database scope — drop everything to prove the caller-owned client survives.
    await dropSchema();

    await runPostgresMigrations((store as any).sql);

    // The caller's client is still usable.
    const row = await (store as any).sql`SELECT COUNT(*) as n FROM delaykit.jobs`;
    expect(Number(row[0].n)).toBe(0);
  });

  it("connect({ runMigrations: false }) throws when schema is behind the library", async () => {
    await dropSchema();

    await expect(
      PostgresStore.connect(TEST_URL, { runMigrations: false }),
    ).rejects.toThrow(new RegExp(`version 0 but this release requires ${LATEST_POSTGRES_MIGRATION_VERSION}`));

    // Recreate for subsequent tests.
    await runPostgresMigrations(TEST_URL);
  });

  it("connect({ runMigrations: false }) succeeds when schema is caught up", async () => {
    const s = await PostgresStore.connect(TEST_URL, { runMigrations: false });
    await s.close();
  });

  it("does not leak the created client when connect() throws on stale schema", async () => {
    await dropSchema();

    // Hammer connect() with a bad-schema setup. Without the fix, each
    // failed call leaks a postgres.js pool, eventually exhausting the
    // DB's max_connections. The assertion is just that the DB keeps
    // accepting new connections — if pools were leaking, this would
    // start failing around connection N.
    for (let i = 0; i < 20; i++) {
      await expect(
        PostgresStore.connect(TEST_URL, { runMigrations: false }),
      ).rejects.toThrow(/version 0/);
    }

    // A fresh connect should still succeed post-recovery.
    await runPostgresMigrations(TEST_URL);
    const s = await PostgresStore.connect(TEST_URL, { runMigrations: false });
    await s.close();
  });

  it("does not close the caller's client when assertMigrationsApplied throws", async () => {
    await dropSchema();

    // The shared store.sql is caller-owned. If connect() rejected on
    // stale schema and we'd accidentally closed it, subsequent queries
    // against it would error.
    await expect(
      PostgresStore.connect((store as any).sql, { runMigrations: false }),
    ).rejects.toThrow(/version 0/);

    // Prove the caller's client is still alive.
    const row = await (store as any).sql`SELECT 1 as n`;
    expect(row[0].n).toBe(1);

    await runPostgresMigrations(TEST_URL);
  });
});

describe("PostgresStore: concurrent migrations serialize via advisory lock", () => {
  it("two parallel connects don't dogpile on the migration", async () => {
    // Tear down so migrations actually run on both connects.
    await dropSchema();

    // Kick off two connects in parallel — the advisory lock serializes
    // them. Second one acquires after the first releases and sees the
    // migrations already applied.
    const [a, b] = await Promise.all([
      PostgresStore.connect(TEST_URL),
      PostgresStore.connect(TEST_URL),
    ]);

    // Both connects succeed; schema is caught up.
    const rows = await (store as any).sql`SELECT MAX(version) as v FROM delaykit.migrations`;
    expect(rows[0].v).toBe(LATEST_POSTGRES_MIGRATION_VERSION);

    await a.close();
    await b.close();
  });
});

describe("PostgresStore: completed_at partial index", () => {
  it("migration 5 creates idx_jobs_completed_at with the expected predicate", async () => {
    const rows = await (store as any).sql`
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = 'delaykit'
        AND indexname = 'idx_jobs_completed_at'
    `;
    expect(rows.length).toBe(1);
    const def = rows[0].indexdef as string;
    expect(def).toContain("completed_at");
    expect(def).toContain("'completed'");
    expect(def).toContain("'failed'");
    expect(def).toContain("'cancelled'");
    expect(def).toContain("completed_at IS NOT NULL");
  });
});

describe("PostgresStore: listFailed cursor preserves microsecond precision", () => {
  // TIMESTAMPTZ has microsecond precision. JS Date roundtrip would truncate
  // to milliseconds, skipping rows that share the boundary millisecond. The
  // cursor must round-trip the full-precision timestamp text.
  it("does not skip rows that share the boundary millisecond", async () => {
    const sql = (store as any).sql;
    const ids = [
      "11111111-1111-1111-1111-111111111111",
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333",
    ];

    for (const id of ids) {
      await store.createJob(makeJob({ id, key: `cur:${id}` }));
      await store.markRunning(id, 1);
    }
    // Three rows, same millisecond, increasing microseconds.
    await sql`UPDATE delaykit.jobs SET status = 'failed', completed_at = '2026-05-01 12:00:00.123100+00'::timestamptz WHERE id = ${ids[0]}`;
    await sql`UPDATE delaykit.jobs SET status = 'failed', completed_at = '2026-05-01 12:00:00.123500+00'::timestamptz WHERE id = ${ids[1]}`;
    await sql`UPDATE delaykit.jobs SET status = 'failed', completed_at = '2026-05-01 12:00:00.123900+00'::timestamptz WHERE id = ${ids[2]}`;

    const first = await store.listFailed({ limit: 1 });
    expect(first.jobs).toHaveLength(1);
    expect(first.jobs[0].id).toBe(ids[2]); // newest (largest µs) first

    const second = await store.listFailed({ limit: 1, cursor: first.cursor! });
    expect(second.jobs).toHaveLength(1);
    expect(second.jobs[0].id).toBe(ids[1]);

    const third = await store.listFailed({ limit: 1, cursor: second.cursor! });
    expect(third.jobs).toHaveLength(1);
    expect(third.jobs[0].id).toBe(ids[0]);
    expect(third.cursor).toBeNull();
  });
});

/**
 * SQLiteStore under write load.
 *
 * Scenarios that stress the WAL + busy_timeout + BEGIN IMMEDIATE story:
 *  1. Bursty `dk.schedule()` calls racing the poll loop (single connection).
 *  2. A second app-side writer on the same DB file writing in parallel
 *     with the scheduler's claim transaction (two connections).
 *  3. Two `SQLiteStore.connect()` calls on the same file racing the
 *     schema migration — analogous to Postgres's advisory-lock serialization.
 */

import { describe, it, expect } from "vitest";
import { rmSync } from "node:fs";
import { DelayKit } from "../src/delaykit.js";
import { SQLiteStore } from "../src/stores/sqlite.js";
import { openSQLiteDatabase } from "../src/stores/sqlite-driver.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { tmpDbPath } from "./helpers/sqlite-fixture.js";

describe("SQLite concurrency", () => {
  it("delivers every job under a burst of concurrent schedules", async () => {
    const store = await SQLiteStore.connect(":memory:");
    const dk = new DelayKit({
      store,
      scheduler: new PollingScheduler({ interval: 10 }),
    });

    const N = 200;
    const fired: string[] = [];
    dk.handle("stress", async ({ key }) => {
      fired.push(key);
    });

    await dk.start();
    try {
      await Promise.all(
        Array.from({ length: N }, (_, i) =>
          dk.schedule("stress", { key: `job_${i}`, delay: "0s" }),
        ),
      );

      const deadline = Date.now() + 30_000;
      while (fired.length < N && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 25));
      }

      expect(fired.length).toBe(N);
      expect(new Set(fired).size).toBe(N);

      const stats = await store.stats();
      expect(stats.pending).toBe(0);
      expect(stats.running).toBe(0);
    } finally {
      await dk.stop({ drainMs: 2_000 });
      await store.close();
    }
  });

  it("concurrent SQLiteStore.connect() on the same file serializes migrations", async () => {
    const { dbPath, dir } = tmpDbPath();
    try {
      const [a, b] = await Promise.all([
        SQLiteStore.connect(dbPath),
        SQLiteStore.connect(dbPath),
      ]);

      // Both connections should be usable; exactly one set of migrations
      // applied; no constraint violations from duplicate inserts.
      expect(await a.getJob("nope")).toBeNull();
      expect(await b.getJob("nope")).toBeNull();

      const versions = (
        a as unknown as {
          db: { prepare(sql: string): { all(): Array<{ version: number }> } };
        }
      ).db
        .prepare("SELECT version FROM delaykit_migrations ORDER BY version ASC")
        .all();
      expect(versions.length).toBeGreaterThan(0);
      expect(new Set(versions.map((v) => v.version)).size).toBe(versions.length);

      await a.close();
      await b.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("tolerates an app-side writer contending for the write lock", async () => {
    const { dbPath, dir } = tmpDbPath();

    try {
      const store = await SQLiteStore.connect(dbPath);
      const appDb = await openSQLiteDatabase(dbPath);
      appDb.exec("PRAGMA journal_mode = WAL");
      appDb.exec("PRAGMA busy_timeout = 5000");
      appDb.exec(`CREATE TABLE IF NOT EXISTS app_data (id INTEGER PRIMARY KEY, data TEXT)`);
      const appInsert = appDb.prepare(`INSERT INTO app_data (data) VALUES (?)`);

      const dk = new DelayKit({
        store,
        scheduler: new PollingScheduler({ interval: 10 }),
      });
      const fired: string[] = [];
      dk.handle("stress", async ({ key }) => {
        fired.push(key);
      });
      await dk.start();

      const N = 100;
      try {
        const work: Promise<unknown>[] = [];
        for (let i = 0; i < N; i++) {
          work.push(dk.schedule("stress", { key: `job_${i}`, delay: "0s" }));
          // Synchronous app-side write inside the same event-loop tick
          // as the schedule — exercises two-connection contention.
          appInsert.run(`row_${i}`);
        }
        await Promise.all(work);

        const deadline = Date.now() + 30_000;
        while (fired.length < N && Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 25));
        }

        expect(fired.length).toBe(N);
        const appCount = (
          appDb.prepare(`SELECT COUNT(*) AS c FROM app_data`).get() as { c: number }
        ).c;
        expect(appCount).toBe(N);
      } finally {
        await dk.stop({ drainMs: 2_000 });
        appDb.close();
        await store.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

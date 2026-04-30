import { describe, it, expect } from "vitest";
import { rmSync } from "node:fs";
import { DelayKit } from "../src/delaykit.js";
import { SQLiteStore } from "../src/stores/sqlite.js";
import { openSQLiteDatabase } from "../src/stores/sqlite-driver.js";
import { PollingScheduler } from "../src/schedulers/polling.js";
import { tmpDbPath } from "./helpers/sqlite-fixture.js";

describe("SQLiteStore + PollingScheduler end-to-end", () => {
  it("schedules, polls, runs a handler, and records completion", async () => {
    const store = await SQLiteStore.connect(":memory:");
    const scheduler = new PollingScheduler({ interval: 25 });
    const dk = new DelayKit({ store, scheduler });

    const seen: string[] = [];
    dk.handle("e2e", async ({ key }) => {
      seen.push(key);
    });

    await dk.start();
    try {
      await dk.schedule("e2e", { key: "job-a", delay: "10ms" });
      await dk.schedule("e2e", { key: "job-b", delay: "10ms" });

      const deadline = Date.now() + 2_000;
      while (seen.length < 2 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 20));
      }
      expect(seen.sort()).toEqual(["job-a", "job-b"]);

      const stats = await store.stats();
      expect(stats.running).toBe(0);
      expect(stats.pending).toBe(0);
    } finally {
      await dk.stop({ drainMs: 1_000 });
      await store.close();
    }
  });

  it("rejects an unset path instead of silently falling back to :memory:", async () => {
    const saved = process.env.DELAYKIT_SQLITE_PATH;
    delete process.env.DELAYKIT_SQLITE_PATH;
    try {
      await expect(SQLiteStore.connect()).rejects.toThrow(/file path/i);
    } finally {
      if (saved !== undefined) process.env.DELAYKIT_SQLITE_PATH = saved;
    }
  });

  it("accepts an explicit :memory: path", async () => {
    const store = await SQLiteStore.connect(":memory:");
    await store.close();
  });

  it("surfaces the driver's open error instead of a driver-missing message", async () => {
    const badPath = "/nonexistent-delaykit-dir-xyz/delaykit.db";
    // bun:sqlite says "unable to open"; better-sqlite3 says "Cannot open database".
    await expect(SQLiteStore.connect(badPath)).rejects.toThrow(/(unable to open|cannot open)/i);
    await expect(SQLiteStore.connect(badPath)).rejects.not.toThrow(
      /install better-sqlite3/i,
    );
  });

  it("accepts a caller-owned database and leaves it open after store.close()", async () => {
    const db = await openSQLiteDatabase(":memory:");
    const store = await SQLiteStore.connect(db);

    // delaykit's tables exist on the caller's connection
    const beforeClose = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='delaykit_jobs'",
      )
      .get();
    expect(beforeClose).toBeTruthy();

    await store.close();

    // The caller's connection is still usable for its own tables
    db.exec("CREATE TABLE app_table (id TEXT PRIMARY KEY)");
    db.prepare("INSERT INTO app_table (id) VALUES (?)").run("x");
    const row = db.prepare("SELECT id FROM app_table").get();
    expect(row).toEqual({ id: "x" });
    db.close();
  });

  it("runs an end-to-end handler that reads from a caller-owned app table", async () => {
    const db = await openSQLiteDatabase(":memory:");
    db.exec(`
      CREATE TABLE reminders (
        id TEXT PRIMARY KEY,
        message TEXT NOT NULL
      )
    `);
    db.prepare("INSERT INTO reminders (id, message) VALUES (?, ?)").run(
      "r1",
      "hello",
    );

    const store = await SQLiteStore.connect(db);
    const dk = new DelayKit({
      store,
      scheduler: new PollingScheduler({ interval: 25 }),
    });

    const seen: string[] = [];
    dk.handle("co-tenant", async ({ key }) => {
      // Same connection used by delaykit reads the app's domain row
      const row = db
        .prepare("SELECT message FROM reminders WHERE id = ?")
        .get(key) as { message: string } | undefined;
      if (row) seen.push(row.message);
    });

    await dk.start();
    try {
      await dk.schedule("co-tenant", { key: "r1", delay: "10ms" });
      const deadline = Date.now() + 2_000;
      while (seen.length === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 20));
      }
      expect(seen).toEqual(["hello"]);
    } finally {
      await dk.stop({ drainMs: 500 });
      await store.close();
      // Caller's db remains usable after delaykit shuts down
      const count = db
        .prepare("SELECT COUNT(*) AS c FROM reminders")
        .get() as { c: number };
      expect(count.c).toBe(1);
      db.close();
    }
  });

  it("persists state across store reopen on a real file", async () => {
    const { dbPath, dir } = tmpDbPath();

    try {
      const first = await SQLiteStore.connect(dbPath);
      const dk1 = new DelayKit({ store: first, scheduler: new PollingScheduler() });
      dk1.handle("persist", async () => {});
      await dk1.schedule("persist", { key: "survives", delay: "1h" });
      await first.close();

      const second = await SQLiteStore.connect(dbPath);
      const active = await second.getActiveJobByKey("persist", "survives");
      expect(active).not.toBeNull();
      expect(active!.key).toBe("survives");
      await second.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

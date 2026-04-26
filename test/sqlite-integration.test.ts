import { describe, it, expect } from "vitest";
import { rmSync } from "node:fs";
import { DelayKit } from "../src/delaykit.js";
import { SQLiteStore } from "../src/stores/sqlite.js";
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
    await expect(SQLiteStore.connect(badPath)).rejects.toThrow(
      /(unable to open|no such file|ENOENT|cannot open)/i,
    );
    // Counter-check: the driver-missing copy must not appear.
    await expect(SQLiteStore.connect(badPath)).rejects.not.toThrow(
      /install better-sqlite3/i,
    );
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

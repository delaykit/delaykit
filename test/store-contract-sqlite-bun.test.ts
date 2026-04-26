/**
 * Runs the shared store contract against `SQLiteStore` backed by the
 * native `bun:sqlite` driver.
 *
 * This file is intended to run under `bun test` — vitest (whether
 * invoked via node or `bun run`) executes tests in a Node-compat VM
 * that neither exposes the `Bun` global nor resolves `bun:sqlite`,
 * so the whole suite skips there.
 */

import { describe, it, expect } from "vitest";
import { SQLiteStore } from "../src/stores/sqlite.js";
import type { SQLiteLike } from "../src/stores/sqlite-driver.js";
import { storeContractSuite } from "./store-contract.js";
import { truncateSqliteJobs } from "./helpers/sqlite-fixture.js";

const isBun = typeof (globalThis as { Bun?: unknown }).Bun !== "undefined";

describe.skipIf(!isBun)("SQLiteStore with bun:sqlite", () => {
  it("auto-selects bun:sqlite when no driver is passed", async () => {
    const mod = (await import("bun:sqlite" as string)) as {
      Database: new (p: string) => SQLiteLike;
    };
    const store = await SQLiteStore.connect(":memory:");
    const db = (store as unknown as { db: unknown }).db;
    expect(db).toBeInstanceOf(mod.Database);
    await store.close();
  });

  storeContractSuite(
    "bun:sqlite (explicit driver)",
    async () => {
      const mod = (await import("bun:sqlite" as string)) as {
        Database: new (p: string) => SQLiteLike;
      };
      const db = new mod.Database(":memory:");
      return SQLiteStore.connect(db);
    },
    async (store) => truncateSqliteJobs(store),
  );
});

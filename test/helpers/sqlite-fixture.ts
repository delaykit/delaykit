import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Store } from "../../src/types.js";
import type { SQLiteLike } from "../../src/stores/sqlite-driver.js";

export function tmpDbPath(): { dbPath: string; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), "delaykit-sqlite-"));
  return { dbPath: join(dir, "delaykit.db"), dir };
}

export function truncateSqliteJobs(store: Store): void {
  (store as unknown as { db: SQLiteLike }).db.exec("DELETE FROM delaykit_jobs");
}

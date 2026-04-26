/**
 * Minimal structural interface satisfied by both `better-sqlite3` and
 * `bun:sqlite`. `SQLiteStore` depends only on this shape, so callers
 * can pass either driver (or runtime-select via `openSQLiteDatabase`).
 *
 * Pragma access is intentionally absent — `better-sqlite3` has
 * `db.pragma()` but `bun:sqlite` does not. The store uses
 * `db.exec("PRAGMA ...")` to stay portable.
 */

export interface SQLiteLikeStatement {
  run(...params: unknown[]): { changes: number };
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

type TxFn<T> = () => T;

export interface SQLiteLikeTransactionFn<T> {
  (): T;
  default: TxFn<T>;
  deferred: TxFn<T>;
  immediate: TxFn<T>;
  exclusive: TxFn<T>;
}

export interface SQLiteLike {
  prepare(sql: string): SQLiteLikeStatement;
  exec(sql: string): void;
  transaction<T>(fn: TxFn<T>): SQLiteLikeTransactionFn<T>;
  close(): void;
}

/**
 * Dynamic import via a variable specifier so TypeScript doesn't try
 * to statically resolve `bun:sqlite` under Node (where the module
 * and its types are absent). Runtime-guarded by the caller.
 *
 * Uses a plain `import()` rather than `new Function` so test runners
 * that sandbox the module graph (e.g. Vitest's VM pool) can still
 * resolve the specifier through their own import handler.
 */
async function dynamicImport(id: string): Promise<Record<string, unknown>> {
  return (await import(id)) as Record<string, unknown>;
}

/**
 * Open a SQLite database using the best driver for the current
 * runtime. Bun: `bun:sqlite` (built-in). Node: `better-sqlite3`
 * (optional peer dependency).
 *
 * Users who want to override the default — e.g. force
 * `better-sqlite3` under Bun — can construct a `Database` themselves
 * and pass it to `SQLiteStore.connect`.
 */
export async function openSQLiteDatabase(path: string): Promise<SQLiteLike> {
  // Only the module-load step is allowed to trigger a fallback to
  // the other driver. Constructor failures (missing parent directory,
  // permission denied, corrupt file) propagate unchanged so the real
  // error isn't masked as "install a driver".
  const g = globalThis as { Bun?: unknown };
  if (typeof g.Bun !== "undefined") {
    let mod: Record<string, unknown> | null = null;
    try {
      mod = await dynamicImport("bun:sqlite");
    } catch {
      // bun:sqlite should always be available under Bun, but if the
      // user stripped it or is on Bun < 1, fall through to
      // better-sqlite3 if they've installed it.
    }
    if (mod) {
      const Ctor = (mod.Database ?? mod.default) as new (p: string) => SQLiteLike;
      return new Ctor(path);
    }
  }

  let mod: Record<string, unknown>;
  try {
    mod = await dynamicImport("better-sqlite3");
  } catch (err) {
    throw new Error(
      "SQLiteStore requires a SQLite driver. Under Bun, bun:sqlite is built in; under Node, install better-sqlite3 (npm install better-sqlite3).",
      { cause: err as Error },
    );
  }
  const Ctor = (mod.default ?? mod) as new (p: string) => SQLiteLike;
  return new Ctor(path);
}

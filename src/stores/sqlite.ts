import { randomUUID } from "node:crypto";
import { openSQLiteDatabase } from "./sqlite-driver.js";
import type { SQLiteLike, SQLiteLikeStatement } from "./sqlite-driver.js";
import type {
  ClaimBatch,
  DelayKitStats,
  FailureReason,
  Job,
  JobStatus,
  ListFailedOptions,
  ListFailedPage,
  SchedulerRetryConfig,
  Store,
} from "../types.js";
import {
  ConcurrentInsertError,
  DEFAULT_TIMEOUT_MS,
  MAX_LIST_FAILED_LIMIT,
  STALLED_GRACE_MS,
  assertCappedLimit,
  assertPositiveLimit,
  decodeListFailedCursor,
  encodeListFailedCursor,
  parseRetryConfig,
  serializeRetryConfig,
  truncateLastError,
} from "../types.js";
import { LATEST_SQLITE_MIGRATION_VERSION, SQLITE_MIGRATIONS } from "./sqlite-migrations.js";

export { LATEST_SQLITE_MIGRATION_VERSION };

const SQLITE_CONSTRAINT_UNIQUE = "SQLITE_CONSTRAINT_UNIQUE";
const SQLITE_CONSTRAINT_PRIMARYKEY = "SQLITE_CONSTRAINT_PRIMARYKEY";

/**
 * Shared `scheduled_for` recompute used by `rescheduleDueAt`,
 * `requeueForNextWindow`, `reclaimStalled`, and `reclaimStalledJobs`.
 * Throttle: anchor to firstAt; debounce: lastAt + waitMs, capped at
 * firstAt + maxWaitMs.
 */
const NEXT_WINDOW_SQL = `CASE
  WHEN kind = 'throttle' THEN first_at + wait_ms
  ELSE MIN(
    last_at + wait_ms,
    CASE WHEN max_wait_ms IS NOT NULL
      THEN first_at + max_wait_ms
      ELSE last_at + wait_ms
    END
  )
END`;

export interface SQLiteStoreOptions {
  runMigrations?: boolean;
}

type Row = Record<string, unknown>;

export class SQLiteStore implements Store {
  private db: SQLiteLike;
  /**
   * Caches `db.prepare(sql)` results so repeat callers reuse compiled
   * statements. `bun:sqlite` doesn't cache internally, so without this
   * every poll tick re-parses every query.
   */
  private stmtCache = new Map<string, SQLiteLikeStatement>();
  private closed = false;
  /** True when `connect()` opened the database; gates `close()`. */
  private readonly ownsDatabase: boolean;

  private constructor(db: SQLiteLike, ownsDatabase: boolean) {
    this.db = db;
    this.ownsDatabase = ownsDatabase;
  }

  private stmt(sql: string): SQLiteLikeStatement {
    let s = this.stmtCache.get(sql);
    if (!s) {
      s = this.db.prepare(sql);
      this.stmtCache.set(sql, s);
    }
    return s;
  }

  /**
   * Open a `SQLiteStore`.
   *
   * Two modes:
   *
   * - **Path mode** (`connect("/path/to.db")` or `connect(":memory:")`):
   *   delaykit opens the database, sets PRAGMAs, owns the lifecycle.
   *   `store.close()` closes the underlying database.
   *
   * - **Caller-owned mode** (`connect(db)` with an existing
   *   `bun:sqlite` / `better-sqlite3` instance): delaykit reuses the
   *   passed-in connection so apps can host their own domain tables on
   *   the same file or in-memory database. `store.close()` leaves the
   *   caller's database open — it's theirs to close.
   *
   * In both modes delaykit applies its standard PRAGMAs on the
   * connection: `journal_mode = WAL` (skipped silently for `:memory:`
   * and platforms that reject it), `busy_timeout = 5000`,
   * `foreign_keys = ON`. Set these *after* `connect()` if you want
   * different values; delaykit doesn't read them back.
   *
   * delaykit's tables are prefixed (`delaykit_jobs`,
   * `delaykit_migrations`), so they don't collide with app tables on
   * the same connection.
   */
  static async connect(
    pathOrDb?: string | SQLiteLike,
    options?: SQLiteStoreOptions,
  ): Promise<SQLiteStore> {
    let db: SQLiteLike;
    let ownsDatabase = false;
    if (typeof pathOrDb === "string" || pathOrDb == null) {
      const resolved = pathOrDb ?? process.env.DELAYKIT_SQLITE_PATH;
      if (!resolved) {
        throw new Error(
          'SQLiteStore requires a file path. Pass it as the first argument or set DELAYKIT_SQLITE_PATH. For an in-memory store, pass ":memory:" explicitly.',
        );
      }
      db = await openSQLiteDatabase(resolved);
      ownsDatabase = true;
    } else {
      db = pathOrDb;
    }
    try {
      db.exec("PRAGMA journal_mode = WAL");
    } catch {
      // :memory: and some platforms reject WAL — fall back silently.
    }
    db.exec("PRAGMA busy_timeout = 5000");
    db.exec("PRAGMA foreign_keys = ON");

    const store = new SQLiteStore(db, ownsDatabase);

    try {
      if (options?.runMigrations === false) {
        store.assertMigrationsApplied();
      } else {
        store.migrate();
      }
    } catch (err) {
      // Only close databases we opened. Caller-owned handles are
      // theirs to close.
      if (ownsDatabase) db.close();
      throw err;
    }

    return store;
  }

  private migrate(): void {
    const run = this.db.transaction(() => {
      // Bootstrap the migrations table so we can read current version.
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS delaykit_migrations (
          version    INTEGER PRIMARY KEY,
          applied_at INTEGER NOT NULL
        );
      `);
      const currentVersion = this.readCurrentMigrationVersion();
      for (const migration of SQLITE_MIGRATIONS) {
        if (migration.version > currentVersion) {
          this.db.exec(migration.sql);
          this.stmt(
              `INSERT OR IGNORE INTO delaykit_migrations (version, applied_at) VALUES (?, ?)`,
            )
            .run(migration.version, Date.now());
        }
      }
    });
    run.immediate();
  }

  private assertMigrationsApplied(): void {
    const current = this.readCurrentMigrationVersion();
    if (current < LATEST_SQLITE_MIGRATION_VERSION) {
      throw new Error(
        `DelayKit schema is at migration version ${current} but this release requires ${LATEST_SQLITE_MIGRATION_VERSION}. Run migrations first (e.g. a postbuild step calling runSQLiteMigrations(path)) or drop 'runMigrations: false'.`,
      );
    }
  }

  private readCurrentMigrationVersion(): number {
    const exists = this.stmt(
        `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'delaykit_migrations'`,
      )
      .get();
    if (!exists) return 0;
    const row = this.stmt(`SELECT COALESCE(MAX(version), 0) AS version FROM delaykit_migrations`)
      .get() as { version: number } | undefined;
    return row?.version ?? 0;
  }

  async createJob(job: Omit<Job, "createdAt">): Promise<Job> {
    const id = job.id || randomUUID();
    const now = Date.now();
    try {
      const row = this.stmt(
          `INSERT INTO delaykit_jobs (
             id, kind, handler, key, version, claimed_version, status,
             scheduled_for, started_at, completed_at,
             attempt, max_attempts, scheduler_ref, last_error, failure_reason, created_at,
             first_at, last_at, wait_ms, max_wait_ms,
             defer_attempts, deferred_since, retry_config
           ) VALUES (
             ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?,
             ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?,
             ?, ?, ?
           )
           RETURNING *`,
        )
        .get(
          id,
          job.kind,
          job.handler,
          job.key,
          job.version,
          job.claimedVersion,
          job.status,
          job.scheduledFor.getTime(),
          dateToMs(job.startedAt),
          dateToMs(job.completedAt),
          job.attempt,
          job.maxAttempts,
          job.schedulerRef,
          truncateLastError(job.lastError),
          job.failureReason,
          now,
          dateToMs(job.firstAt),
          dateToMs(job.lastAt),
          job.waitMs,
          job.maxWaitMs,
          job.deferAttempts,
          dateToMs(job.deferredSince),
          job.retryConfig
            ? JSON.stringify(serializeRetryConfig(job.retryConfig))
            : null,
        );
      return this.rowToJob(row as Row);
    } catch (err) {
      if (isUniqueViolation(err)) {
        throw new ConcurrentInsertError(job.handler, job.key);
      }
      throw err;
    }
  }

  async getJob(id: string): Promise<Job | null> {
    const row = this.stmt(`SELECT * FROM delaykit_jobs WHERE id = ?`)
      .get(id) as Row | undefined;
    return row ? this.rowToJob(row) : null;
  }

  async getActiveJobByKey(handler: string, key: string): Promise<Job | null> {
    const row = this.stmt(
        `SELECT * FROM delaykit_jobs
         WHERE handler = ? AND key = ? AND status IN ('pending', 'running')
         LIMIT 1`,
      )
      .get(handler, key) as Row | undefined;
    return row ? this.rowToJob(row) : null;
  }

  async cancelJob(id: string): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'cancelled', completed_at = ?,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'pending'`,
      )
      .run(Date.now(), id);
    return result.changes > 0;
  }

  async updateScheduledFor(id: string, scheduledFor: Date): Promise<void> {
    this.stmt(`UPDATE delaykit_jobs SET scheduled_for = ? WHERE id = ?`)
      .run(scheduledFor.getTime(), id);
  }

  async deleteJob(id: string): Promise<void> {
    this.stmt(`DELETE FROM delaykit_jobs WHERE id = ?`).run(id);
  }

  async updatePatternEvent(
    key: string,
    handler: string,
    kind: "debounce" | "throttle",
    now: Date,
    waitMs: number,
    maxWaitMs: number | null,
  ): Promise<Job | null> {
    const nowMs = now.getTime();
    // `IS` handles NULL-safe equality so (max_wait_ms IS NULL) matches when
    // the caller passes null, and (max_wait_ms IS 5000) works for numbers.
    const row = this.stmt(
        `UPDATE delaykit_jobs
         SET version = version + 1,
             last_at = ?,
             first_at = CASE
               WHEN status = 'running' AND (first_at IS NULL OR first_at <= started_at)
               THEN ?
               ELSE first_at
             END
         WHERE key = ?
           AND status IN ('pending', 'running')
           AND kind = ?
           AND handler = ?
           AND wait_ms = ?
           AND max_wait_ms IS ?
         RETURNING *`,
      )
      .get(nowMs, nowMs, key, kind, handler, waitMs, maxWaitMs) as Row | undefined;
    if (!row) {
      const existing = await this.getActiveJobByKey(handler, key);
      if (existing) {
        if (existing.kind !== kind) {
          throw new Error(
            `Cannot use ${kind} for key "${key}": an active ${existing.kind} job exists for this key.`,
          );
        }
        if (existing.handler !== handler) {
          throw new Error(
            `Config mismatch for key "${key}": active job uses handler "${existing.handler}" but "${handler}" was requested.`,
          );
        }
        if (existing.waitMs !== waitMs || existing.maxWaitMs !== maxWaitMs) {
          throw new Error(
            `Config mismatch for key "${key}": wait/maxWait does not match active window.`,
          );
        }
      }
      return null;
    }
    return this.rowToJob(row);
  }

  async markRunning(id: string, version: number): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'running', started_at = ?, claimed_version = ?,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'pending' AND version = ?`,
      )
      .run(Date.now(), version, id, version);
    return result.changes > 0;
  }

  async markCompleted(id: string, version: number): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'completed', completed_at = ?,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'running' AND version = ?`,
      )
      .run(Date.now(), id, version);
    return result.changes > 0;
  }

  async markFailed(id: string, version: number, error: Error, reason: FailureReason): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'failed', last_error = ?, failure_reason = ?, completed_at = ?,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'running' AND version = ?`,
      )
      .run(truncateLastError(error.message), reason, Date.now(), id, version);
    return result.changes > 0;
  }

  async retryJob(
    id: string,
    version: number,
    nextAttempt: number,
    scheduledFor: Date,
    lastError: string,
  ): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending', attempt = ?, scheduled_for = ?,
             started_at = NULL, completed_at = NULL, claimed_version = NULL,
             last_error = ?,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'running' AND version = ?`,
      )
      .run(
        nextAttempt,
        scheduledFor.getTime(),
        truncateLastError(lastError),
        id,
        version,
      );
    return result.changes > 0;
  }

  async rescheduleDueAt(id: string, version: number): Promise<Job | null> {
    const row = this.stmt(
        `UPDATE delaykit_jobs
         SET version = version + 1,
             scheduled_for = ${NEXT_WINDOW_SQL}
         WHERE id = ? AND status = 'pending' AND version = ?
         RETURNING *`,
      )
      .get(id, version) as Row | undefined;
    return row ? this.rowToJob(row) : null;
  }

  async requeueForNextWindow(id: string): Promise<Job | null> {
    const row = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending',
             version = version + 1,
             started_at = NULL,
             completed_at = NULL,
             claimed_version = NULL,
             attempt = 0,
             defer_attempts = 0,
             deferred_since = NULL,
             scheduled_for = ${NEXT_WINDOW_SQL}
         WHERE id = ? AND status = 'running'
         RETURNING *`,
      )
      .get(id) as Row | undefined;
    return row ? this.rowToJob(row) : null;
  }

  async replaceJob(
    id: string,
    scheduledFor: Date,
    maxAttempts: number,
  ): Promise<Job | null> {
    const row = this.stmt(
        `UPDATE delaykit_jobs
         SET version = version + 1, scheduled_for = ?, status = 'pending',
             attempt = 0, max_attempts = ?, scheduler_ref = NULL,
             last_error = NULL, failure_reason = NULL,
             started_at = NULL, completed_at = NULL,
             claimed_version = NULL,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'pending'
         RETURNING *`,
      )
      .get(scheduledFor.getTime(), maxAttempts, id) as Row | undefined;
    return row ? this.rowToJob(row) : null;
  }

  async deferJob(
    id: string,
    version: number,
    scheduledFor: Date,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null> {
    const now = Date.now();
    const tx = this.db.transaction(() => {
      // Read current row to compute horizon_exceeded before writing.
      const current = this.stmt(
          `SELECT deferred_since FROM delaykit_jobs
           WHERE id = ? AND status = 'pending' AND version = ?`,
        )
        .get(id, version) as { deferred_since: number | null } | undefined;
      if (!current) return null;

      const deferredSince = current.deferred_since ?? now;
      const horizonExceeded = deferredSince + horizonMs <= now;

      if (horizonExceeded) {
        const row = this.stmt(
            `UPDATE delaykit_jobs
             SET version = version + 1,
                 defer_attempts = defer_attempts + 1,
                 deferred_since = COALESCE(deferred_since, ?),
                 status = 'failed',
                 completed_at = ?,
                 last_error = ?,
                 failure_reason = 'defer_horizon'
             WHERE id = ? AND version = ?
             RETURNING *`,
          )
          .get(now, now, truncateLastError(terminalError), id, version) as Row | undefined;
        return row ?? null;
      }

      const row = this.stmt(
          `UPDATE delaykit_jobs
           SET version = version + 1,
               defer_attempts = defer_attempts + 1,
               deferred_since = COALESCE(deferred_since, ?),
               scheduled_for = ?,
               last_error = ?
           WHERE id = ? AND version = ?
           RETURNING *`,
        )
        .get(
          now,
          scheduledFor.getTime(),
          truncateLastError(deferredError),
          id,
          version,
        ) as Row | undefined;
      return row ?? null;
    });
    const row = tx.immediate();
    return row ? this.rowToJob(row as Row) : null;
  }

  async noteMissingHandler(
    id: string,
    version: number,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null> {
    const now = Date.now();
    const tx = this.db.transaction(() => {
      const current = this.stmt(
          `SELECT deferred_since FROM delaykit_jobs
           WHERE id = ? AND status = 'pending' AND version = ?`,
        )
        .get(id, version) as { deferred_since: number | null } | undefined;
      if (!current) return null;

      const deferredSince = current.deferred_since ?? now;
      const horizonExceeded = deferredSince + horizonMs <= now;

      if (horizonExceeded) {
        const row = this.stmt(
            `UPDATE delaykit_jobs
             SET version = version + 1,
                 defer_attempts = defer_attempts + 1,
                 deferred_since = COALESCE(deferred_since, ?),
                 status = 'failed',
                 completed_at = ?,
                 last_error = ?,
                 failure_reason = 'defer_horizon'
             WHERE id = ? AND version = ?
             RETURNING *`,
          )
          .get(now, now, truncateLastError(terminalError), id, version) as Row | undefined;
        return row ?? null;
      }

      // scheduled_for intentionally unchanged — capable replicas must
      // still see this row as due on their next claim cycle.
      const row = this.stmt(
          `UPDATE delaykit_jobs
           SET version = version + 1,
               defer_attempts = defer_attempts + 1,
               deferred_since = COALESCE(deferred_since, ?),
               last_error = ?
           WHERE id = ? AND version = ?
           RETURNING *`,
        )
        .get(now, truncateLastError(deferredError), id, version) as Row | undefined;
      return row ?? null;
    });
    const row = tx.immediate();
    return row ? this.rowToJob(row as Row) : null;
  }

  async resetJob(id: string): Promise<Job | null> {
    try {
      const row = this.stmt(
          `UPDATE delaykit_jobs
           SET status = 'pending',
               attempt = 0,
               version = version + 1,
               scheduled_for = ?,
               started_at = NULL,
               completed_at = NULL,
               claimed_version = NULL,
               last_error = NULL,
               failure_reason = NULL,
               defer_attempts = 0,
               deferred_since = NULL,
               scheduler_ref = NULL
           WHERE id = ? AND status = 'failed'
           RETURNING *`,
        )
        .get(Date.now(), id) as Row | undefined;
      return row ? this.rowToJob(row) : null;
    } catch (err) {
      if (isUniqueViolation(err)) return null;
      throw err;
    }
  }

  async resetJobAt(id: string, version: number, scheduledFor: Date): Promise<Job | null> {
    try {
      const row = this.stmt(
          `UPDATE delaykit_jobs
           SET status = 'pending',
               attempt = 0,
               version = version + 1,
               scheduled_for = ?,
               started_at = NULL,
               completed_at = NULL,
               claimed_version = NULL,
               last_error = NULL,
               failure_reason = NULL,
               defer_attempts = 0,
               deferred_since = NULL,
               scheduler_ref = NULL
           WHERE id = ? AND status = 'failed' AND version = ?
           RETURNING *`,
        )
        .get(scheduledFor.getTime(), id, version) as Row | undefined;
      return row ? this.rowToJob(row) : null;
    } catch (err) {
      if (isUniqueViolation(err)) return null;
      throw err;
    }
  }

  async listFailed(opts: ListFailedOptions): Promise<ListFailedPage> {
    assertCappedLimit(opts.limit, MAX_LIST_FAILED_LIMIT);
    const cursor = opts.cursor ? decodeListFailedCursor(opts.cursor) : null;

    const where: string[] = [`status = 'failed'`, `completed_at IS NOT NULL`];
    const params: unknown[] = [];
    if (opts.handler != null) { where.push(`handler = ?`); params.push(opts.handler); }
    if (opts.reason != null) { where.push(`failure_reason = ?`); params.push(opts.reason); }
    if (opts.since != null) { where.push(`completed_at >= ?`); params.push(opts.since.getTime()); }
    if (opts.until != null) { where.push(`completed_at <= ?`); params.push(opts.until.getTime()); }
    if (cursor != null) {
      where.push(`(completed_at < ? OR (completed_at = ? AND id < ?))`);
      params.push(cursor.completedAtMs, cursor.completedAtMs, cursor.id);
    }
    params.push(opts.limit + 1);

    const rows = this.stmt(
        `SELECT * FROM delaykit_jobs
         WHERE ${where.join(" AND ")}
         ORDER BY completed_at DESC, id DESC
         LIMIT ?`,
      )
      .all(...(params as never[])) as Row[];

    const more = rows.length > opts.limit;
    const page = (more ? rows.slice(0, opts.limit) : rows).map((r) => this.rowToJob(r));
    const last = page[page.length - 1];
    return {
      jobs: page,
      cursor: more && last ? encodeListFailedCursor(last.completedAt!, last.id) : null,
    };
  }

  async updateSchedulerRef(id: string, version: number, ref: string): Promise<boolean> {
    const result = this.stmt(
        `UPDATE delaykit_jobs SET scheduler_ref = ? WHERE id = ? AND version = ?`,
      )
      .run(ref, id, version);
    return result.changes > 0;
  }

  async unknownDueHandlers(knownHandlers: string[]): Promise<string[]> {
    const now = Date.now();
    if (knownHandlers.length === 0) {
      const rows = this.stmt(
          `SELECT DISTINCT handler FROM delaykit_jobs
           WHERE status = 'pending' AND scheduled_for <= ?`,
        )
        .all(now) as Array<{ handler: string }>;
      return rows.map((r) => r.handler);
    }
    const placeholders = knownHandlers.map(() => "?").join(",");
    const rows = this.stmt(
        `SELECT DISTINCT handler FROM delaykit_jobs
         WHERE status = 'pending'
           AND scheduled_for <= ?
           AND handler NOT IN (${placeholders})`,
      )
      .all(now, ...knownHandlers) as Array<{ handler: string }>;
    return rows.map((r) => r.handler);
  }

  async unknownDueJobs(knownHandlers: string[], limit: number): Promise<Job[]> {
    const now = Date.now();
    // The kind/last_at/wait_ms predicate mirrors claimDueJobs's
    // settlement arm — un-settled debounce rows aren't actually
    // deliverable yet, so they shouldn't start the missing-handler
    // horizon clock.
    //
    // `deferred_since NULLS FIRST` (SQLite default for ASC) prioritizes
    // rows that have not had their horizon clock started yet, so a
    // misconfiguration with more orphan rows than `limit` doesn't
    // strand back-page rows for full horizon cycles before they're
    // noted. Once every orphan has a clock, ordering falls through to
    // `deferred_since ASC` so the rows closest to horizon flip first.
    const settledPredicate = `(
      kind != 'debounce'
      OR (last_at IS NOT NULL AND (? - last_at) >= wait_ms)
      OR (max_wait_ms IS NOT NULL AND first_at IS NOT NULL
          AND (? - first_at) >= max_wait_ms)
    )`;
    if (knownHandlers.length === 0) {
      const rows = this.stmt(
          `SELECT * FROM delaykit_jobs
           WHERE status = 'pending'
             AND scheduled_for <= ?
             AND ${settledPredicate}
           ORDER BY deferred_since ASC, scheduled_for ASC, id ASC
           LIMIT ?`,
        )
        .all(now, now, now, limit) as Row[];
      return rows.map((row) => this.rowToJob(row));
    }
    const placeholders = knownHandlers.map(() => "?").join(",");
    const rows = this.stmt(
        `SELECT * FROM delaykit_jobs
         WHERE status = 'pending'
           AND scheduled_for <= ?
           AND handler NOT IN (${placeholders})
           AND ${settledPredicate}
         ORDER BY deferred_since ASC, scheduled_for ASC, id ASC
         LIMIT ?`,
      )
      .all(now, ...knownHandlers, now, now, limit) as Row[];
    return rows.map((row) => this.rowToJob(row));
  }

  async claimDueJobs(budget: number, handlerNames: string[]): Promise<ClaimBatch> {
    if (handlerNames.length === 0) return { toRun: [], rescheduled: [] };

    const now = Date.now();
    const placeholders = handlerNames.map(() => "?").join(",");

    const tx = this.db.transaction((): { toRun: Row[]; rescheduled: Row[] } => {
      const candidates = this.stmt(
          `SELECT id, version, kind, first_at, last_at, wait_ms, max_wait_ms
           FROM delaykit_jobs
           WHERE status = 'pending'
             AND scheduled_for <= ?
             AND handler IN (${placeholders})
           ORDER BY scheduled_for ASC, id ASC
           LIMIT ?`,
        )
        .all(now, ...handlerNames, budget) as Array<{
        id: string;
        version: number;
        kind: string;
        first_at: number | null;
        last_at: number | null;
        wait_ms: number | null;
        max_wait_ms: number | null;
      }>;

      const toRun: Row[] = [];
      const rescheduled: Row[] = [];

      const claimStmt = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'running',
             started_at = ?,
             claimed_version = version,
             defer_attempts = 0,
             deferred_since = NULL
         WHERE id = ? AND version = ? AND status = 'pending'
         RETURNING *`,
      );
      const advanceStmt = this.stmt(
        `UPDATE delaykit_jobs
         SET version = version + 1,
             scheduled_for = ?
         WHERE id = ? AND version = ? AND status = 'pending'
         RETURNING *`,
      );

      for (const c of candidates) {
        const waitMs = c.wait_ms ?? 0;
        const isSettled =
          c.kind !== "debounce" ||
          (c.last_at != null && now - c.last_at >= waitMs) ||
          (c.max_wait_ms != null &&
            c.first_at != null &&
            now - c.first_at >= c.max_wait_ms);

        if (isSettled) {
          const row = claimStmt.get(now, c.id, c.version) as Row | undefined;
          if (row) toRun.push(row);
        } else {
          const byWait = (c.last_at ?? 0) + waitMs;
          const byMaxWait =
            c.max_wait_ms != null && c.first_at != null
              ? c.first_at + c.max_wait_ms
              : byWait;
          const nextAt = Math.min(byWait, byMaxWait);
          const row = advanceStmt.get(nextAt, c.id, c.version) as Row | undefined;
          if (row) rescheduled.push(row);
        }
      }
      return { toRun, rescheduled };
    });

    const { toRun: toRunRows, rescheduled: rescheduledRows } = tx.immediate();

    const toRun = toRunRows.map((r) => this.rowToJob(r));
    const rescheduled = rescheduledRows.map((r) => this.rowToJob(r));
    const byDueThenId = (a: Job, b: Job) => {
      const d = a.scheduledFor.getTime() - b.scheduledFor.getTime();
      return d !== 0 ? d : a.id.localeCompare(b.id);
    };
    toRun.sort(byDueThenId);
    rescheduled.sort(byDueThenId);
    return { toRun, rescheduled };
  }

  async reclaimStalled(id: string, leaseMs: number): Promise<Job | null> {
    const now = Date.now();
    // Pattern with version advance: requeue fresh window.
    const requeued = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending',
             version = version + 1,
             attempt = 0,
             started_at = NULL, completed_at = NULL, claimed_version = NULL,
             defer_attempts = 0, deferred_since = NULL,
             scheduled_for = ${NEXT_WINDOW_SQL}
         WHERE id = ? AND status = 'running'
           AND started_at IS NOT NULL
           AND started_at + ? < ?
           AND kind != 'once'
           AND claimed_version IS NOT NULL
           AND version > claimed_version
         RETURNING *`,
      )
      .get(id, leaseMs, now) as Row | undefined;
    if (requeued) return this.rowToJob(requeued);

    const reclaimed = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending', attempt = attempt + 1,
             started_at = NULL, claimed_version = NULL,
             defer_attempts = 0, deferred_since = NULL
         WHERE id = ? AND status = 'running'
           AND started_at IS NOT NULL
           AND started_at + ? < ?
         RETURNING *`,
      )
      .get(id, leaseMs, now) as Row | undefined;
    return reclaimed ? this.rowToJob(reclaimed) : null;
  }

  async reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]> {
    const cutoffMs =
      Math.max(DEFAULT_TIMEOUT_MS, ...handlerTimeouts.values()) + STALLED_GRACE_MS;
    const now = Date.now();

    const requeued = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending',
             version = version + 1,
             started_at = NULL, completed_at = NULL, claimed_version = NULL,
             attempt = 0,
             defer_attempts = 0, deferred_since = NULL,
             scheduled_for = ${NEXT_WINDOW_SQL}
         WHERE status = 'running'
           AND started_at IS NOT NULL
           AND started_at + ? < ?
           AND kind != 'once'
           AND claimed_version IS NOT NULL
           AND version > claimed_version
         RETURNING *`,
      )
      .all(cutoffMs, now) as Row[];

    const reclaimed = this.stmt(
        `UPDATE delaykit_jobs
         SET status = 'pending', attempt = attempt + 1,
             started_at = NULL, claimed_version = NULL,
             defer_attempts = 0, deferred_since = NULL
         WHERE status = 'running'
           AND started_at IS NOT NULL
           AND started_at + ? < ?
         RETURNING *`,
      )
      .all(cutoffMs, now) as Row[];

    return [
      ...requeued.map((r) => this.rowToJob(r)),
      ...reclaimed.map((r) => this.rowToJob(r)),
    ];
  }

  async pruneTerminal(olderThan: Date, limit?: number): Promise<number> {
    assertPositiveLimit(limit);
    const cutoff = olderThan.getTime();
    if (limit === undefined) {
      const result = this.stmt(
          `DELETE FROM delaykit_jobs
           WHERE status IN ('completed', 'failed', 'cancelled')
             AND completed_at IS NOT NULL
             AND completed_at < ?`,
        )
        .run(cutoff);
      return result.changes;
    }
    const result = this.stmt(
        `DELETE FROM delaykit_jobs
         WHERE id IN (
           SELECT id FROM delaykit_jobs
           WHERE status IN ('completed', 'failed', 'cancelled')
             AND completed_at IS NOT NULL
             AND completed_at < ?
           ORDER BY completed_at ASC, id ASC
           LIMIT ?
         )`,
      )
      .run(cutoff, limit);
    return result.changes;
  }

  async stats(): Promise<DelayKitStats> {
    const now = Date.now();
    const dayAgo = now - 24 * 60 * 60 * 1000;

    // Debounce-settlement predicate used in both `duePending` and
    // `oldestDuePending`. Un-settled debounce rows are pending-but-not-due
    // — the wake window hasn't elapsed yet.
    const settledPredicate = `(
      kind != 'debounce'
      OR (last_at IS NOT NULL AND (? - last_at) >= wait_ms)
      OR (max_wait_ms IS NOT NULL AND first_at IS NOT NULL
          AND (? - first_at) >= max_wait_ms)
    )`;

    const byHandler = this.stmt(
        `SELECT handler,
           SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
           SUM(CASE WHEN status = 'pending' AND scheduled_for <= ?
                         AND ${settledPredicate}
                    THEN 1 ELSE 0 END) AS duePending,
           SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
           SUM(CASE WHEN status = 'pending' AND deferred_since IS NOT NULL
                    THEN 1 ELSE 0 END) AS deferred,
           SUM(CASE WHEN status = 'failed' AND completed_at >= ?
                    THEN 1 ELSE 0 END) AS failed24h
         FROM delaykit_jobs
         GROUP BY handler
         HAVING pending > 0 OR running > 0 OR failed24h > 0
         ORDER BY handler ASC`,
      )
      .all(now, now, now, dayAgo) as Array<{
      handler: string;
      pending: number;
      duePending: number;
      running: number;
      deferred: number;
      failed24h: number;
    }>;

    const totals = byHandler.reduce(
      (acc, h) => {
        acc.pending += h.pending;
        acc.duePending += h.duePending;
        acc.running += h.running;
        acc.deferred += h.deferred;
        acc.failed24h += h.failed24h;
        return acc;
      },
      { pending: 0, duePending: 0, running: 0, deferred: 0, failed24h: 0 },
    );

    const oldestDue = this.stmt(
        `SELECT id, handler, scheduled_for
         FROM delaykit_jobs
         WHERE status = 'pending' AND scheduled_for <= ?
           AND ${settledPredicate}
         ORDER BY scheduled_for ASC, id ASC
         LIMIT 1`,
      )
      .get(now, now, now) as
      | { id: string; handler: string; scheduled_for: number }
      | undefined;

    const oldestRun = this.stmt(
        `SELECT id, handler, started_at FROM delaykit_jobs
         WHERE status = 'running'
         ORDER BY started_at ASC, id ASC
         LIMIT 1`,
      )
      .get() as { id: string; handler: string; started_at: number } | undefined;

    return {
      pending: totals.pending,
      duePending: totals.duePending,
      running: totals.running,
      deferred: totals.deferred,
      failed24h: totals.failed24h,
      oldestDuePending: oldestDue
        ? {
            id: oldestDue.id,
            handler: oldestDue.handler,
            scheduledFor: new Date(oldestDue.scheduled_for),
          }
        : null,
      oldestRunning: oldestRun
        ? {
            id: oldestRun.id,
            handler: oldestRun.handler,
            startedAt: new Date(oldestRun.started_at),
          }
        : null,
      byHandler,
    };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.stmtCache.clear();
    if (this.ownsDatabase) this.db.close();
  }

  private rowToJob(row: Row): Job {
    return {
      id: row.id as string,
      kind: row.kind as Job["kind"],
      handler: row.handler as string,
      key: row.key as string,
      version: row.version as number,
      claimedVersion: (row.claimed_version as number | null) ?? null,
      status: row.status as JobStatus,
      scheduledFor: new Date(row.scheduled_for as number),
      startedAt: row.started_at != null ? new Date(row.started_at as number) : null,
      completedAt:
        row.completed_at != null ? new Date(row.completed_at as number) : null,
      attempt: row.attempt as number,
      maxAttempts: row.max_attempts as number,
      schedulerRef: (row.scheduler_ref as string | null) ?? null,
      lastError: (row.last_error as string | null) ?? null,
      failureReason: (row.failure_reason as FailureReason | null) ?? null,
      createdAt: new Date(row.created_at as number),
      firstAt: row.first_at != null ? new Date(row.first_at as number) : null,
      lastAt: row.last_at != null ? new Date(row.last_at as number) : null,
      waitMs: (row.wait_ms as number | null) ?? null,
      maxWaitMs: (row.max_wait_ms as number | null) ?? null,
      deferAttempts: (row.defer_attempts as number) ?? 0,
      deferredSince:
        row.deferred_since != null ? new Date(row.deferred_since as number) : null,
      retryConfig: parseRetryFromText(row.retry_config as string | null),
    };
  }
}

function dateToMs(d: Date | null | undefined): number | null {
  return d != null ? d.getTime() : null;
}

function parseRetryFromText(raw: string | null): SchedulerRetryConfig | null {
  if (!raw) return null;
  try {
    return parseRetryConfig(JSON.parse(raw));
  } catch {
    return null;
  }
}

function isUniqueViolation(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: string }).code;
  if (code === SQLITE_CONSTRAINT_UNIQUE || code === SQLITE_CONSTRAINT_PRIMARYKEY) {
    return true;
  }
  // Fallback for drivers whose `err.code` isn't a stable public API
  // (notably bun:sqlite). Matches the cross-driver SQLite wording.
  const message = (err as { message?: string }).message;
  return typeof message === "string" && /UNIQUE constraint failed/i.test(message);
}

/**
 * Apply pending DelayKit SQLite migrations. Intended for deploy-time
 * use or a one-shot CLI. Strings get a short-lived DB handle that's
 * closed after; caller-owned handles stay open.
 */
export async function runSQLiteMigrations(
  pathOrDb: string | SQLiteLike,
): Promise<void> {
  const store = await SQLiteStore.connect(pathOrDb, { runMigrations: true });
  await store.close();
}

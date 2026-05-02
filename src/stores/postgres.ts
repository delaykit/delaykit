import type postgres from "postgres";
import { randomUUID } from "node:crypto";
import type { ClaimBatch, DelayKitStats, FailureReason, Job, JobStatus, ListFailedOptions, ListFailedPage, Store } from "../types.js";
import {
  ConcurrentInsertError,
  DEFAULT_TIMEOUT_MS,
  MAX_LIST_FAILED_LIMIT,
  STALLED_GRACE_MS,
  assertCappedLimit,
  assertPositiveLimit,
  parseRetryConfig,
  serializeRetryConfig,
  truncateLastError,
} from "../types.js";
import { LATEST_POSTGRES_MIGRATION_VERSION, POSTGRES_MIGRATIONS, SCHEMA } from "./postgres-migrations.js";
export { LATEST_POSTGRES_MIGRATION_VERSION };

async function loadPostgres(): Promise<typeof postgres> {
  try {
    const mod = await import("postgres");
    return mod.default;
  } catch {
    throw new Error(
      "PostgresStore requires the 'postgres' package. Install it with: npm install postgres",
    );
  }
}

// https://www.postgresql.org/docs/current/errcodes-appendix.html
const PG_UNIQUE_VIOLATION = "23505";
const PG_INVALID_TEXT_REPRESENTATION = "22P02";

// ASCII "dela"/"migr" — stable identifiers for pg_advisory_lock across
// versions. The namespace+key pair is DelayKit-specific; anyone else
// hashing to the same 64-bit pair would be an astronomical coincidence.
const ADVISORY_LOCK_NS = 0x64656c61;
const ADVISORY_LOCK_KEY = 0x6d696772;

export interface PostgresStoreOptions {
  runMigrations?: boolean;
}

export class PostgresStore implements Store {
  private sql: postgres.Sql;
  /** True when `connect()` opened the client; gates `close()`. */
  private readonly ownsClient: boolean;

  private constructor(sql: postgres.Sql, ownsClient: boolean) {
    this.sql = sql;
    this.ownsClient = ownsClient;
  }

  static async connect(
    connectionStringOrClient?: string | postgres.Sql,
    options?: PostgresStoreOptions,
  ): Promise<PostgresStore> {
    let sql: postgres.Sql;
    let ownsClient = false;
    if (typeof connectionStringOrClient === "string" || connectionStringOrClient == null) {
      const resolved = connectionStringOrClient ?? process.env.DELAYKIT_DATABASE_URL;
      if (!resolved) {
        throw new Error(
          "Database connection string is required. Pass it as the first argument or set the DELAYKIT_DATABASE_URL environment variable.",
        );
      }
      const postgres = await loadPostgres();
      sql = postgres(resolved);
      ownsClient = true;
    } else {
      sql = connectionStringOrClient;
    }
    const store = new PostgresStore(sql, ownsClient);

    try {
      if (options?.runMigrations === false) {
        await store.assertMigrationsApplied();
      } else {
        await store.migrate();
      }
    } catch (err) {
      // Only close clients we opened. Caller-owned clients are theirs
      // to close.
      if (ownsClient) await sql.end().catch(() => {});
      throw err;
    }

    return store;
  }

  async migrate(): Promise<void> {
    // Serialize concurrent migrators so a post-deploy traffic spike
    // doesn't produce dozens of cold starts racing on the same DDL.
    // Reserve a single pooled connection so the advisory lock's
    // acquire and release land on the same session.
    const reserved = await this.sql.reserve();
    try {
      await reserved`SELECT pg_advisory_lock(${ADVISORY_LOCK_NS}::int, ${ADVISORY_LOCK_KEY}::int)`;
      try {
        const currentVersion = await readCurrentMigrationVersion(reserved);
        for (const migration of POSTGRES_MIGRATIONS) {
          if (migration.version > currentVersion) {
            await reserved.unsafe(migration.sql);
          }
        }
      } finally {
        await reserved`SELECT pg_advisory_unlock(${ADVISORY_LOCK_NS}::int, ${ADVISORY_LOCK_KEY}::int)`;
      }
    } finally {
      reserved.release();
    }
  }

  private async assertMigrationsApplied(): Promise<void> {
    const current = await readCurrentMigrationVersion(this.sql);
    if (current < LATEST_POSTGRES_MIGRATION_VERSION) {
      throw new Error(
        `DelayKit schema is at migration version ${current} but this release requires ${LATEST_POSTGRES_MIGRATION_VERSION}. Run migrations first (e.g. a postbuild step calling runPostgresMigrations(DATABASE_URL)) or drop 'runMigrations: false'.`,
      );
    }
  }

  /**
   * Shared `scheduled_for` recompute used by `rescheduleDueAt`,
   * `requeueForNextWindow`, `reclaimStalled`, and `reclaimStalledJobs`.
   * Throttle: anchor to firstAt; debounce: lastAt + waitMs, capped at
   * firstAt + maxWaitMs. Returned as a pending fragment so it composes
   * with the surrounding tagged template.
   */
  private nextWindowSql() {
    return this.sql`CASE
      WHEN kind = 'throttle' THEN first_at + (wait_ms * INTERVAL '1 millisecond')
      ELSE LEAST(
        last_at + (wait_ms * INTERVAL '1 millisecond'),
        CASE WHEN max_wait_ms IS NOT NULL
          THEN first_at + (max_wait_ms * INTERVAL '1 millisecond')
          ELSE last_at + (wait_ms * INTERVAL '1 millisecond')
        END
      )
    END`;
  }

  async createJob(job: Omit<Job, "createdAt">): Promise<Job> {
    const id = job.id || randomUUID();
    try {
      const rows = await this.sql`
        INSERT INTO delaykit.jobs (
          id, kind, handler, key, version, claimed_version, status,
          scheduled_for, started_at, completed_at,
          attempt, max_attempts, scheduler_ref, last_error, failure_reason,
          first_at, last_at, wait_ms, max_wait_ms,
          defer_attempts, deferred_since, retry_config
        ) VALUES (
          ${id}, ${job.kind}, ${job.handler}, ${job.key},
          ${job.version}, ${job.claimedVersion}, ${job.status},
          ${job.scheduledFor}, ${job.startedAt}, ${job.completedAt},
          ${job.attempt}, ${job.maxAttempts}, ${job.schedulerRef}, ${truncateLastError(job.lastError)}, ${job.failureReason},
          ${job.firstAt}, ${job.lastAt}, ${job.waitMs}, ${job.maxWaitMs},
          ${job.deferAttempts}, ${job.deferredSince},
          ${job.retryConfig ? this.sql.json(serializeRetryConfig(job.retryConfig)) : null}
        )
        RETURNING *
      `;
      return this.rowToJob(rows[0]);
    } catch (err: any) {
      if (err.code === PG_UNIQUE_VIOLATION) {
        throw new ConcurrentInsertError(job.handler, job.key);
      }
      throw err;
    }
  }

  async getJob(id: string): Promise<Job | null> {
    try {
      const rows = await this.sql`
        SELECT * FROM delaykit.jobs WHERE id = ${id}
      `;
      return rows.length > 0 ? this.rowToJob(rows[0]) : null;
    } catch (err: any) {
      // Invalid UUID format → treat as not found
      if (err.code === PG_INVALID_TEXT_REPRESENTATION) return null;
      throw err;
    }
  }

  async getActiveJobByKey(handler: string, key: string): Promise<Job | null> {
    const rows = await this.sql`
      SELECT * FROM delaykit.jobs
      WHERE handler = ${handler} AND key = ${key} AND status IN ('pending', 'running')
      LIMIT 1
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async cancelJob(id: string): Promise<boolean> {
    const result = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'cancelled', completed_at = NOW(),
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'pending'
    `;
    return result.count > 0;
  }

  async updateScheduledFor(id: string, scheduledFor: Date): Promise<void> {
    await this.sql`
      UPDATE delaykit.jobs
      SET scheduled_for = ${scheduledFor}
      WHERE id = ${id}
    `;
  }

  async deleteJob(id: string): Promise<void> {
    await this.sql`DELETE FROM delaykit.jobs WHERE id = ${id}`;
  }

  async updatePatternEvent(
    key: string,
    handler: string,
    kind: "debounce" | "throttle",
    now: Date,
    waitMs: number,
    maxWaitMs: number | null,
  ): Promise<Job | null> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET version = version + 1,
          last_at = ${now},
          first_at = CASE
            WHEN status = 'running' AND (first_at IS NULL OR first_at <= started_at)
            THEN ${now}
            ELSE first_at
          END
      WHERE key = ${key}
        AND status IN ('pending', 'running')
        AND kind = ${kind}
        AND handler = ${handler}
        AND wait_ms = ${waitMs}
        AND (max_wait_ms IS NOT DISTINCT FROM ${maxWaitMs})
      RETURNING *
    `;
    if (rows.length === 0) {
      // Check if mismatch vs missing
      const existing = await this.getActiveJobByKey(handler, key);
      if (existing) {
        if (existing.kind !== kind) {
          throw new Error(`Cannot use ${kind} for key "${key}": an active ${existing.kind} job exists for this key.`);
        }
        if (existing.handler !== handler) {
          throw new Error(`Config mismatch for key "${key}": active job uses handler "${existing.handler}" but "${handler}" was requested.`);
        }
        if (existing.waitMs !== waitMs || existing.maxWaitMs !== maxWaitMs) {
          throw new Error(`Config mismatch for key "${key}": wait/maxWait does not match active window.`);
        }
      }
      return null;
    }
    return this.rowToJob(rows[0]);
  }

  async markRunning(id: string, version: number): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'running', started_at = now(), claimed_version = ${version},
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'pending' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async markCompleted(id: string, version: number): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'completed', completed_at = now(),
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async markFailed(id: string, version: number, error: Error, reason: FailureReason): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'failed', last_error = ${truncateLastError(error.message)},
          failure_reason = ${reason}, completed_at = now(),
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async retryJob(id: string, version: number, nextAttempt: number, scheduledFor: Date, lastError: string): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending', attempt = ${nextAttempt}, scheduled_for = ${scheduledFor},
          started_at = NULL, completed_at = NULL, claimed_version = NULL,
          last_error = ${truncateLastError(lastError)},
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async rescheduleJob(id: string, version: number, scheduledFor: Date): Promise<Job | null> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending',
          version = version + 1,
          attempt = 0,
          scheduled_for = ${scheduledFor},
          started_at = NULL,
          completed_at = NULL,
          claimed_version = NULL,
          last_error = NULL,
          failure_reason = NULL,
          defer_attempts = 0,
          deferred_since = NULL,
          scheduler_ref = NULL
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async rescheduleDueAt(id: string, version: number): Promise<Job | null> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET version = version + 1,
          scheduled_for = ${this.nextWindowSql()}
      WHERE id = ${id} AND status = 'pending' AND version = ${version}
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async requeueForNextWindow(id: string): Promise<Job | null> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending',
          version = version + 1,
          started_at = NULL,
          completed_at = NULL,
          claimed_version = NULL,
          attempt = 0,
          defer_attempts = 0,
          deferred_since = NULL,
          scheduled_for = ${this.nextWindowSql()}
      WHERE id = ${id} AND status = 'running'
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async replaceJob(id: string, scheduledFor: Date, maxAttempts: number): Promise<Job | null> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET version = version + 1, scheduled_for = ${scheduledFor}, status = 'pending',
          attempt = 0, max_attempts = ${maxAttempts}, scheduler_ref = NULL,
          last_error = NULL, failure_reason = NULL,
          started_at = NULL, completed_at = NULL, claimed_version = NULL,
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'pending'
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async deferJob(
    id: string,
    version: number,
    scheduledFor: Date,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null> {
    // The status/version predicates appear on both the CTE and the
    // UPDATE so the CAS holds across READ COMMITTED races. Without the
    // duplicate on the UPDATE, EvalPlanQual would re-evaluate only
    // `j.id = t.id` after another transaction's commit and clobber a
    // row that's now `running` / `completed` / `cancelled`.
    const rows = await this.sql`
      WITH target AS (
        SELECT id,
               COALESCE(deferred_since, now()) + (${horizonMs} * INTERVAL '1 millisecond') <= now()
                 AS horizon_exceeded
        FROM delaykit.jobs
        WHERE id = ${id} AND status = 'pending' AND version = ${version}
      )
      UPDATE delaykit.jobs j
      SET version = version + 1,
          defer_attempts = defer_attempts + 1,
          deferred_since = COALESCE(deferred_since, now()),
          status         = CASE WHEN t.horizon_exceeded THEN 'failed' ELSE 'pending' END,
          completed_at   = CASE WHEN t.horizon_exceeded THEN now()    ELSE completed_at END,
          scheduled_for  = CASE WHEN t.horizon_exceeded THEN scheduled_for ELSE ${scheduledFor} END,
          last_error     = CASE WHEN t.horizon_exceeded THEN ${truncateLastError(terminalError)} ELSE ${truncateLastError(deferredError)} END,
          failure_reason = CASE WHEN t.horizon_exceeded THEN 'defer_horizon' ELSE failure_reason END
      FROM target t
      WHERE j.id = t.id
        AND j.status = 'pending'
        AND j.version = ${version}
      RETURNING j.*
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async noteMissingHandler(
    id: string,
    version: number,
    deferredError: string,
    terminalError: string,
    horizonMs: number,
  ): Promise<Job | null> {
    // See `deferJob` for why the CAS predicates are duplicated on the
    // UPDATE clause.
    const rows = await this.sql`
      WITH target AS (
        SELECT id,
               COALESCE(deferred_since, now()) + (${horizonMs} * INTERVAL '1 millisecond') <= now()
                 AS horizon_exceeded
        FROM delaykit.jobs
        WHERE id = ${id} AND status = 'pending' AND version = ${version}
      )
      UPDATE delaykit.jobs j
      SET version = version + 1,
          defer_attempts = defer_attempts + 1,
          deferred_since = COALESCE(deferred_since, now()),
          status         = CASE WHEN t.horizon_exceeded THEN 'failed' ELSE 'pending' END,
          completed_at   = CASE WHEN t.horizon_exceeded THEN now()    ELSE completed_at END,
          last_error     = CASE WHEN t.horizon_exceeded THEN ${truncateLastError(terminalError)} ELSE ${truncateLastError(deferredError)} END,
          failure_reason = CASE WHEN t.horizon_exceeded THEN 'defer_horizon' ELSE failure_reason END
      FROM target t
      WHERE j.id = t.id
        AND j.status = 'pending'
        AND j.version = ${version}
      RETURNING j.*
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async resetJob(id: string): Promise<Job | null> {
    try {
      const rows = await this.sql`
        UPDATE delaykit.jobs
        SET status = 'pending',
            attempt = 0,
            version = version + 1,
            scheduled_for = now(),
            started_at = NULL,
            completed_at = NULL,
            claimed_version = NULL,
            last_error = NULL,
            failure_reason = NULL,
            defer_attempts = 0,
            deferred_since = NULL,
            scheduler_ref = NULL
        WHERE id = ${id} AND status = 'failed'
        RETURNING *
      `;
      return rows.length > 0 ? this.rowToJob(rows[0]) : null;
    } catch (err: any) {
      if (err.code === PG_INVALID_TEXT_REPRESENTATION) return null;
      if (err.code === PG_UNIQUE_VIOLATION) return null; // key slot already occupied
      throw err;
    }
  }

  async resetJobAt(id: string, version: number, scheduledFor: Date): Promise<Job | null> {
    try {
      const rows = await this.sql`
        UPDATE delaykit.jobs
        SET status = 'pending',
            attempt = 0,
            version = version + 1,
            scheduled_for = ${scheduledFor},
            started_at = NULL,
            completed_at = NULL,
            claimed_version = NULL,
            last_error = NULL,
            failure_reason = NULL,
            defer_attempts = 0,
            deferred_since = NULL,
            scheduler_ref = NULL
        WHERE id = ${id} AND status = 'failed' AND version = ${version}
        RETURNING *
      `;
      return rows.length > 0 ? this.rowToJob(rows[0]) : null;
    } catch (err: any) {
      if (err.code === PG_INVALID_TEXT_REPRESENTATION) return null;
      if (err.code === PG_UNIQUE_VIOLATION) return null;
      throw err;
    }
  }

  async listFailed(opts: ListFailedOptions): Promise<ListFailedPage> {
    assertCappedLimit(opts.limit, MAX_LIST_FAILED_LIMIT);
    // Cursor carries the timestamptz text (microsecond-precise) instead of
    // a JS Date roundtrip, which would truncate to milliseconds and skip
    // rows that share the boundary millisecond at the page edge.
    const cursor = opts.cursor ? decodePostgresCursor(opts.cursor) : null;

    const rows = await this.sql`
      SELECT *, completed_at::text AS _cursor_at FROM delaykit.jobs
      WHERE status = 'failed'
        AND completed_at IS NOT NULL
        AND ${opts.handler == null ? this.sql`TRUE` : this.sql`handler = ${opts.handler}`}
        AND ${opts.reason == null ? this.sql`TRUE` : this.sql`failure_reason = ${opts.reason}`}
        AND ${opts.since == null ? this.sql`TRUE` : this.sql`completed_at >= ${opts.since}`}
        AND ${opts.until == null ? this.sql`TRUE` : this.sql`completed_at <= ${opts.until}`}
        AND ${cursor == null
          ? this.sql`TRUE`
          // Double-cast (text→timestamptz) so postgres.js binds as TEXT and
          // Postgres parses server-side with microsecond precision. Direct
          // `${param}::timestamptz` round-trips through postgres.js's
          // ms-precision Date parser and truncates microseconds.
          : this.sql`(completed_at, id) < ((${cursor!.completedAtText})::text::timestamptz, ${cursor!.id})`}
      ORDER BY completed_at DESC, id DESC
      LIMIT ${opts.limit + 1}
    `;

    const more = rows.length > opts.limit;
    const sliced = more ? rows.slice(0, opts.limit) : rows;
    const page = sliced.map((r) => this.rowToJob(r));
    const last = sliced[sliced.length - 1];
    return {
      jobs: page,
      cursor: more && last
        ? encodePostgresCursor(last._cursor_at as string, last.id as string)
        : null,
    };
  }

  async updateSchedulerRef(id: string, version: number, ref: string): Promise<boolean> {
    const result = await this.sql`
      UPDATE delaykit.jobs
      SET scheduler_ref = ${ref}
      WHERE id = ${id} AND version = ${version}
    `;
    return result.count > 0;
  }

  async reclaimStalled(id: string, leaseMs: number): Promise<Job | null> {
    // Pattern with version advance: requeue fresh window
    const requeued = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending',
          version = version + 1,
          attempt = 0,
          started_at = NULL, completed_at = NULL, claimed_version = NULL,
          defer_attempts = 0, deferred_since = NULL,
          scheduled_for = ${this.nextWindowSql()}
      WHERE id = ${id} AND status = 'running'
        AND started_at IS NOT NULL
        AND started_at + (${leaseMs} * INTERVAL '1 millisecond') < now()
        AND kind != 'once'
        AND claimed_version IS NOT NULL
        AND version > claimed_version
      RETURNING *
    `;
    if (requeued.length > 0) return this.rowToJob(requeued[0]);

    // Normal reclaim: increment attempt, caller handles exhaustion + onFailure
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending', attempt = attempt + 1,
          started_at = NULL, claimed_version = NULL,
          defer_attempts = 0, deferred_since = NULL
      WHERE id = ${id} AND status = 'running'
        AND started_at IS NOT NULL
        AND started_at + (${leaseMs} * INTERVAL '1 millisecond') < now()
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]> {
    // Reclaim cutoff is `max(DEFAULT_TIMEOUT_MS, ...handlerTimeouts) +
    // STALLED_GRACE_MS`. The DEFAULT_TIMEOUT_MS floor protects rows
    // whose handler isn't in the current registration map (rolling
    // deploy, rename) from being reclaimed earlier than baseline.
    const cutoffMs = Math.max(DEFAULT_TIMEOUT_MS, ...handlerTimeouts.values()) + STALLED_GRACE_MS;

    // Pattern rows whose version advanced mid-execution → requeue a
    // fresh window. Runs first so the second UPDATE doesn't match
    // these rows.
    const requeued = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending',
          version = version + 1,
          started_at = NULL, completed_at = NULL, claimed_version = NULL,
          attempt = 0,
          defer_attempts = 0, deferred_since = NULL,
          scheduled_for = ${this.nextWindowSql()}
      WHERE status = 'running'
        AND started_at IS NOT NULL
        AND started_at + (${cutoffMs} * INTERVAL '1 millisecond') < now()
        AND kind != 'once'
        AND claimed_version IS NOT NULL
        AND version > claimed_version
      RETURNING *
    `;

    // Remaining expired rows → bump attempt, set pending. Caller
    // handles exhaustion + onFailure.
    const reclaimed = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'pending', attempt = attempt + 1,
          started_at = NULL, claimed_version = NULL,
          defer_attempts = 0, deferred_since = NULL
      WHERE status = 'running'
        AND started_at IS NOT NULL
        AND started_at + (${cutoffMs} * INTERVAL '1 millisecond') < now()
      RETURNING *
    `;

    return [
      ...requeued.map((r) => this.rowToJob(r)),
      ...reclaimed.map((r) => this.rowToJob(r)),
    ];
  }

  async unknownDueHandlers(knownHandlers: string[]): Promise<string[]> {
    const rows = await this.sql`
      SELECT DISTINCT handler
      FROM delaykit.jobs
      WHERE status = 'pending'
        AND scheduled_for <= now()
        AND NOT (handler = ANY(${knownHandlers}::text[]))
    `;
    return rows.map((r) => r.handler as string);
  }

  async unknownDueJobs(knownHandlers: string[], limit: number): Promise<Job[]> {
    // The kind/last_at/wait_ms predicate mirrors claimDueJobs's
    // settlement arm — un-settled debounce rows aren't actually
    // deliverable yet, so they shouldn't start the missing-handler
    // horizon clock.
    //
    // `deferred_since NULLS FIRST` prioritizes rows that have not had
    // their horizon clock started yet, so a misconfiguration with more
    // orphan rows than `limit` doesn't strand back-page rows for full
    // horizon cycles before they're noted. Once every orphan has a
    // clock, ordering falls through to `deferred_since ASC` so the
    // rows closest to horizon flip first.
    const rows = await this.sql`
      SELECT *
      FROM delaykit.jobs
      WHERE status = 'pending'
        AND scheduled_for <= now()
        AND NOT (handler = ANY(${knownHandlers}::text[]))
        AND (
          kind != 'debounce'
          OR (last_at IS NOT NULL AND (now() - last_at) >= (wait_ms * INTERVAL '1 millisecond'))
          OR (max_wait_ms IS NOT NULL AND first_at IS NOT NULL
              AND (now() - first_at) >= (max_wait_ms * INTERVAL '1 millisecond'))
        )
      ORDER BY deferred_since ASC NULLS FIRST, scheduled_for ASC, id ASC
      LIMIT ${limit}
    `;
    return rows.map((row) => this.rowToJob(row));
  }

  async claimDueJobs(budget: number, handlerNames: string[]): Promise<ClaimBatch> {
    // Handler availability is replica-local, so filter candidates at
    // selection time. Rows whose handler isn't registered here stay
    // pending — available for other replicas that can run them.
    //
    // Two-arm CTE: settled rows flip directly to running; un-settled
    // debounce rows have their scheduled_for advanced.
    // FOR UPDATE SKIP LOCKED lets concurrent pollers claim disjoint sets.
    if (handlerNames.length === 0) return { toRun: [], rescheduled: [] };
    const result = await this.sql`
      WITH candidates AS (
        SELECT id, version, kind, first_at, last_at, wait_ms, max_wait_ms
        FROM delaykit.jobs
        WHERE status = 'pending'
          AND scheduled_for <= now()
          AND handler = ANY(${handlerNames}::text[])
        ORDER BY scheduled_for ASC, id ASC
        LIMIT ${budget}
        FOR UPDATE SKIP LOCKED
      ),
      classify AS (
        SELECT id, version, kind,
          CASE
            WHEN kind != 'debounce' THEN TRUE
            WHEN last_at IS NOT NULL AND (now() - last_at) >= (wait_ms * INTERVAL '1 millisecond') THEN TRUE
            WHEN max_wait_ms IS NOT NULL AND first_at IS NOT NULL
              AND (now() - first_at) >= (max_wait_ms * INTERVAL '1 millisecond') THEN TRUE
            ELSE FALSE
          END AS is_settled
        FROM candidates
      ),
      advanced AS (
        UPDATE delaykit.jobs AS j
        SET version = j.version + 1,
            scheduled_for = LEAST(
              j.last_at + (j.wait_ms * INTERVAL '1 millisecond'),
              CASE WHEN j.max_wait_ms IS NOT NULL
                THEN j.first_at + (j.max_wait_ms * INTERVAL '1 millisecond')
                ELSE j.last_at + (j.wait_ms * INTERVAL '1 millisecond')
              END
            )
        FROM classify c
        WHERE j.id = c.id AND j.version = c.version AND NOT c.is_settled
        RETURNING j.*, 'rescheduled'::text AS bucket
      ),
      claimed AS (
        UPDATE delaykit.jobs AS j
        SET status = 'running',
            started_at = now(),
            claimed_version = j.version,
            defer_attempts = 0,
            deferred_since = NULL
        FROM classify c
        WHERE j.id = c.id AND j.version = c.version AND c.is_settled
        RETURNING j.*, 'toRun'::text AS bucket
      )
      SELECT * FROM claimed
      UNION ALL
      SELECT * FROM advanced
    `;

    const toRun: Job[] = [];
    const rescheduled: Job[] = [];
    for (const row of result) {
      const bucket = (row as { bucket: string }).bucket;
      const job = this.rowToJob(row);
      if (bucket === "toRun") toRun.push(job);
      else rescheduled.push(job);
    }
    // UNION ALL does not preserve per-CTE ORDER BY — sort in JS so
    // callers see (scheduled_for, id) ordering within each bucket.
    const byDueThenId = (a: Job, b: Job) => {
      const diff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
      return diff !== 0 ? diff : a.id.localeCompare(b.id);
    };
    toRun.sort(byDueThenId);
    rescheduled.sort(byDueThenId);
    return { toRun, rescheduled };
  }

  async pruneTerminal(olderThan: Date, limit?: number): Promise<number> {
    assertPositiveLimit(limit);
    // Unlimited path avoids the subquery-and-lock pattern — the planner
    // does a single index scan + delete. The limited path exists so
    // scheduled retention jobs can prune in bounded batches.
    const result = limit === undefined
      ? await this.sql`
          DELETE FROM delaykit.jobs
          WHERE status IN ('completed', 'failed', 'cancelled')
            AND completed_at IS NOT NULL
            AND completed_at < ${olderThan}
        `
      : await this.sql`
          DELETE FROM delaykit.jobs
          WHERE id IN (
            SELECT id FROM delaykit.jobs
            WHERE status IN ('completed', 'failed', 'cancelled')
              AND completed_at IS NOT NULL
              AND completed_at < ${olderThan}
            ORDER BY completed_at ASC, id ASC
            LIMIT ${limit}
          )
        `;
    return result.count;
  }

  async stats(): Promise<DelayKitStats> {
    const [row] = await this.sql`
      WITH
      by_handler AS (
        SELECT
          handler,
          COUNT(*) FILTER (WHERE status = 'pending')::int                                                               AS pending,
          COUNT(*) FILTER (WHERE status = 'pending' AND scheduled_for <= now()
            AND (kind != 'debounce'
              OR (last_at IS NOT NULL AND (now() - last_at) >= (wait_ms * INTERVAL '1 millisecond'))
              OR (max_wait_ms IS NOT NULL AND first_at IS NOT NULL
                  AND (now() - first_at) >= (max_wait_ms * INTERVAL '1 millisecond'))))::int    AS due_pending,
          COUNT(*) FILTER (WHERE status = 'running')::int                                                               AS running,
          COUNT(*) FILTER (WHERE status = 'pending' AND deferred_since IS NOT NULL)::int                                AS deferred,
          COUNT(*) FILTER (WHERE status = 'failed' AND completed_at >= now() - INTERVAL '24 hours')::int               AS failed_24h
        FROM delaykit.jobs
        GROUP BY handler
        HAVING
          COUNT(*) FILTER (WHERE status = 'pending') > 0
          OR COUNT(*) FILTER (WHERE status = 'running') > 0
          OR COUNT(*) FILTER (WHERE status = 'failed' AND completed_at >= now() - INTERVAL '24 hours') > 0
      ),
      totals AS (
        SELECT
          COALESCE(SUM(pending),    0)::int AS pending,
          COALESCE(SUM(due_pending),0)::int AS due_pending,
          COALESCE(SUM(running),    0)::int AS running,
          COALESCE(SUM(deferred),   0)::int AS deferred,
          COALESCE(SUM(failed_24h), 0)::int AS failed_24h
        FROM by_handler
      ),
      oldest_due_pending AS (
        SELECT id, handler, scheduled_for
        FROM delaykit.jobs
        WHERE status = 'pending' AND scheduled_for <= now()
          AND (kind != 'debounce'
            OR (last_at IS NOT NULL AND (now() - last_at) >= (wait_ms * INTERVAL '1 millisecond'))
            OR (max_wait_ms IS NOT NULL AND first_at IS NOT NULL
                AND (now() - first_at) >= (max_wait_ms * INTERVAL '1 millisecond')))
        ORDER BY scheduled_for ASC, id ASC
        LIMIT 1
      ),
      oldest_running AS (
        SELECT id, handler, started_at
        FROM delaykit.jobs
        WHERE status = 'running'
        ORDER BY started_at ASC, id ASC
        LIMIT 1
      )
      SELECT
        t.pending, t.due_pending, t.running, t.deferred, t.failed_24h,
        odp.id          AS odp_id,
        odp.handler     AS odp_handler,
        odp.scheduled_for AS odp_scheduled_for,
        orw.id          AS orw_id,
        orw.handler     AS orw_handler,
        orw.started_at  AS orw_started_at,
        (
          SELECT json_agg(
            json_build_object(
              'handler',    bh.handler,
              'pending',    bh.pending,
              'duePending', bh.due_pending,
              'running',    bh.running,
              'deferred',   bh.deferred,
              'failed24h',  bh.failed_24h
            )
            ORDER BY bh.handler ASC
          )
          FROM by_handler bh
        ) AS by_handler
      FROM totals t
      LEFT JOIN oldest_due_pending odp ON true
      LEFT JOIN oldest_running orw ON true
    `;

    return {
      pending:    row.pending,
      duePending: row.due_pending,
      running:    row.running,
      deferred:   row.deferred,
      failed24h:  row.failed_24h,
      oldestDuePending: row.odp_id
        ? { id: row.odp_id, handler: row.odp_handler, scheduledFor: new Date(row.odp_scheduled_for) }
        : null,
      oldestRunning: row.orw_id
        ? { id: row.orw_id, handler: row.orw_handler, startedAt: new Date(row.orw_started_at) }
        : null,
      byHandler: row.by_handler ?? [],
    };
  }

  async close(): Promise<void> {
    if (this.ownsClient) await this.sql.end();
  }

  private rowToJob(row: Record<string, unknown>): Job {
    return {
      id: row.id as string,
      kind: row.kind as Job["kind"],
      handler: row.handler as string,
      key: row.key as string,
      version: row.version as number,
      claimedVersion: (row.claimed_version as number | null) ?? null,
      status: row.status as JobStatus,
      scheduledFor: new Date(row.scheduled_for as string | number | Date),
      startedAt: row.started_at ? new Date(row.started_at as string | number | Date) : null,
      completedAt: row.completed_at ? new Date(row.completed_at as string | number | Date) : null,
      attempt: row.attempt as number,
      maxAttempts: row.max_attempts as number,
      schedulerRef: (row.scheduler_ref as string | null) ?? null,
      lastError: (row.last_error as string | null) ?? null,
      failureReason: (row.failure_reason as FailureReason | null) ?? null,
      createdAt: new Date(row.created_at as string | number | Date),
      firstAt: row.first_at ? new Date(row.first_at as string | number | Date) : null,
      lastAt: row.last_at ? new Date(row.last_at as string | number | Date) : null,
      waitMs: (row.wait_ms as number | null) ?? null,
      maxWaitMs: (row.max_wait_ms as number | null) ?? null,
      deferAttempts: (row.defer_attempts as number) ?? 0,
      deferredSince: row.deferred_since ? new Date(row.deferred_since as string | number | Date) : null,
      retryConfig: parseRetryConfig(row.retry_config),
    };
  }
}

async function readCurrentMigrationVersion(sql: postgres.Sql | postgres.ReservedSql): Promise<number> {
  const exists = await sql`
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = ${SCHEMA} AND table_name = 'migrations'
  `;
  if (exists.length === 0) return 0;
  const result = await sql`
    SELECT COALESCE(MAX(version), 0) as version FROM delaykit.migrations
  `;
  return result[0].version;
}

/**
 * Apply pending DelayKit migrations. Intended for deploy-time use
 * (e.g. a `postbuild` script). Strings get a short-lived client
 * that's closed after; `postgres.Sql` instances are caller-owned.
 */
export async function runPostgresMigrations(
  connectionStringOrClient: string | postgres.Sql,
): Promise<void> {
  const store = await PostgresStore.connect(connectionStringOrClient, {
    runMigrations: true,
  });
  await store.close();
}

/**
 * Postgres-specific cursor codec. Encodes the `completed_at::text`
 * representation (microsecond-precise) plus row id; the JS Date roundtrip
 * used for Memory/SQLite would truncate to milliseconds and skip rows that
 * share the boundary millisecond. `|` is safe — neither the timestamptz
 * text format nor UUIDs use it.
 */
function encodePostgresCursor(completedAtText: string, id: string): string {
  return Buffer.from(`${completedAtText}|${id}`, "utf8").toString("base64url");
}

function decodePostgresCursor(cursor: string): { completedAtText: string; id: string } {
  const raw = Buffer.from(cursor, "base64url").toString("utf8");
  const sep = raw.indexOf("|");
  if (sep <= 0) throw new Error(`Invalid cursor: ${cursor}`);
  const completedAtText = raw.slice(0, sep);
  const id = raw.slice(sep + 1);
  if (!completedAtText || !id) throw new Error(`Invalid cursor: ${cursor}`);
  return { completedAtText, id };
}

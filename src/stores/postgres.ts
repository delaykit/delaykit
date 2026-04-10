import type postgres from "postgres";
import { randomUUID } from "node:crypto";
import type { Job, JobStatus, Store } from "../types.js";
import { DEFAULT_TIMEOUT_MS, STALLED_GRACE_MS } from "../types.js";
import { MIGRATIONS, SCHEMA } from "./postgres-migrations.js";

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

export interface PostgresStoreOptions {
  runMigrations?: boolean;
}

export class PostgresStore implements Store {
  private sql: postgres.Sql;

  private constructor(sql: postgres.Sql) {
    this.sql = sql;
  }

  static async connect(
    connectionStringOrClient?: string | postgres.Sql,
    options?: PostgresStoreOptions,
  ): Promise<PostgresStore> {
    let sql: postgres.Sql;
    if (typeof connectionStringOrClient === "string" || connectionStringOrClient == null) {
      const resolved = connectionStringOrClient ?? process.env.DELAYKIT_DATABASE_URL;
      if (!resolved) {
        throw new Error(
          "Database connection string is required. Pass it as the first argument or set the DELAYKIT_DATABASE_URL environment variable.",
        );
      }
      const postgres = await loadPostgres();
      sql = postgres(resolved);
    } else {
      sql = connectionStringOrClient;
    }
    const store = new PostgresStore(sql);

    if (options?.runMigrations !== false) {
      await store.migrate();
    }

    return store;
  }

  private async migrate(): Promise<void> {
    const exists = await this.sql`
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = ${SCHEMA} AND table_name = 'migrations'
    `;

    let currentVersion = 0;
    if (exists.length > 0) {
      const result = await this.sql`
        SELECT COALESCE(MAX(version), 0) as version FROM delaykit.migrations
      `;
      currentVersion = result[0].version;
    }

    for (const migration of MIGRATIONS) {
      if (migration.version > currentVersion) {
        await this.sql.unsafe(migration.sql);
      }
    }
  }

  async createJob(job: Omit<Job, "createdAt">): Promise<Job> {
    const id = job.id || randomUUID();
    try {
      const rows = await this.sql`
        INSERT INTO delaykit.jobs (
          id, kind, handler, key, version, claimed_version, status,
          scheduled_for, started_at, completed_at,
          attempt, max_attempts, scheduler_ref, last_error,
          first_at, last_at, wait_ms, max_wait_ms
        ) VALUES (
          ${id}, ${job.kind}, ${job.handler}, ${job.key},
          ${job.version}, ${job.claimedVersion}, ${job.status},
          ${job.scheduledFor}, ${job.startedAt}, ${job.completedAt},
          ${job.attempt}, ${job.maxAttempts}, ${job.schedulerRef}, ${job.lastError},
          ${job.firstAt}, ${job.lastAt}, ${job.waitMs}, ${job.maxWaitMs}
        )
        RETURNING *
      `;
      return this.rowToJob(rows[0]);
    } catch (err: any) {
      if (err.code === PG_UNIQUE_VIOLATION) {
        throw new Error(`Job with active key "${job.key}" already exists (concurrent insert)`);
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
      SET status = 'cancelled', completed_at = NOW()
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
      SET status = 'running', started_at = now(), claimed_version = ${version}
      WHERE id = ${id} AND status = 'pending' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async markCompleted(id: string, version: number): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'completed', completed_at = now()
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async markFailed(id: string, version: number, error: Error): Promise<boolean> {
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET status = 'failed', last_error = ${error.message}, completed_at = now()
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
          last_error = ${lastError}
      WHERE id = ${id} AND status = 'running' AND version = ${version}
      RETURNING id
    `;
    return rows.length > 0;
  }

  async rescheduleDueAt(id: string, version: number): Promise<Job | null> {
    // Only debounce reschedules (throttle always fires), but formula handles both
    const rows = await this.sql`
      UPDATE delaykit.jobs
      SET version = version + 1,
          scheduled_for = CASE
            WHEN kind = 'throttle' THEN first_at + (wait_ms * INTERVAL '1 millisecond')
            ELSE LEAST(
              last_at + (wait_ms * INTERVAL '1 millisecond'),
              CASE WHEN max_wait_ms IS NOT NULL
                THEN first_at + (max_wait_ms * INTERVAL '1 millisecond')
                ELSE last_at + (wait_ms * INTERVAL '1 millisecond')
              END
            )
          END
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
          scheduled_for = CASE
            WHEN kind = 'throttle' THEN first_at + (wait_ms * INTERVAL '1 millisecond')
            ELSE LEAST(
              last_at + (wait_ms * INTERVAL '1 millisecond'),
              CASE WHEN max_wait_ms IS NOT NULL
                THEN first_at + (max_wait_ms * INTERVAL '1 millisecond')
                ELSE last_at + (wait_ms * INTERVAL '1 millisecond')
              END
            )
          END
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
          last_error = NULL, started_at = NULL, completed_at = NULL, claimed_version = NULL
      WHERE id = ${id} AND status = 'pending'
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
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
          scheduled_for = CASE
            WHEN kind = 'throttle' THEN first_at + (wait_ms * INTERVAL '1 millisecond')
            ELSE LEAST(
              last_at + (wait_ms * INTERVAL '1 millisecond'),
              CASE WHEN max_wait_ms IS NOT NULL
                THEN first_at + (max_wait_ms * INTERVAL '1 millisecond')
                ELSE last_at + (wait_ms * INTERVAL '1 millisecond')
              END
            )
          END
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
          started_at = NULL, claimed_version = NULL
      WHERE id = ${id} AND status = 'running'
        AND started_at IS NOT NULL
        AND started_at + (${leaseMs} * INTERVAL '1 millisecond') < now()
      RETURNING *
    `;
    return rows.length > 0 ? this.rowToJob(rows[0]) : null;
  }

  async reclaimStalledJobs(handlerTimeouts: Map<string, number>): Promise<Job[]> {
    const rows = await this.sql`
      SELECT * FROM delaykit.jobs
      WHERE status = 'running' AND started_at IS NOT NULL
    `;

    const reclaimed: Job[] = [];
    const now = Date.now();

    for (const row of rows) {
      const job = this.rowToJob(row);
      const timeout = handlerTimeouts.get(job.handler) ?? DEFAULT_TIMEOUT_MS;

      if (now - job.startedAt!.getTime() > timeout + STALLED_GRACE_MS) {
        if (job.kind !== "once" && job.claimedVersion != null && job.version > job.claimedVersion) {
          // Pattern with version advance: requeue fresh window
          const requeued = await this.sql`
            UPDATE delaykit.jobs
            SET status = 'pending',
                version = version + 1,
                started_at = NULL, completed_at = NULL, claimed_version = NULL,
                attempt = 0,
                scheduled_for = CASE
                  WHEN kind = 'throttle' THEN first_at + (wait_ms * INTERVAL '1 millisecond')
                  ELSE LEAST(
                    last_at + (wait_ms * INTERVAL '1 millisecond'),
                    CASE WHEN max_wait_ms IS NOT NULL
                      THEN first_at + (max_wait_ms * INTERVAL '1 millisecond')
                      ELSE last_at + (wait_ms * INTERVAL '1 millisecond')
                    END
                  )
                END
            WHERE id = ${job.id} AND status = 'running'
            RETURNING *
          `;
          if (requeued.length > 0) reclaimed.push(this.rowToJob(requeued[0]));
        } else {
          // Reclaim: increment attempt. Caller handles exhaustion + onFailure.
          await this.sql`
            UPDATE delaykit.jobs
            SET status = 'pending', attempt = attempt + 1,
                started_at = NULL, claimed_version = NULL
            WHERE id = ${job.id} AND status = 'running'
          `;
          job.status = "pending";
          job.attempt += 1;
          job.startedAt = null;
          job.claimedVersion = null;
          reclaimed.push(job);
        }
      }
    }

    return reclaimed;
  }

  async getDueJobs(limit: number): Promise<Job[]> {
    const rows = await this.sql`
      SELECT * FROM delaykit.jobs
      WHERE status = 'pending' AND scheduled_for <= now()
      ORDER BY scheduled_for ASC
      LIMIT ${limit}
    `;
    return rows.map((r) => this.rowToJob(r));
  }

  async close(): Promise<void> {
    await this.sql.end();
  }

  private rowToJob(row: any): Job {
    return {
      id: row.id,
      kind: row.kind,
      handler: row.handler,
      key: row.key,
      version: row.version,
      claimedVersion: row.claimed_version ?? null,
      status: row.status as JobStatus,
      scheduledFor: new Date(row.scheduled_for),
      startedAt: row.started_at ? new Date(row.started_at) : null,
      completedAt: row.completed_at ? new Date(row.completed_at) : null,
      attempt: row.attempt,
      maxAttempts: row.max_attempts,
      schedulerRef: row.scheduler_ref,
      lastError: row.last_error,
      createdAt: new Date(row.created_at),
      firstAt: row.first_at ? new Date(row.first_at) : null,
      lastAt: row.last_at ? new Date(row.last_at) : null,
      waitMs: row.wait_ms,
      maxWaitMs: row.max_wait_ms,
    };
  }
}

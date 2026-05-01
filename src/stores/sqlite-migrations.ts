/**
 * SQLite schema for DelayKit. Timestamps are INTEGER ms-since-epoch;
 * UUIDs are TEXT. No schema namespace in SQLite — tables are prefixed
 * `delaykit_` to stay out of the way of the app's own schema.
 *
 * The single-writer nature of SQLite makes an advisory lock unnecessary;
 * migrations run inside a BEGIN IMMEDIATE transaction in `SQLiteStore.migrate`.
 */

export const SQLITE_MIGRATIONS: { version: number; sql: string }[] = [
  {
    version: 1,
    sql: `
      CREATE TABLE IF NOT EXISTS delaykit_jobs (
        id              TEXT PRIMARY KEY,
        kind            TEXT NOT NULL DEFAULT 'once',
        handler         TEXT NOT NULL,
        key             TEXT NOT NULL,
        version         INTEGER NOT NULL DEFAULT 1,
        claimed_version INTEGER,
        status          TEXT NOT NULL DEFAULT 'pending',
        scheduled_for   INTEGER NOT NULL,
        started_at      INTEGER,
        completed_at    INTEGER,
        attempt         INTEGER NOT NULL DEFAULT 0,
        max_attempts    INTEGER NOT NULL DEFAULT 1,
        scheduler_ref   TEXT,
        last_error      TEXT,
        created_at      INTEGER NOT NULL,
        first_at        INTEGER,
        last_at         INTEGER,
        wait_ms         INTEGER,
        max_wait_ms     INTEGER,
        defer_attempts  INTEGER NOT NULL DEFAULT 0,
        deferred_since  INTEGER,
        retry_config    TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_delaykit_jobs_status_scheduled
        ON delaykit_jobs (scheduled_for)
        WHERE status = 'pending';

      CREATE UNIQUE INDEX IF NOT EXISTS idx_delaykit_jobs_key_active
        ON delaykit_jobs (handler, key)
        WHERE status IN ('pending', 'running');

      CREATE INDEX IF NOT EXISTS idx_delaykit_jobs_status_running
        ON delaykit_jobs (started_at)
        WHERE status = 'running';

      CREATE INDEX IF NOT EXISTS idx_delaykit_jobs_completed_at
        ON delaykit_jobs (completed_at)
        WHERE status IN ('completed', 'failed', 'cancelled')
          AND completed_at IS NOT NULL;

      CREATE TABLE IF NOT EXISTS delaykit_migrations (
        version    INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      );
    `,
  },
  {
    version: 2,
    sql: `
      ALTER TABLE delaykit_jobs ADD COLUMN failure_reason TEXT;
    `,
  },
];

export const LATEST_SQLITE_MIGRATION_VERSION: number =
  SQLITE_MIGRATIONS[SQLITE_MIGRATIONS.length - 1].version;

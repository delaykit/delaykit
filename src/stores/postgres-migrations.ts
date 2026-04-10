export const SCHEMA = "delaykit";

export const MIGRATIONS = [
  {
    version: 1,
    sql: `
      CREATE SCHEMA IF NOT EXISTS delaykit;

      CREATE TABLE IF NOT EXISTS delaykit.jobs (
        id              UUID PRIMARY KEY,
        kind            TEXT NOT NULL DEFAULT 'once',
        handler         TEXT NOT NULL,
        key             TEXT NOT NULL,
        version         INT NOT NULL DEFAULT 1,
        claimed_version INT,
        status          TEXT NOT NULL DEFAULT 'pending',
        scheduled_for   TIMESTAMPTZ NOT NULL,
        started_at      TIMESTAMPTZ,
        completed_at    TIMESTAMPTZ,
        attempt         INT NOT NULL DEFAULT 0,
        max_attempts    INT NOT NULL DEFAULT 1,
        scheduler_ref   TEXT,
        last_error      TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        first_at        TIMESTAMPTZ,
        last_at         TIMESTAMPTZ,
        wait_ms         INT,
        max_wait_ms     INT
      );

      CREATE INDEX IF NOT EXISTS idx_jobs_status_scheduled
        ON delaykit.jobs (scheduled_for)
        WHERE status = 'pending';

      CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_key_active
        ON delaykit.jobs (handler, key)
        WHERE status IN ('pending', 'running');

      CREATE INDEX IF NOT EXISTS idx_jobs_status_running
        ON delaykit.jobs (started_at)
        WHERE status = 'running';

      CREATE TABLE IF NOT EXISTS delaykit.migrations (
        version INT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );

      INSERT INTO delaykit.migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
    `,
  },
  {
    version: 2,
    sql: `
      -- Scope active-job uniqueness to (handler, key) instead of just (key).
      -- This allows different handlers to use the same key concurrently.
      DROP INDEX IF EXISTS delaykit.idx_jobs_key_active;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_key_active
        ON delaykit.jobs (handler, key)
        WHERE status IN ('pending', 'running');

      INSERT INTO delaykit.migrations (version) VALUES (2) ON CONFLICT DO NOTHING;
    `,
  },
];

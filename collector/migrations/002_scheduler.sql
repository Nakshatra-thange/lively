ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS check_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS change_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stable_streak INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_changed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_verdict TEXT,
    ADD COLUMN IF NOT EXISTS seen_hash TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_sync_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS sync_interval_seconds INTEGER NOT NULL DEFAULT 86400;

ALTER TABLE job_checks ADD COLUMN IF NOT EXISTS lease_token TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_job_checks_lease
    ON job_checks (job_id, lease_token)
    WHERE checker = 'combined' AND lease_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_due
    ON jobs (next_check_at)
    WHERE state IN ('new', 'live', 'suspect', 'reposted');

CREATE INDEX IF NOT EXISTS idx_companies_due
    ON companies (next_sync_at)
    WHERE active;

UPDATE companies SET next_sync_at = COALESCE(last_synced_at, now()) + interval '6 hours';
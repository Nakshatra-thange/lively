ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS simhash BIGINT,
    ADD COLUMN IF NOT EXISTS family_id BIGINT,
    ADD COLUMN IF NOT EXISTS family_size INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS ghost_score REAL,
    ADD COLUMN IF NOT EXISTS ghost_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS predicted_label ghost_label,
    ADD COLUMN IF NOT EXISTS ghost_scored_at TIMESTAMPTZ;

ALTER TABLE labels ADD COLUMN IF NOT EXISTS stratum TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_family ON jobs (family_id);
CREATE INDEX IF NOT EXISTS idx_jobs_ghost ON jobs (ghost_score DESC) WHERE ghost_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_predicted ON jobs (predicted_label);
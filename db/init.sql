CREATE TYPE ats_source AS ENUM ('greenhouse', 'lever', 'ashby', 'workable');
CREATE TYPE job_state AS ENUM ('new', 'live', 'suspect', 'closed', 'reposted');
CREATE TYPE check_verdict AS ENUM ('live', 'closed', 'uncertain');
CREATE TYPE ghost_label AS ENUM ('live', 'closed', 'ghost', 'repost');

CREATE TABLE companies (
    id BIGSERIAL PRIMARY KEY,
    source ats_source NOT NULL,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    board_url TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    job_count INTEGER NOT NULL DEFAULT 0,
    fail_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_synced_at TIMESTAMPTZ,
    change_rate REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, slug)
);

CREATE INDEX idx_companies_active_synced ON companies (active, last_synced_at);

CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    source ats_source NOT NULL,
    external_id TEXT NOT NULL,
    url TEXT NOT NULL,
    apply_url TEXT,
    title TEXT NOT NULL,
    location TEXT,
    department TEXT,
    employment_type TEXT,
    remote BOOLEAN,
    description TEXT NOT NULL DEFAULT '',
    content_hash TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    posted_at TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_interval_seconds INTEGER NOT NULL DEFAULT 86400,
    state job_state NOT NULL DEFAULT 'new',
    repost_of BIGINT REFERENCES jobs (id) ON DELETE SET NULL,
    repost_count INTEGER NOT NULL DEFAULT 0,
    closed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (company_id, external_id)
);

CREATE INDEX idx_jobs_state_next_check ON jobs (state, next_check_at);
CREATE INDEX idx_jobs_next_check ON jobs (next_check_at);
CREATE INDEX idx_jobs_company ON jobs (company_id);
CREATE INDEX idx_jobs_company_fp ON jobs (company_id, fingerprint);
CREATE INDEX idx_jobs_fingerprint ON jobs (fingerprint);
CREATE INDEX idx_jobs_closed_at ON jobs (closed_at) WHERE closed_at IS NOT NULL;

CREATE TABLE job_checks (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    checker TEXT NOT NULL,
    verdict check_verdict NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    http_status INTEGER,
    latency_ms INTEGER,
    reason TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_job_checks_job_time ON job_checks (job_id, checked_at DESC);
CREATE INDEX idx_job_checks_time ON job_checks (checked_at DESC);

CREATE TABLE status_events (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    from_state job_state,
    to_state job_state NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_status_events_job ON status_events (job_id, created_at DESC);
CREATE INDEX idx_status_events_time ON status_events (created_at DESC);
CREATE INDEX idx_status_events_to_state ON status_events (to_state, created_at DESC);

CREATE TABLE labels (
    job_id BIGINT PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    label ghost_label NOT NULL,
    note TEXT,
    labeled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

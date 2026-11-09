CREATE TABLE jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_key text NOT NULL UNIQUE CHECK (octet_length(request_key) BETWEEN 1 AND 200),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 200),
    duration_ms integer NOT NULL CHECK (duration_ms BETWEEN 1 AND 60000),
    should_fail boolean NOT NULL,
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempt bigint NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_until timestamptz,
    CHECK ((state = 'running') = (lease_until IS NOT NULL)),
    CHECK (state <> 'queued' OR attempt = 0),
    CHECK (state NOT IN ('running', 'succeeded', 'failed') OR attempt > 0)
);
CREATE INDEX jobs_claimable ON jobs (id) WHERE state IN ('queued', 'running');

CREATE TABLE effects (
    job_id bigint PRIMARY KEY REFERENCES jobs(id),
    attempt bigint NOT NULL CHECK (attempt > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

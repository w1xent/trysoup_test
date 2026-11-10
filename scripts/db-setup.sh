#!/bin/sh
set -eu
export PGHOST=/var/run/postgresql PGDATABASE=queue
if psql -U queue_admin -XAtqc 'SELECT 1' >/dev/null 2>&1; then
    export PGUSER=queue_admin
else
    # Upgrade a database initialized by the original Compose version.
    export PGUSER=queue
fi
export ADMIN_PASSWORD="$(cat /run/db/admin_password)"
export API_PASSWORD="$(cat /run/db/api_password)"
export WORKER_PASSWORD="$(cat /run/db/worker_password)"
psql -X -v ON_ERROR_STOP=1 <<'SQL'
\getenv admin_password ADMIN_PASSWORD
\getenv api_password API_PASSWORD
\getenv worker_password WORKER_PASSWORD
BEGIN;
SELECT 'CREATE ROLE queue_admin LOGIN SUPERUSER' WHERE NOT EXISTS(SELECT FROM pg_roles WHERE rolname='queue_admin') \gexec
ALTER ROLE queue_admin PASSWORD :'admin_password';
SELECT 'CREATE ROLE queue_api LOGIN' WHERE NOT EXISTS(SELECT FROM pg_roles WHERE rolname='queue_api') \gexec
SELECT 'CREATE ROLE queue_worker LOGIN' WHERE NOT EXISTS(SELECT FROM pg_roles WHERE rolname='queue_worker') \gexec
ALTER ROLE queue_api NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_password';
ALTER ROLE queue_worker NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'worker_password';
SET ROLE queue_admin;
\i /setup/schema.sql
ALTER DATABASE queue OWNER TO queue_admin;
ALTER SCHEMA public OWNER TO queue_admin;
ALTER TABLE jobs OWNER TO queue_admin;
ALTER TABLE effects OWNER TO queue_admin;
ALTER SEQUENCE jobs_id_seq OWNER TO queue_admin;
REVOKE ALL ON DATABASE queue FROM PUBLIC;
GRANT CONNECT ON DATABASE queue TO queue_api,queue_worker;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO queue_api,queue_worker;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM queue_api,queue_worker;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM queue_api,queue_worker;
GRANT SELECT ON jobs TO queue_api;
GRANT INSERT(owner_id,request_key,name,duration_ms,should_fail) ON jobs TO queue_api;
GRANT UPDATE(state,lease_until) ON jobs TO queue_api;
GRANT USAGE ON SEQUENCE jobs_id_seq TO queue_api;
GRANT SELECT ON jobs TO queue_worker;
GRANT UPDATE(state,lease_until,attempt) ON jobs TO queue_worker;
GRANT INSERT ON effects TO queue_worker;
SELECT 'ALTER ROLE queue NOLOGIN NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD NULL'
  WHERE EXISTS(SELECT FROM pg_roles WHERE rolname='queue') \gexec
-- PostgreSQL requires the bootstrap role (OID 10) to remain a superuser.
SELECT 'ALTER ROLE queue NOSUPERUSER'
  WHERE EXISTS(SELECT FROM pg_roles WHERE rolname='queue' AND oid<>10) \gexec
COMMIT;
SQL

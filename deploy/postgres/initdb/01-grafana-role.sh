#!/bin/sh
# Runs once, on first start of an empty data volume (postgres image /docker-entrypoint-initdb.d).
# Creates the read-only role Grafana connects as (TASK-6.5, FR-40): minion_readonly can only read
# the minion_ro views (created by migration 0007); minion_grafana logs in as it. The ENFORCED
# limits are privileges (SELECT on minion_ro only), a connection limit and temp_file_limit; the
# statement_timeout / read-only transaction are session defaults the role can SET away. The password is a DEV default; set MINION_GRAFANA_DB_PASSWORD.
# An existing volume does not re-run this: see docs/ARCHITECTURE.md §7 for the manual commands.
set -eu

psql -v ON_ERROR_STOP=1 -v pw="$MINION_GRAFANA_DB_PASSWORD" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'minion_readonly') THEN
        CREATE ROLE minion_readonly NOLOGIN;
    END IF;
END
$$;
CREATE ROLE minion_grafana LOGIN PASSWORD :'pw' IN ROLE minion_readonly;
ALTER ROLE minion_grafana SET statement_timeout = '10s';
ALTER ROLE minion_grafana SET default_transaction_read_only = on;
ALTER ROLE minion_grafana CONNECTION LIMIT 5;
-- PGC_SUSET: the role cannot raise it itself (work_mem-heavy queries spill to disk, capped here).
ALTER ROLE minion_grafana SET temp_file_limit = '1GB';
-- No TEMP tables for PUBLIC (a temp table is a writable scratch area); only the app owner keeps it.
SELECT format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT TEMPORARY ON DATABASE %I TO %I', current_database(), current_user) \gexec
-- minion_grafana may only connect to the application database.
REVOKE CONNECT ON DATABASE postgres, template1 FROM PUBLIC;
SQL

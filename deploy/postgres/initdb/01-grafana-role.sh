#!/bin/sh
# Runs once, on first start of an empty data volume (postgres image /docker-entrypoint-initdb.d).
# Creates the read-only role Grafana connects as (TASK-6.5, FR-40); the SQL and the list of what
# is and is not enforced are in 01-grafana-role.sql.in. The password is a DEV default; set
# MINION_GRAFANA_DB_PASSWORD. An existing volume does not re-run this: see docs/ARCHITECTURE.md
# section 7 for the manual commands.
set -eu

psql -v ON_ERROR_STOP=1 -v pw="$MINION_GRAFANA_DB_PASSWORD" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/01-grafana-role.sql.in

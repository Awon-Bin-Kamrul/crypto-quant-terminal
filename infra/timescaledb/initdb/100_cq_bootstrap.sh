#!/usr/bin/env bash
# ------------------------------------------------------------------------------------------
# Crypto Quant Terminal — first-boot database bootstrap.
#
# Runs once, when the TimescaleDB data volume is empty, after the image's own scripts
# (000_install_timescaledb.sh, 001_timescaledb_tune.sh). It creates:
#   * the least-privilege runtime role   (${CQ_DB_APP_USER})   DML only, no DDL
#   * the schemas                         market · trading · research · ref
#   * an isolated integration-test DB     (${CQ_DB_TEST_NAME}) with identical setup
#
# Tables, hypertables and policies are NOT created here — they belong to Alembic (M2), so
# every environment reaches the same schema through the same versioned migrations.
#
# The postgres entrypoint executes this file if it is executable and sources it otherwise,
# so it avoids `exit` and `set -u` and keeps all logic inside functions.
# ------------------------------------------------------------------------------------------

cq_require_env() {
  local name
  for name in "$@"; do
    if [ -z "${!name:-}" ]; then
      echo "cq-bootstrap: required environment variable ${name} is empty" >&2
      return 1
    fi
  done
}

cq_psql() {
  # $1 = database; remaining args are passed through to psql.
  local db="$1"
  shift
  psql --no-psqlrc -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" "$@"
}

cq_create_app_role() {
  echo "cq-bootstrap: creating runtime role ${CQ_DB_APP_USER}"
  cq_psql "${POSTGRES_DB}" \
    -v app_user="${CQ_DB_APP_USER}" \
    -v app_password="${CQ_DB_APP_PASSWORD}" <<'SQL'
CREATE ROLE :"app_user" WITH
  LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 150
  PASSWORD :'app_password';

-- Runtime guard rails: a stuck query or an abandoned transaction must never pin locks.
-- Long research reads can raise statement_timeout per session with SET LOCAL.
ALTER ROLE :"app_user" SET statement_timeout = '60s';
ALTER ROLE :"app_user" SET lock_timeout = '10s';
ALTER ROLE :"app_user" SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE :"app_user" SET timezone = 'UTC';

-- Cluster-wide: lets the app read pg_stat_statements for performance triage.
GRANT pg_read_all_stats TO :"app_user";
SQL
}

cq_create_test_database() {
  echo "cq-bootstrap: creating test database ${CQ_DB_TEST_NAME}"
  # template1 already carries the timescaledb extension (installed by the image).
  cq_psql "${POSTGRES_DB}" \
    -v test_db="${CQ_DB_TEST_NAME}" \
    -v owner="${POSTGRES_USER}" <<'SQL'
CREATE DATABASE :"test_db" OWNER :"owner" TEMPLATE template1;
SQL
}

cq_prepare_database() {
  local db="$1"
  echo "cq-bootstrap: preparing database ${db}"
  cq_psql "${db}" \
    -v db="${db}" \
    -v owner="${POSTGRES_USER}" \
    -v app_user="${CQ_DB_APP_USER}" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

ALTER DATABASE :"db" SET timezone = 'UTC';

-- Nobody but the owner and the runtime role may connect.
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db" TO :"app_user";
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS market   AUTHORIZATION :"owner";
CREATE SCHEMA IF NOT EXISTS trading  AUTHORIZATION :"owner";
CREATE SCHEMA IF NOT EXISTS research AUTHORIZATION :"owner";
CREATE SCHEMA IF NOT EXISTS ref      AUTHORIZATION :"owner";

GRANT USAGE ON SCHEMA market, trading, research, ref TO :"app_user";

-- Everything the owner creates later (via migrations) is usable, but not alterable,
-- by the runtime role. TABLES covers views and continuous aggregates too.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA market, trading, research, ref
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA market, trading, research, ref
  GRANT USAGE, SELECT ON SEQUENCES TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA market, trading, research, ref
  GRANT EXECUTE ON FUNCTIONS TO :"app_user";
SQL
}

cq_bootstrap_main() {
  cq_require_env POSTGRES_USER POSTGRES_DB CQ_DB_APP_USER CQ_DB_APP_PASSWORD CQ_DB_TEST_NAME \
    || return 1

  if [ "${CQ_DB_APP_USER}" = "${POSTGRES_USER}" ]; then
    echo "cq-bootstrap: CQ_DB_APP_USER must differ from the owner role ${POSTGRES_USER}" >&2
    return 1
  fi
  if [ "${CQ_DB_TEST_NAME}" = "${POSTGRES_DB}" ]; then
    echo "cq-bootstrap: CQ_DB_TEST_NAME must differ from the main database ${POSTGRES_DB}" >&2
    return 1
  fi

  cq_create_app_role || return 1
  cq_create_test_database || return 1
  cq_prepare_database "${POSTGRES_DB}" || return 1
  cq_prepare_database "${CQ_DB_TEST_NAME}" || return 1
  echo "cq-bootstrap: done"
}

cq_bootstrap_main

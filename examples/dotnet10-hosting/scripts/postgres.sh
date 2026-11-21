#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.yml"
STATE="$ROOT/.local/postgres"
ENV_FILE="$STATE/compose.env"
CONFIG_FILE="$STATE/universaldata.yaml"
PROJECT="ud-dotnet10-hosting"
DOTNET="${UD_DOTNET:-dotnet}"
PROJECT_FILE="$ROOT/UniversalData.Dotnet10Hosting.csproj"

usage() {
    cat >&2 <<'EOF'
Usage: scripts/postgres.sh <prepare|up|verify|pause|resume|stop|clean>

prepare creates private passwords, host policy, named source credentials, and the
primary init SQL under .local/postgres. up starts PostgreSQL 17 primary plus two
independently seeded streaming standbys. verify performs the real write, WAL replay,
pause/resume, and standby-write rejection checks.
EOF
}

secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
    else
        od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
    fi
}

compose() {
    docker compose --project-name "$PROJECT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

prepare() {
    umask 077
    mkdir -p "$STATE/private" "$STATE/primary-init" "$STATE/state" "$STATE/resources"
    local app_base="${UD_APP_BASE:-}"
    if [[ -z "$app_base" ]]; then
        if [[ -d "$ROOT/bin/Debug/net10.0" ]]; then app_base="$ROOT/bin/Debug/net10.0"; else app_base="$ROOT/bin/Release/net10.0"; fi
    fi
    mkdir -p "$app_base"
    app_base="$(cd "$app_base" && pwd)"

    if [[ ! -s "$ENV_FILE" ]]; then
        cat >"$ENV_FILE" <<EOF
COMPOSE_PROJECT_NAME=$PROJECT
UD_POSTGRES_SUPERUSER_PASSWORD=$(secret)
UD_REPLICATION_PASSWORD=$(secret)
UD_WRITER_PASSWORD=$(secret)
UD_READER_PASSWORD=$(secret)
UD_PRIMARY_PORT=${UD_PRIMARY_PORT:-55432}
UD_REPLICA_1_PORT=${UD_REPLICA_1_PORT:-55433}
UD_REPLICA_2_PORT=${UD_REPLICA_2_PORT:-55434}
EOF
    fi
    # shellcheck disable=SC1090
    source "$ENV_FILE"

    cat >"$STATE/pg_hba.conf" <<EOF
local all all trust
host replication replicator 0.0.0.0/0 scram-sha-256
host all all 0.0.0.0/0 scram-sha-256
EOF

    cat >"$STATE/primary-init/01-roles-and-schema.sql" <<EOF
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '$UD_REPLICATION_PASSWORD';
CREATE ROLE orders_writer LOGIN PASSWORD '$UD_WRITER_PASSWORD';
CREATE ROLE orders_reader LOGIN PASSWORD '$UD_READER_PASSWORD';
ALTER ROLE orders_reader SET default_transaction_read_only = on;

\connect orders
CREATE TABLE orders (
    id INTEGER NOT NULL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    total_cents BIGINT NOT NULL
);
CREATE INDEX orders_shop_sequence_id_idx ON orders (shop_id, sequence DESC, id ASC);
GRANT CONNECT ON DATABASE orders TO orders_writer, orders_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO orders_writer, orders_reader;
GRANT SELECT, INSERT ON TABLE orders TO orders_writer;
GRANT SELECT ON TABLE orders TO orders_reader;
EOF
    chmod 700 "$STATE"
    chmod 755 "$STATE/primary-init"
    chmod 644 "$STATE/pg_hba.conf" "$STATE/primary-init/01-roles-and-schema.sql"

    local primary_connection="host=127.0.0.1 port=$UD_PRIMARY_PORT dbname=orders user=orders_writer password=$UD_WRITER_PASSWORD"
    local replica1_connection="host=127.0.0.1 port=$UD_REPLICA_1_PORT dbname=orders user=orders_reader password=$UD_READER_PASSWORD"
    local replica2_connection="host=127.0.0.1 port=$UD_REPLICA_2_PORT dbname=orders user=orders_reader password=$UD_READER_PASSWORD"
    local replica1_write_connection="host=127.0.0.1 port=$UD_REPLICA_1_PORT dbname=orders user=orders_writer password=$UD_WRITER_PASSWORD"
    local replica2_write_connection="host=127.0.0.1 port=$UD_REPLICA_2_PORT dbname=orders user=orders_writer password=$UD_WRITER_PASSWORD"
    printf '%s' "$primary_connection" >"$STATE/private/orders-primary.connection"
    printf '%s' "$replica1_connection" >"$STATE/private/orders-replica-1.connection"
    printf '%s' "$replica2_connection" >"$STATE/private/orders-replica-2.connection"
    printf '%s' "$replica1_write_connection" >"$STATE/private/orders-replica-1-write.connection"
    printf '%s' "$replica2_write_connection" >"$STATE/private/orders-replica-2-write.connection"

    cat >"$STATE/private/host-policy.json" <<EOF
{
  "dataDirectory": "$(printf '%s' "$STATE/state")",
  "fileRoots": ["$(printf '%s' "$STATE/resources")"],
  "networkOrigins": [],
  "pluginRoots": ["$(printf '%s' "$app_base")"],
  "credentialFiles": {
    "orders-primary": "$(printf '%s' "$STATE/private/orders-primary.connection")",
    "orders-replica-1": "$(printf '%s' "$STATE/private/orders-replica-1.connection")",
    "orders-replica-2": "$(printf '%s' "$STATE/private/orders-replica-2.connection")",
    "orders-replica-1-write": "$(printf '%s' "$STATE/private/orders-replica-1-write.connection")",
    "orders-replica-2-write": "$(printf '%s' "$STATE/private/orders-replica-2-write.connection")"
  },
  "resources": {
    "orders-primary": ["Open", "Read", "Write", "Discover"],
    "orders-replica-1": ["Open", "Read", "Discover"],
    "orders-replica-2": ["Open", "Read", "Discover"],
    "orders-replica-1-write": ["Open", "Read", "Write", "Discover"],
    "orders-replica-2-write": ["Open", "Read", "Write", "Discover"]
  },
  "tenants": {
    "dotnet10-hosting": {
      "fileRoots": ["$(printf '%s' "$STATE/resources")"],
      "networkOrigins": [],
      "pluginRoots": ["$(printf '%s' "$app_base")"],
      "credentialReferences": ["orders-primary", "orders-replica-1", "orders-replica-2", "orders-replica-1-write", "orders-replica-2-write"],
      "resources": {
        "orders-primary": ["Open", "Read", "Write", "Discover"],
        "orders-replica-1": ["Open", "Read", "Discover"],
        "orders-replica-2": ["Open", "Read", "Discover"],
        "orders-replica-1-write": ["Open", "Read", "Write", "Discover"],
        "orders-replica-2-write": ["Open", "Read", "Write", "Discover"]
      }
    }
  }
}
EOF
    cat >"$CONFIG_FILE" <<EOF
hostPolicy:
  path: '$STATE/private/host-policy.json'
  tenant: dotnet10-hosting
sources:
  orders-primary:
    kind: postgresql
    resource: orders-primary
    credentialReference: orders-primary
  orders-replica-1:
    kind: postgresql
    resource: orders-replica-1
    credentialReference: orders-replica-1
  orders-replica-2:
    kind: postgresql
    resource: orders-replica-2
    credentialReference: orders-replica-2
  orders-replica-1-write:
    kind: postgresql
    resource: orders-replica-1-write
    credentialReference: orders-replica-1-write
  orders-replica-2-write:
    kind: postgresql
    resource: orders-replica-2-write
    credentialReference: orders-replica-2-write
worker:
  maxConcurrentSessions: 4
  maxConnectionsPerSource: 2
EOF
    printf 'PostgreSQL fixture prepared at %s\n' "$STATE"
}

wait_ready() {
    local service="$1"
    for _ in $(seq 1 60); do
        if compose exec -T "$service" pg_isready -U postgres -d orders >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    echo "$service did not become ready" >&2
    return 1
}

up() {
    prepare
    compose up -d orders-primary orders-replica-1 orders-replica-2
    wait_ready orders-primary
    wait_ready orders-replica-1
    wait_ready orders-replica-2
}

primary_psql() {
    admin_psql orders-primary "$1"
}

admin_psql() {
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    compose exec -T -e "PGPASSWORD=$UD_POSTGRES_SUPERUSER_PASSWORD" "$1" \
        psql -U postgres -d orders -Atqc "$2"
}

reader_psql() {
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    local service="$1"; shift
    compose exec -T -e "PGPASSWORD=$UD_READER_PASSWORD" "$service" \
        psql -h 127.0.0.1 -U orders_reader -d orders -Atqc "$1"
}

writer_psql() {
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    local service="$1"; shift
    compose exec -T -e "PGPASSWORD=$UD_WRITER_PASSWORD" "$service" \
        psql -h 127.0.0.1 -U orders_writer -d orders --set=VERBOSITY=verbose -Atqc "$1"
}

run_host_write() {
    run_host_write_on_source "$1" "$2" "$3" orders-primary
}

run_host_write_on_source() {
    local id="$1"; local sequence="$2"; local total="$3"; local source="$4"
    # Generic Host uses AddUniversalData(configPath), while OrdersService is the same singleton
    # used by the ASP.NET endpoint. No fake client or direct ADO.NET path is involved.
    UD_UNIVERSALDATA_CONFIG="$CONFIG_FILE" Orders__DefaultSource="$source" \
        "$DOTNET" run --project "$PROJECT_FILE" --no-build -- \
        --command write --source "$source" --id "$id" --shop-id 1 --sequence "$sequence" --total "$total"
}

wait_replay() {
    local service="$1"; local lsn="$2"
    for _ in $(seq 1 60); do
        if [[ "$(reader_psql "$service" "SELECT CASE WHEN pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn THEN 1 ELSE 0 END")" == "1" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "$service did not replay WAL position $lsn" >&2
    return 1
}

verify() {
    up
    local first_id=$((100000000 + $(od -An -N4 -tu4 /dev/urandom) % 1000000000))
    local second_id=$((first_id + 1)) absent_a=$((first_id + 2)) absent_b=$((first_id + 3))
    run_host_write "$first_id" "$first_id" 12.50 >/dev/null
    local first_lsn
    first_lsn="$(primary_psql "SELECT pg_current_wal_lsn()")"
    [[ "$(primary_psql 'SELECT pg_is_in_recovery()')" == "f" ]]
    wait_replay orders-replica-1 "$first_lsn"
    wait_replay orders-replica-2 "$first_lsn"
    for service in orders-replica-1 orders-replica-2; do
        [[ "$(reader_psql "$service" "SELECT id || ':' || shop_id || ':' || sequence || ':' || total_cents FROM orders WHERE id = $first_id")" == "$first_id:1:$first_id:1250" ]]
    done

    # Pause one standby, prove primary read-after-write still works, then resume and wait for
    # that independent instance to replay the second commit.
    local replica=orders-replica-1
    admin_psql "$replica" "SELECT pg_wal_replay_pause()" >/dev/null
    trap 'admin_psql orders-replica-1 "SELECT pg_wal_replay_resume()" >/dev/null || true' EXIT
    for _ in $(seq 1 60); do
        [[ "$(admin_psql "$replica" "SELECT pg_get_wal_replay_pause_state()")" != paused ]] || break
        sleep 1
    done
    [[ "$(admin_psql "$replica" "SELECT pg_get_wal_replay_pause_state()")" == paused ]]
    run_host_write "$second_id" "$second_id" 13.50 >/dev/null
    [[ "$(primary_psql "SELECT id || ':' || total_cents FROM orders WHERE id = $second_id")" == "$second_id:1350" ]]
    local second_lsn
    second_lsn="$(primary_psql "SELECT pg_current_wal_lsn()")"
    [[ "$(reader_psql "$replica" "SELECT COUNT(*) FROM orders WHERE id = $second_id")" == 0 ]]
    admin_psql "$replica" "SELECT pg_wal_replay_resume()" >/dev/null
    trap - EXIT
    wait_replay "$replica" "$second_lsn"
    wait_replay orders-replica-2 "$second_lsn"

    # The writer credential is intentionally available as a named probe source. PostgreSQL hot
    # standby must reject the actual SDK INSERT; success would fail this verification.
    local service rejected probe="$absent_a"
    for service in orders-replica-1 orders-replica-2; do
        if run_host_write_on_source "$probe" "$probe" 99.99 "$service-write" >/dev/null 2>&1; then
            echo "Unexpected SDK write to $service" >&2
            return 1
        fi
        if rejected="$(writer_psql "$service" "INSERT INTO orders VALUES ($probe, 1, $probe, 9999)" 2>&1)"; then
            echo "Unexpected database write to $service" >&2
            return 1
        fi
        [[ "$rejected" == *25006* ]]
        [[ "$(reader_psql "$service" "SELECT COUNT(*) FROM orders WHERE id = $probe")" == 0 ]]
        [[ "$(reader_psql "$service" "SELECT total_cents FROM orders WHERE id = $second_id")" == 1350 ]]
        probe="$absent_b"
    done
    local streaming
    streaming="$(primary_psql "SELECT COUNT(*) FROM pg_stat_replication WHERE state = 'streaming' AND application_name IN ('orders-replica-1', 'orders-replica-2')")"
    [[ "$streaming" == "2" ]]

    echo "primary_recovery=$(primary_psql 'SELECT pg_is_in_recovery()')"
    echo "replica_1_recovery=$(reader_psql orders-replica-1 'SELECT pg_is_in_recovery()')"
    echo "replica_2_recovery=$(reader_psql orders-replica-2 'SELECT pg_is_in_recovery()')"
    echo "first_commit_wal=$first_lsn"
    echo "second_commit_wal=$second_lsn"
    echo "streaming_standbys=$streaming"
    echo "business_key=$first_id replicated independently to both standbys"
    echo "standby_writes=replica-1-and-replica-2-rejected"
}

action="${1:-}"
case "$action" in
    prepare) prepare ;;
    up) up ;;
    verify) verify ;;
    pause) prepare; admin_psql "${2:-orders-replica-1}" "SELECT pg_wal_replay_pause()" ;;
    resume) prepare; admin_psql "${2:-orders-replica-1}" "SELECT pg_wal_replay_resume()" ;;
    stop) prepare; compose down ;;
    clean)
        prepare
        compose down --volumes --remove-orphans
        case "$STATE" in "$ROOT/.local/postgres") ;; *) echo "Refusing unexpected clean path: $STATE" >&2; exit 1 ;; esac
        rm -rf -- "$STATE"
        ;;
    *) usage; exit 2 ;;
esac

#!/usr/bin/env bash
set -Eeuo pipefail

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"
: "${PRIMARY_PORT:?PRIMARY_PORT is required}"
: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"
: "${REPLICA_NAME:?REPLICA_NAME is required}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

mkdir -p "$PGDATA"
if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
    find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    export PGPASSWORD="$REPLICATION_PASSWORD"
    until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER" -d orders >/dev/null 2>&1; do
        sleep 1
    done
    pg_basebackup \
        --host="$PRIMARY_HOST" \
        --port="$PRIMARY_PORT" \
        --username="$REPLICATION_USER" \
        --pgdata="$PGDATA" \
        --format=plain \
        --wal-method=stream \
        --write-recovery-conf \
        --progress
    unset PGPASSWORD
    # pg_basebackup writes primary_conninfo. Add an explicit application name so the
    # primary's pg_stat_replication and each standby's replay position are identifiable.
    printf "\nprimary_conninfo = 'host=%s port=%s user=%s password=%s application_name=%s'\n" \
        "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPLICATION_USER" "$REPLICATION_PASSWORD" "$REPLICA_NAME" \
        >> "$PGDATA/postgresql.auto.conf"
    touch "$PGDATA/standby.signal"
    chown -R postgres:postgres "$PGDATA"
fi

exec docker-entrypoint.sh postgres -c hot_standby=on -c listen_addresses='*'

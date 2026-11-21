#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-sqlite}"
OUTPUT="${2:-$ROOT/.local}"
APP_BASE="${UD_APP_BASE:-${3:-}}"
if [[ -z "$APP_BASE" ]]; then
    if [[ -d "$ROOT/bin/Debug/net10.0" ]]; then
        APP_BASE="$ROOT/bin/Debug/net10.0"
    else
        APP_BASE="$ROOT/bin/Release/net10.0"
    fi
fi

if [[ "$MODE" != "sqlite" ]]; then
    echo "Usage: $0 sqlite [output-directory] [application-base-directory]" >&2
    exit 2
fi

case "$OUTPUT" in
    /*) ;;
    *) OUTPUT="$ROOT/$OUTPUT" ;;
esac
APP_BASE="$(cd "$APP_BASE" 2>/dev/null && pwd || printf '%s' "$APP_BASE")"
mkdir -p "$APP_BASE" "$OUTPUT/sqlite/resources" "$OUTPUT/sqlite/private" "$OUTPUT/sqlite/state"
chmod 700 "$OUTPUT/sqlite/private" "$OUTPUT/sqlite/state"

database="$OUTPUT/sqlite/resources/orders.sqlite"
policy="$OUTPUT/sqlite/private/host-policy.json"
credential="$OUTPUT/sqlite/private/orders-sqlite.connection"
printf 'Data Source=%s' "$database" >"$credential"
chmod 600 "$credential"

cat >"$policy" <<EOF
{
  "dataDirectory": "$(printf '%s' "$OUTPUT/sqlite/state")",
  "fileRoots": ["$(printf '%s' "$OUTPUT/sqlite/resources")"],
  "networkOrigins": [],
  "pluginRoots": ["$(printf '%s' "$APP_BASE")"],
  "credentialFiles": {"orders-sqlite": "$(printf '%s' "$credential")"},
  "resources": {"$(printf '%s' "$database")": ["Open", "Read", "Write", "Discover"]},
  "tenants": {
    "dotnet10-hosting": {
      "fileRoots": ["$(printf '%s' "$OUTPUT/sqlite/resources")"],
      "networkOrigins": [],
      "pluginRoots": ["$(printf '%s' "$APP_BASE")"],
      "credentialReferences": ["orders-sqlite"],
      "resources": {"$(printf '%s' "$database")": ["Open", "Read", "Write", "Discover"]}
    }
  }
}
EOF
chmod 600 "$policy"

cat >"$OUTPUT/sqlite/universaldata.yaml" <<EOF
hostPolicy:
  path: '$policy'
  tenant: dotnet10-hosting
sources:
  orders-sqlite:
    kind: sqlite
    resource: '$database'
    credentialReference: orders-sqlite
worker:
  maxConcurrentSessions: 2
  maxConnectionsPerSource: 2
EOF
chmod 600 "$OUTPUT/sqlite/universaldata.yaml"
printf 'SQLite fixture generated at %s\n' "$OUTPUT/sqlite"

"""Real disposable PostgreSQL with verified TLS, native SQL and logical CDC."""
import argparse
import json
import os
import pathlib
import secrets
import socket
import subprocess
import tempfile

p = argparse.ArgumentParser()
p.add_argument('--bin', required=True, type=pathlib.Path)
p.add_argument('--build', required=True, type=pathlib.Path)
a = p.parse_args()
binaries, build = a.bin.resolve(), a.build.resolve()
def run(command, **kw):
    return subprocess.run(list(map(str, command)), check=True, capture_output=True,
                          text=True, timeout=60, **kw)
with tempfile.TemporaryDirectory(prefix='ud-advanced-pg-') as tmp:
    root = pathlib.Path(tmp)
    root.chmod(0o700)
    data, local = root / 'data', root / 'socket'
    local.mkdir()
    password = secrets.token_urlsafe(24)
    pwfile = root / 'password'
    pwfile.write_text(password)
    pwfile.chmod(0o600)
    run([binaries / 'initdb', '-D', data, '-U', 'fixture', '--pwfile', pwfile,
         '-A', 'scram-sha-256', '--no-locale', '-E', 'UTF8'])
    run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
         '-keyout', data / 'server.key', '-out', data / 'server.crt', '-subj', '/CN=localhost',
         '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1', '-addext', 'extendedKeyUsage=serverAuth'])
    (data / 'server.key').chmod(0o600)
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    with (data / 'postgresql.conf').open('a') as f:
        f.write(f"\nlisten_addresses='127.0.0.1'\nport={port}\nssl=on\nwal_level=logical\nmax_replication_slots=4\nmax_wal_senders=4\n")
    (data / 'pg_hba.conf').write_text('local all all scram-sha-256\nhostssl all all 127.0.0.1/32 scram-sha-256\nhostssl replication all 127.0.0.1/32 scram-sha-256\n')
    config = root / 'config'
    config.write_text(f"host=localhost hostaddr=127.0.0.1 port={port} user=fixture dbname=postgres password={password} sslmode=verify-full sslrootcert={data / 'server.crt'} connect_timeout=10")
    config.chmod(0o600)
    started = False
    try:
        run([binaries / 'pg_ctl', '-D', data, '-l', root / 'server.log', '-o', f'-k {local}', '-w', 'start'])
        started = True
        for target in ('sql', 'cdc'):
            result = run([build / f'tests/test_advanced_{target}', build / 'plugins/ud_connector_postgresql.so', config])
            print(result.stderr.strip(), flush=True)
            print(result.stdout.strip(), flush=True)
    finally:
        if started:
            run([binaries / 'pg_ctl', '-D', data, '-m', 'immediate', '-w', 'stop'])
print('PostgreSQL verified TLS, 1003-row typed receipts, prepared cursor and snapshot/WAL resume passed')

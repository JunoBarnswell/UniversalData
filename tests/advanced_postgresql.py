"""Real disposable PostgreSQL with verified TLS, native SQL and logical CDC."""
import argparse
import json
import os
import pathlib
import secrets
import socket
import sqlite3
import subprocess
import tempfile
from product_version import PROTOCOL_VERSION

p = argparse.ArgumentParser()
p.add_argument('--bin', required=True, type=pathlib.Path)
p.add_argument('--build', required=True, type=pathlib.Path)
a = p.parse_args()
binaries, build = a.bin.resolve(), a.build.resolve()
def run(command, **kw):
    result = subprocess.run(list(map(str, command)), capture_output=True,
                            text=True, timeout=60, **kw)
    if result.returncode:
        raise RuntimeError(f"fixture child exited {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result
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
        if (build / 'plugins/ud_connector_sqlite.so').is_file():
            env = dict(os.environ); env['PGPASSWORD'] = password
            run([binaries / 'psql', '-h', '127.0.0.1', '-p', port, '-U', 'fixture', '-d', 'postgres',
                 '-c', "INSERT INTO ud_advanced_cdc SELECT n,'stream-'||n,1 FROM generate_series(2,100) n"], env=env)
            target = root / 'target.db'
            with sqlite3.connect(target) as db:
                db.execute('CREATE TABLE output(id INTEGER NOT NULL PRIMARY KEY,name TEXT,rev INTEGER NOT NULL)')
            state, spill = root / 'state', root / 'spill'; state.mkdir(); spill.mkdir()
            fields = [dict(name='id',type='int64',nullable=False,keyOrder=1),dict(name='name',type='utf8',nullable=True),dict(name='rev',type='int64',nullable=False)]
            package = dict(version=PROTOCOL_VERSION,id='actual-cdc-epoch',
                connections=[dict(id='source',driver=str(build / 'plugins/ud_connector_postgresql.so'),resource='application_name=ud-advanced-cdc',credentialReference='file:'+str(config)),
                             dict(id='target',driver=str(build / 'plugins/ud_connector_sqlite.so'),resource=str(target),credentialReference='fixture'),
                             dict(id='spill',driver='file',resource=str(spill))],
                policy=dict(buffers=dict(connection='spill',residentBytes=1,workingBytes=8388608,spillBytes=10000000,maxBlocks=4096)),
                controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='p'))],constraints=[]),
                dataFlows=[dict(id='p',components=[
                    dict(id='source',component='postgresql.cdc.source',config=dict(connection='source',table='ud_advanced_cdc',schema=dict(version=PROTOCOL_VERSION,fields=fields),slot='ud_advanced_pipeline',publication='ud_advanced_publication',maxTransactions=1)),
                    dict(id='destination',component='sql.destination',config=dict(connection='target',table='output',mode='changeset',beforeField='before',keys=[0],provisionReceipts=True,
                         schemaPolicy=dict(policy='explicitMapping',mapping=[dict(expected=name,actual=name) for name in ('id','name','rev')])))],
                    paths=[{'from':'source','output':0,'to':'destination','input':0}])])
            discovery, schema = root / 'discovery.json', root / 'schema.json'
            discovery.write_text(json.dumps({**package,'connections':[
                {**package['connections'][0],'credentialReference':'fixture'},
                *package['connections'][1:]]}))
            discovery_env = dict(env,PGHOST='localhost',PGHOSTADDR='127.0.0.1',PGPORT=str(port),
                PGUSER='fixture',PGDATABASE='postgres',PGSSLMODE='verify-full',PGSSLROOTCERT=str(data / 'server.crt'))
            run([build / 'tests/test_next_phase_queries','--discover',discovery,'source','ud_advanced_cdc',schema],env=discovery_env)
            package['dataFlows'][0]['components'][0]['config']['schema'] = json.loads(schema.read_text())
            document = root / 'pipeline.json'; document.write_text(json.dumps(package))
            runner = build / 'tests/test_next_phase_execution'
            interrupted = subprocess.run([str(runner),str(document),str(state),'0','2','4','1','destination'],capture_output=True,text=True,timeout=60)
            assert interrupted.returncode == 91, (interrupted.returncode,interrupted.stdout,interrupted.stderr)
            resumed = run([runner,document,state,'1','2'])
            facts = json.loads(resumed.stdout); assert facts['result'] == 0, facts
            with sqlite3.connect(target) as db:
                rows = db.execute('SELECT id,name,rev FROM output ORDER BY id').fetchall()
                assert rows == [(1,'updated',2)] + [(i,'stream-'+str(i),1) for i in range(2,101)]
            assert not list(spill.glob('*.udw'))
            print('Actual PostgreSQL CDC -> Native Buffer/Barrier/Journal -> SQLite Changeset: target commit crash and exact 100-row resume passed',flush=True)
    finally:
        if started:
            run([binaries / 'pg_ctl', '-D', data, '-m', 'immediate', '-w', 'stop'])
print('PostgreSQL verified TLS, 1003-row typed receipts, prepared cursor and snapshot/WAL resume passed')

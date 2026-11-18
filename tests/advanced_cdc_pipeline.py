"""Actual CDC -> existing Native Epoch/Journal -> real SQLite receipt recovery."""
import argparse
import json
import pathlib
import sqlite3
import subprocess
import tempfile
from product_version import PROTOCOL_VERSION

p = argparse.ArgumentParser()
p.add_argument('--build', required=True, type=pathlib.Path)
p.add_argument('--driver', required=True, type=pathlib.Path)
p.add_argument('--credentials', required=True, type=pathlib.Path)
p.add_argument('--component', required=True)
p.add_argument('--added-column', action='store_true')
p.add_argument('--slot')
a = p.parse_args(); build = a.build.resolve()
with tempfile.TemporaryDirectory(prefix='ud-actual-cdc-epoch-') as folder:
    root = pathlib.Path(folder); root.chmod(0o700)
    target = root / 'target.db'; spill = root / 'spill'; state = root / 'state'; spill.mkdir(); state.mkdir()
    fields = [dict(name='id',type='int64',nullable=False),dict(name='name',type='utf8',nullable=True),dict(name='rev',type='int64',nullable=False)]
    if a.added_column: fields.append(dict(name='added',type='int64',nullable=True))
    with sqlite3.connect(target) as db:
        db.execute('CREATE TABLE output(id INTEGER NOT NULL PRIMARY KEY,name TEXT,rev INTEGER NOT NULL'+(',added INTEGER' if a.added_column else '')+')')
    source = dict(connection='source',table='ud_advanced_cdc',schema=dict(version=PROTOCOL_VERSION,fields=fields),serverId=64029,maxTransactions=1)
    if a.slot: source['slot'] = a.slot
    package = dict(version=PROTOCOL_VERSION,id='actual-cdc-epoch',
        connections=[dict(id='source',driver=str(a.driver.resolve()),resource='actual-source',credentialReference='file:'+str(a.credentials.resolve())),
                     dict(id='target',driver=str(build / 'plugins/ud_connector_sqlite.so'),resource=str(target),credentialReference='fixture'),
                     dict(id='spill',driver='file',resource=str(spill))],
        policy=dict(buffers=dict(connection='spill',residentBytes=1,workingBytes=8388608,spillBytes=10000000,maxBlocks=4096)),
        controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='p'))],constraints=[]),
        dataFlows=[dict(id='p',components=[dict(id='source',component=a.component,config=source),
            dict(id='destination',component='sql.destination',config=dict(connection='target',table='output',mode='changeset',beforeField='before',keys=[0],provisionReceipts=True))],
            paths=[{'from':'source','output':0,'to':'destination','input':0}])])
    doc = root / 'pipeline.json'; doc.write_text(json.dumps(package))
    runner = build / 'tests/test_next_phase_execution'
    stopped = subprocess.run([str(runner),str(doc),str(state),'0','2','4','1','destination'],capture_output=True,text=True,timeout=90)
    assert stopped.returncode == 91, (stopped.returncode,stopped.stdout,stopped.stderr)
    resumed = subprocess.run([str(runner),str(doc),str(state),'1','2'],capture_output=True,text=True,timeout=90)
    assert resumed.returncode == 0, (resumed.returncode,resumed.stdout,resumed.stderr)
    facts = json.loads(resumed.stdout); assert facts['result'] == 0, facts
    with sqlite3.connect(target) as db:
        rows = db.execute('SELECT * FROM output ORDER BY id').fetchall()
        assert rows == [(1,'updated',2,None) if a.added_column else (1,'updated',2)],rows
        assert db.execute('SELECT COUNT(*) FROM ud_sdk_receipts_v11').fetchone()[0] > 0
    assert not list(spill.glob('*.udw'))
print(json.dumps(dict(component=a.component,actualSource=True,nativeEpoch=True,targetCommitCrash=True,typedReceiptResume=True,bufferSpillCleanup=True)))

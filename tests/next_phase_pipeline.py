"""R1–R5 real destinations, native snapshots, receipts, and separate-process kills."""
from contextlib import closing
import collections, copy, json, pathlib, sqlite3, subprocess, sys, tempfile
from product_version import PROTOCOL_VERSION as VERSION
DRIVER,PLUGIN=sys.argv[1:3]
def schema(fields): return dict(version=VERSION,fields=fields)
def field(name,kind='int64',nullable=False,**extra): return dict(name=name,type=kind,nullable=nullable,**extra)
def component(id,kind,**config): return dict(id=id,component=kind,config=config)
def edge(source,target,output=0,input=0): return {'from':source,'output':output,'to':target,'input':input}
def package(root,count=10003,duplicates='first'):
    reference=[dict(ref_tenant=t,ref_key=k,label=f'{t}-{k}') for t in ('A','B') for k in (101,102,103)]
    reference.append(dict(ref_tenant='A',ref_key=101,label='duplicate'))
    (root/'reference.ndjson').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in reference))
    rows=[];errors=collections.Counter();normal=[]
    for id in range(1,count+1):
        tenant='Z' if id%29==0 else 'AB'[id%2];fk=None if id%23==0 else 999 if id%31==0 else 101+id%3
        amount='bad' if id%19==0 else None if id%17==0 else '200' if id%13==0 else str(id%11)
        rows.append((1000,id,tenant,fk,amount))
        if amount=='bad': errors['cast']+=1;continue
        violations=[]
        if amount is None:violations.append('required')
        elif int(amount)>100:violations.append('range')
        if tenant=='Z':violations.append('tenant')
        if fk is not None and (tenant=='Z' or fk==999):violations.append('fk')
        if violations:errors.update(violations);continue
        if fk is None:errors['cache.miss']+=1;continue
        normal.append((1000,id,tenant,fk,int(amount),int(amount)*2,tenant,fk,f'{tenant}-{fk}'))
    with closing(sqlite3.connect(root/'source.db')) as db, db:
        db.execute('CREATE TABLE src(water INTEGER NOT NULL,id INTEGER NOT NULL,tenant TEXT NOT NULL,fk INTEGER,amount TEXT,PRIMARY KEY(water,id))')
        db.executemany('INSERT INTO src VALUES(?,?,?,?,?)',rows)
    with closing(sqlite3.connect(root/'target.db')) as db, db:
        db.execute('CREATE TABLE output(water INTEGER NOT NULL,id INTEGER NOT NULL,tenant TEXT NOT NULL,fk INTEGER,amount INTEGER NOT NULL,doubled INTEGER NOT NULL,ref_tenant TEXT NOT NULL,ref_key INTEGER NOT NULL,label TEXT NOT NULL,PRIMARY KEY(water,id))')
        db.execute('CREATE TABLE errors(executionId TEXT NOT NULL,taskId TEXT NOT NULL,pipelineId TEXT NOT NULL,componentId TEXT NOT NULL,ruleId TEXT NOT NULL,ruleVersion INTEGER NOT NULL,columnId TEXT NOT NULL,sourceRowId TEXT NOT NULL,errorCode INTEGER NOT NULL,policy TEXT NOT NULL,errorKind TEXT NOT NULL,providerCode TEXT NOT NULL,inputFormat TEXT NOT NULL,sourcePosition U64_TEXT,packageId TEXT NOT NULL,PRIMARY KEY(sourceRowId,ruleId,componentId))')
    (root/'spill').mkdir();(root/'state').mkdir()
    original=schema([field('water',keyOrder=1),field('id',keyOrder=2),field('tenant','utf8'),field('fk',nullable=True),field('amount','utf8',True)])
    converted=copy.deepcopy(original);converted['fields'][-1]['type']='int64'
    ref=schema([field('ref_tenant','utf8'),field('ref_key'),field('label','utf8')])
    connections=[dict(id=id,driver='file',resource=str(root/path)) for id,path in [('reference','reference.ndjson'),('spill','spill'),('normalFile','normal.ndjson'),('castErrors','cast.ndjson'),('qualityErrors','quality.ndjson'),('missErrors','miss.ndjson'),('duplicateErrors','duplicate.ndjson')]]
    connections.extend(dict(id=id,driver=PLUGIN,resource=str(root/path),credentialReference='fixture',maxLeases=8) for id,path in [('source','source.db'),('target','target.db')])
    build=[component('reference','file.ndjson.source',connection='reference',schema=ref,batchSize=3),component('build','cacheBuild',cache='ref',mapping=[0,1,2])]
    build_paths=[edge('reference','build')]
    if duplicates=='error':build.append(component('duplicateErrors','file.ndjson.destination',connection='duplicateErrors'));build_paths.append(edge('build','duplicateErrors'))
    nodes=[component('facts','sql.source',connection='source',table='src',sql='SELECT * FROM src ORDER BY id DESC',schema=original,batchSize=127,incremental=dict(mode='composite',keys=['water','id'],after=[1000,0],unique=True,monotonic=True),schemaPolicy=dict(policy='strict')),
        component('reference','file.ndjson.source',connection='reference',schema=ref,batchSize=2),
        component('cast','cast',schema=converted,errorPolicy='redirect'),
        component('derive','derive',columns=[dict(name='doubled',expression={'op':'mul','args':[{'column':'amount'},{'literal':2,'type':'int64'}]})]),
        component('quality','quality',errorPolicy='redirect',rules=[dict(id='required',version=2,kind='notNull',columns=['amount','doubled']),dict(id='range',kind='range',columns=['amount'],minimum=0,maximum=100),dict(id='tenant',kind='regex',columns=['tenant'],pattern='^[AB]$'),dict(id='fk',kind='foreignKey',columns=['tenant','fk'],referenceColumns=['ref_tenant','ref_key'])]),
        component('lookup','cacheLookup',cache='ref',keys=[2,3],errorPolicy='redirect'),
        component('fan','multicast'),component('normalSQL','sql.destination',connection='target',table='output',mode='upsert',keys=[0,1],provisionReceipts=True),component('normalFile','file.ndjson.destination',connection='normalFile')]
    paths=[edge('facts','cast'),edge('cast','derive'),edge('derive','quality'),edge('reference','quality',input=1),edge('quality','lookup'),edge('lookup','fan'),edge('fan','normalSQL'),edge('fan','normalFile',output=1)]
    for origin,file_connection in [('cast','castErrors'),('quality','qualityErrors'),('lookup','missErrors')]:
        nodes.extend([component(origin+'Fan','multicast'),component(origin+'File','file.ndjson.destination',connection=file_connection),component(origin+'SQL','sql.destination',connection='target',table='errors',mode='upsert',keys=[7,4,3],provisionReceipts=True)])
        paths.extend([edge(origin,origin+'Fan',output=1),edge(origin+'Fan',origin+'SQL'),edge(origin+'Fan',origin+'File',output=1)])
    doc=dict(version=VERSION,id='next-phase',policy=dict(maxConcurrency=4,buffers=dict(connection='spill',residentBytes=8192,workingBytes=4194304,spillBytes=200000000,maxBlocks=10000)),connections=connections,
        caches=[dict(id='ref',sourceConnection='reference',schema=ref,keys=[0,1],duplicates=duplicates,errorPolicy='redirect' if duplicates=='error' else 'fail',scope='execution')],
        controlFlow=dict(tasks=[dict(id='build',kind='dataFlow',config=dict(pipeline='build')),dict(id='flow',kind='dataFlow',config=dict(pipeline='flow'))],constraints=[{'from':'build','to':'flow','on':'success'}]),
        dataFlows=[dict(id='build',components=build,paths=build_paths),dict(id='flow',components=nodes,paths=paths)])
    (root/'package.json').write_text(json.dumps(doc));return doc,normal,errors

def run(root,resume=False,kill=0,after=1,component='',expected=0,workers=4,deny='',memory=67108864):
    result=subprocess.run([DRIVER,str(root/'package.json'),str(root/'state'),str(int(resume)),str(workers),str(kill),str(after),component,str(memory),str(deny)],capture_output=True,text=True,timeout=180)
    if kill and after>0: assert result.returncode==91,(result.stdout,result.stderr);return
    assert result.returncode==0,(result.stdout,result.stderr)
    facts=json.loads(result.stdout)
    assert facts['result']==expected,(root.name,resume,expected,facts['result'],result.stderr)
    return facts

def verify(root,normal,errors,facts):
    with closing(sqlite3.connect(root/'target.db')) as db, db:
        actual=db.execute('SELECT * FROM output ORDER BY water,id').fetchall();assert actual==normal,(len(actual),len(normal))
        actual_errors=dict(db.execute('SELECT ruleId,count(*) FROM errors GROUP BY ruleId'));assert actual_errors==dict(errors),(actual_errors,errors)
        assert db.execute('SELECT count(DISTINCT sourceRowId) FROM errors').fetchone()[0] < sum(errors.values())
        assert db.execute('SELECT count(*) FROM errors WHERE length(sourceRowId)!=32 OR policy!=\'redirect\'').fetchone()[0]==0
    files=[json.loads(line) for path in ('cast.ndjson','quality.ndjson','miss.ndjson') for line in (root/path).read_text().splitlines()]
    assert collections.Counter(row['ruleId'] for row in files)==errors
    assert all('input' not in row for row in files)
    assert facts['committedErrors']==2*sum(errors.values()) and facts['acknowledgedErrors']==facts['committedErrors'],facts
    assert facts['diagnostics']['buffers']['encodes']>0 and facts['diagnostics']['buffers']['decodes']>0
    flow=[p for p in facts['diagnostics']['pipelines'] if p['id']=='flow'][-1]
    source=next(n for tree in flow['trees'] for n in tree['nodes'] if n['id']=='facts')
    assert source['facts']['cursor']['lastRead']==10003 and source['facts']['cursor']['lastConfirmed']==10003,source
    assert all(n['readyMs'] and n['endMs'] for tree in flow['trees'] for n in tree['nodes'])
    assert not list((root/'spill').glob('*.udw'))

if __name__=="__main__":
 with tempfile.TemporaryDirectory(prefix='ud-next-phase-') as temp:
     base=pathlib.Path(temp)
     # Complete and crash-boundary cases use separate processes and fresh databases.
     for name,kill,after,kill_component in [('complete',0,0,''),('snapshot',3,10,'facts'),('normalCommit',4,3,'normalSQL'),('errorCommit',4,2,'qualitySQL')]:
         root=base/name;root.mkdir();doc,normal,errors=package(root)
         if kill:
             run(root,kill=kill,after=after,component=kill_component)
             with closing(sqlite3.connect(root/'target.db')) as db, db:
                 if name=='snapshot':assert db.execute('SELECT count(*) FROM output').fetchone()[0]==0
                 else:assert db.execute('SELECT count(*) FROM ud_sdk_receipts_v11').fetchone()[0]>0
         facts=run(root,resume=bool(kill));verify(root,normal,errors,facts)
         again=run(root,resume=True)
         with closing(sqlite3.connect(root/'target.db')) as db, db:assert db.execute('SELECT count(*) FROM output').fetchone()[0]==len(normal)
         assert again['acknowledgedErrors']==facts['acknowledgedErrors']
     # Cached data remains subject to its actual source grant after a target commit.
     root=base/'revoked';root.mkdir();doc,normal,errors=package(root)
     facts=run(root,kill=4,after=0,component='normalSQL',deny=root/'reference.ndjson',expected=13)
     assert facts['committed']>0
     assert not list((root/'spill').glob('*.udw'))
     # Persistent Wire payload corruption cannot be recovered using a live re-read.
     root=base/'corruptBlob';root.mkdir();doc,normal,errors=package(root)
     run(root,kill=4,after=1,component='normalSQL')
     blobs=sorted((root/'state').glob('blob-*.udb'));assert blobs
     for blob in blobs:
         body=bytearray(blob.read_bytes());body[-1]^=1;blob.write_bytes(body)
     run(root,resume=True,expected=9)
     # Independent partition positions, different rates, one interrupted target.
     root=base/'partitions';root.mkdir();doc,_,_=package(root,count=1003)
     with closing(sqlite3.connect(root/'target.db')) as db, db:
         db.execute('CREATE TABLE partitioned(water INTEGER NOT NULL,id INTEGER NOT NULL,tenant TEXT NOT NULL,fk INTEGER,amount TEXT,PRIMARY KEY(water,id))')
     original=copy.deepcopy(doc['dataFlows'][1]['components'][0]['config'])
     nodes=[];paths=[]
     for part,batch in [(0,7),(1,31)]:
         config=dict(original,partitionCount=2,partitionId=part,batchSize=batch)
         nodes.extend([component('source'+str(part),'sql.source',**config),component('target'+str(part),'sql.destination',connection='target',table='partitioned',mode='upsert',keys=[0,1],provisionReceipts=True)])
         paths.append(edge('source'+str(part),'target'+str(part)))
     doc.pop('caches');doc['controlFlow']=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='partitioned'))],constraints=[])
     doc['dataFlows']=[dict(id='partitioned',components=nodes,paths=paths)]
     (root/'package.json').write_text(json.dumps(doc))
     run(root,kill=4,after=2,component='target1',workers=1)
     facts=run(root,resume=True,workers=1)
     with closing(sqlite3.connect(root/'target.db')) as db, db:
         assert db.execute('SELECT count(*),count(DISTINCT id),min(id),max(id) FROM partitioned').fetchone()==(1003,1003,1,1003)
     flow=facts['diagnostics']['pipelines'][0]
     cursors=[n['facts']['cursor'] for tree in flow['trees'] for n in tree['nodes'] if n['id'].startswith('source')]
     assert sum(c['lastRead'] for c in cursors)==1003
     assert all(c['lastConfirmed']==c['lastRead'] and not c['commitUnknown'] for c in cursors)
     # Actual metadata drift rejects before target files/receipts are created.
     root=base/'drift';root.mkdir();doc,normal,errors=package(root)
     with closing(sqlite3.connect(root/'source.db')) as db, db:db.execute('ALTER TABLE src ADD COLUMN extra TEXT')
     facts=run(root,expected=10)
     assert not (root/'normal.ndjson').exists()
     with closing(sqlite3.connect(root/'target.db')) as db, db:assert db.execute('SELECT count(*) FROM output').fetchone()[0]==0
     # Explicit duplicate quarantine uses the normal destination and preserves First.
     root=base/'duplicates';root.mkdir();doc,normal,errors=package(root,duplicates='error');facts=run(root)
     assert len((root/'duplicate.ndjson').read_text().splitlines())==1
     # File mutation, definition changes, and NULL cursor declarations are rejected.
     root=base/'mutated';root.mkdir();doc,normal,errors=package(root);run(root,kill=4,after=1,component='normalSQL')
     (root/'reference.ndjson').write_text((root/'reference.ndjson').read_text()+'{"ref_tenant":"A","ref_key":888,"label":"changed"}\n')
     facts=run(root,resume=True,expected=9)
     root=base/'null';root.mkdir();doc,normal,errors=package(root);doc['dataFlows'][1]['components'][0]['config']['schema']['fields'][0]['nullable']=True;(root/'package.json').write_text(json.dumps(doc));run(root,expected=5)
 print('R1–R5 snapshot, multi-rule FK, shared cache, dual SQL/File destinations, partitioned separate-process crash replay, grant revocation, Wire corruption and schema guards passed')

import pathlib,tempfile,subprocess,json
bin=pathlib.Path('/workspace/.universaldata-tools/postgresql-server/bin');build=pathlib.Path('/workspace/UniversalData/build/next-phase-linux')
def command(name,*args):return subprocess.run([str(bin/name),*map(str,args)],check=True,capture_output=True,text=True,timeout=30).stdout.strip()
with tempfile.TemporaryDirectory(prefix='ud-pg-next-') as folder:
 root=pathlib.Path(folder);(root/'socket').mkdir();(root/'state').mkdir();(root/'spill').mkdir();started=False
 try:
  command('initdb','-D',root/'db','-A','trust','-U','fixture','--no-locale','-E','UTF8');command('pg_ctl','-D',root/'db','-l',root/'server.log','-o',f"-F -k {root/'socket'} -h ''",'-w','start');started=True
  conn=f"host={root/'socket'} user=fixture dbname=postgres"
  def sql(text):return command('psql','-h',root/'socket','-U','fixture','-d','postgres','-At','-c',text)
  sql("CREATE TABLE src(water timestamptz NOT NULL,id bigint NOT NULL,amount numeric(38,2) NOT NULL,flag boolean NOT NULL,raw bytea NOT NULL,name text NOT NULL,PRIMARY KEY(water,id));CREATE TABLE dst(LIKE src INCLUDING ALL);INSERT INTO src SELECT '2024-01-01T00:00:00Z',n,n::numeric/100,n%2=0,decode('00ff','hex'),'中文' FROM generate_series(1,1003) n")
  fields=[dict(name='water',type='timestamp',timezone='UTC',nullable=False,keyOrder=1),dict(name='id',type='int64',nullable=False,keyOrder=2),dict(name='amount',type='decimal128',precision=38,scale=2,nullable=False),dict(name='flag',type='bool',nullable=False),dict(name='raw',type='binary',nullable=False),dict(name='name',type='utf8',nullable=False)]
  package=dict(version=7,id='postgresql-next-phase',connections=[dict(id='db',driver=str(build/'plugins/ud_connector_postgresql.so'),resource=conn,credentialReference='fixture',maxLeases=1),dict(id='spill',driver='file',resource=str(root/'spill'))],policy=dict(buffers=dict(connection='spill',residentBytes=8192,workingBytes=4194304,spillBytes=200000000,maxBlocks=10000)),controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='flow'))],constraints=[]),dataFlows=[dict(id='flow',components=[dict(id='source',component='sql.source',config=dict(connection='db',table='src',sql='SELECT * FROM src ORDER BY id DESC',schema=dict(version=7,fields=fields),batchSize=127,incremental=dict(mode='composite',keys=['water','id'],after=['2024-01-01T00:00:00Z',0],unique=True,monotonic=True),schemaPolicy=dict(policy='strict'))),dict(id='target',component='sql.destination',config=dict(connection='db',table='dst',mode='upsert',keys=[0,1]))],paths=[dict(**{'from':'source','to':'target'},output=0,input=0)])])
  path=root/'package.json';path.write_text(json.dumps(package));driver=build/'tests/test_next_phase_execution'
  def run(resume,kill=False):
   result=subprocess.run([str(driver),str(path),str(root/'state'),str(int(resume)),'4','4' if kill else '0','2','target'],capture_output=True,text=True,timeout=30)
   if kill:assert result.returncode==91,(result.stdout,result.stderr);return json.loads(next(line for line in result.stderr.splitlines() if line.startswith('{')))
   facts=json.loads(result.stdout);assert result.returncode==0 and facts['result']==0,facts;return facts
  before=run(False,True);assert sql('SELECT count(*) FROM dst')=='254'
  facts=run(True);assert sql('SELECT count(*) FROM dst')=='1003';assert sql('SELECT count(*) FROM dst d FULL JOIN src s USING(water,id) WHERE d.amount IS DISTINCT FROM s.amount OR d.flag IS DISTINCT FROM s.flag OR d.raw IS DISTINCT FROM s.raw OR d.name IS DISTINCT FROM s.name')=='0'
  source=next(n for t in facts['diagnostics']['pipelines'][0]['trees'] for n in t['nodes'] if n['id']=='source');assert source['facts']['cursor']['lastConfirmed']==1003
  assert facts['diagnostics']['connections']['sessions']<=1 and facts['diagnostics']['connections']['activeLeases']==0
  record=dict(exitCode=0,sourceFingerprint='d7387494a8455cdc45cd661c2987256305c1894b50d31bad2eb60ed0cd511696',rows=1003,confirmedAtCrash=254,types=['Timestamp UTC','Composite Int64 PK','Decimal128(38,2)','Bool','Binary','UTF8'],singlePhysicalSession=True,exactTargetEquality=True,interruption=before,package=package,diagnostics=facts)
  pathlib.Path('/workspace/UniversalData/docs/acceptance/evidence/etl-next-phase-linux/postgresql-vertical.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n');print('PostgreSQL native composite source -> discovery/preflight -> snapshot -> SQL upsert, real process crash/resume and six typed domains passed')
 finally:
  if started:command('pg_ctl','-D',root/'db','-m','immediate','-w','stop')

"""Disposable local PostgreSQL: typed driver, pooled package and real interruption."""
import argparse,copy,json,pathlib,subprocess,tempfile,time
parser=argparse.ArgumentParser();parser.add_argument('--bin',required=True,type=pathlib.Path);parser.add_argument('--build',required=True,type=pathlib.Path)
a=parser.parse_args();binaries=a.bin.resolve();build=a.build.resolve()
version=json.loads((pathlib.Path(__file__).resolve().parents[1]/'metadata/product.json').read_text())['product']['protocolVersion']
def command(name,*args,**kwargs):return subprocess.run([str(binaries/name),*map(str,args)],check=True,capture_output=True,text=True,timeout=30,**kwargs)
with tempfile.TemporaryDirectory(prefix='ud-postgresql-') as directory:
 root=pathlib.Path(directory);data=root/'data';socket=root/'socket';socket.mkdir();started=False
 try:
  command('initdb','-D',data,'-A','trust','-U','fixture','--no-locale','-E','UTF8')
  command('pg_ctl','-D',data,'-l',root/'server.txt','-o',f"-F -k {socket} -h '' -c max_prepared_transactions=32",'-w','start');started=True
  config=f'host={socket} user=fixture dbname=postgres';(root/'config.txt').write_text(config)
  driver=build/'plugins/ud_connector_postgresql.so'
  result=subprocess.run([str(build/'tests/test_postgresql_values'),str(driver),str(root/'config.txt')],check=True,capture_output=True,text=True,timeout=30);print(result.stdout.strip(),flush=True)
  def sql(statement):return command('psql','-h',socket,'-U','fixture','-d','postgres','-At','-c',statement).stdout.strip()
  sql('CREATE TABLE data(id BIGINT PRIMARY KEY)')
  base={'version':version,'id':'postgresql-four-engines','policy':{'maxConcurrency':1},'variables':{'total':{'type':'int64','default':0},'statement':{'type':'utf8','default':''}},
   'connections':[{'id':'db','driver':str(driver),'resource':'db','credentialReference':'fixture','maxLeases':1}], 'controlFlow':{'tasks':[],'constraints':[]},'dataFlows':[]}
  runner=build/'tests/test_four_engine_connections'
  def run(package,status=0,total=0):
   path=root/'package.json';path.write_text(json.dumps(package))
   result=subprocess.run([str(runner),str(path),config,str(status),str(total),str(pathlib.Path(tempfile.mkdtemp(prefix='state-',dir=root)))],capture_output=True,text=True,timeout=20)
   assert result.returncode==0,result.stdout+result.stderr
   return json.loads(result.stdout)
  def task(name,statement):return {'id':name,'kind':'sql.execute','config':{'connection':'db','statement':statement}}
  children={'tasks':[task('first','INSERT INTO data VALUES(1)'),task('second','INSERT INTO data VALUES(2)')],'constraints':[{'from':'first','to':'second','on':'success'}]}
  transaction={'id':'transaction','kind':'sequence','config':{},'transaction':{'mode':'required','participants':['db'],'provisionReceipts':True},'children':children}
  q=copy.deepcopy(base);q['controlFlow']['tasks']=[transaction];assert run(q)['connections']['commits']==1;assert sql('SELECT COUNT(*) FROM data')=='2'
  sql('DELETE FROM data');q=copy.deepcopy(base);q['variables']['huge']={'type':'int64','default':9223372036854775807};q['controlFlow']['tasks']=[copy.deepcopy(transaction)]
  q['controlFlow']['tasks'][0]['children']['tasks'].append({'id':'bad','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'huge'},{'literal':1,'type':'int64'}]}}})
  q['controlFlow']['tasks'][0]['children']['constraints'].append({'from':'second','to':'bad','on':'success'})
  assert run(q,7)['connections']['rollbacks']==1;assert sql('SELECT COUNT(*) FROM data')=='0'
  q=copy.deepcopy(base);q['controlFlow']['tasks']=[{'id':'pool','kind':'foreach','config':{'variable':'statement','items':["SET TIME ZONE 'HST'","INSERT INTO data SELECT 1 WHERE current_setting('TimeZone')='UTC'",'INSERT INTO data VALUES(2)']},'children':{'tasks':[task('execute',{'$variable':'statement'})],'constraints':[]}}]
  result=run(q);assert result['connections']['poolHits']>=2 and sql('SELECT COUNT(*) FROM data')=='2'
  q=copy.deepcopy(base);q['variables']['id']={'type':'int64','default':0};add={'id':'sum','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'total'},{'ref':'id'}]}}}
  q['controlFlow']['tasks']=[{'id':'query','kind':'foreach','config':{'variable':'id','enumerator':'core.query','connection':'db','query':'SELECT id FROM data ORDER BY id'},'children':{'tasks':[add],'constraints':[]}}];run(q,total=3)
  q=copy.deepcopy(base);q['variables']['name']={'type':'utf8','default':''};counter={'id':'count','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'total'},{'literal':1,'type':'int64'}]}}}
  q['controlFlow']['tasks']=[{'id':'schema','kind':'foreach','config':{'variable':'name','enumerator':'core.schema','connection':'db','table':'data'},'children':{'tasks':[counter],'constraints':[]}}];run(q,total=1)
  q=copy.deepcopy(base);q['variables']['id']={'type':'int64','default':0};q['controlFlow']['tasks']=[{'id':'timeout','kind':'foreach','timeoutMs':50,'config':{'variable':'id','enumerator':'core.query','connection':'db','query':'SELECT 1::bigint AS id FROM pg_sleep(5)'},'children':{'tasks':[],'constraints':[]}}]
  start=time.monotonic();result=run(q,18);assert time.monotonic()-start<1 and result['connections']['activeLeases']==0
  assert sql("SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event='PgSleep'")=='0'
  q=copy.deepcopy(base);q['controlFlow']['tasks']=[task('disconnect','INSERT INTO data SELECT 999 FROM pg_sleep(10)')];path=root/'disconnect.json';path.write_text(json.dumps(q))
  child=subprocess.Popen([str(runner),str(path),config,'16','0'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   for _ in range(100):
    pid=sql("SELECT pid FROM pg_stat_activity WHERE application_name LIKE 'UniversalDataSDK/%' AND query LIKE 'INSERT INTO data SELECT 999%'")
    if pid:break
    time.sleep(.01)
   assert pid,'Driver did not reach the controlled in-flight write'
   sql(f'SELECT pg_terminate_backend({int(pid)})')
   stdout,stderr=child.communicate(timeout=5);assert child.returncode==0,stdout+stderr
   result=json.loads(stdout);assert result['connections']['activeLeases']==0 and result['outcome']==16
  finally:
   if child.poll() is None:child.kill();child.wait()
  print('PostgreSQL package commit/rollback, clean-session pool reuse, Query/Schema enumeration, deadline server cancellation and disconnected write CommitUnknown passed',flush=True)
 finally:
  if started:command('pg_ctl','-D',data,'-m','immediate','-w','stop')

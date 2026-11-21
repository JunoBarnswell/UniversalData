"""Temporary SQLite/files: pooled leases, native transactions and enumerators."""
import copy,json,pathlib,sqlite3,subprocess,sys,tempfile
runner,driver=map(lambda x:str(pathlib.Path(x).resolve()),sys.argv[1:3])
version=json.loads((pathlib.Path(__file__).resolve().parents[1]/'metadata/product.json').read_text())['product']['protocolVersion']
with tempfile.TemporaryDirectory(prefix='ud-four-connections-') as directory:
 r=pathlib.Path(directory);db=r/'data.db';connection=sqlite3.connect(db);connection.execute('CREATE TABLE data(id INTEGER PRIMARY KEY)');connection.commit()
 p={'version':version,'id':'connections','policy':{'maxConcurrency':1},'variables':{'total':{'type':'int64','default':0},'statement':{'type':'utf8','default':''}},
    'connections':[{'id':'db','driver':driver,'resource':'db','credentialReference':'fixture','maxLeases':1}],
    'controlFlow':{'tasks':[],'constraints':[]},'dataFlows':[]}
 insert=lambda i:{'id':f'insert{i}','kind':'sql.execute','config':{'connection':'db','statement':f'INSERT INTO data VALUES({i})'}}
 def run(package,status=0,total=0,state=None):
  path=r/'package.json';path.write_text(json.dumps(package));result=subprocess.run([runner,str(path),str(db),str(status),str(total),*([str(state)] if state else [])],capture_output=True,text=True,timeout=30)
  assert result.returncode==0,result.stdout+result.stderr
  return json.loads(result.stdout) if result.stdout else {}
 children={'tasks':[insert(1),insert(2)],'constraints':[{'from':'insert1','to':'insert2','on':'success'}]}
 txn={'id':'transaction','kind':'sequence','transaction':{'mode':'required','participants':['db']},'config':{},'children':children}
 q=copy.deepcopy(p);q['controlFlow']['tasks']=[txn]
 result=run(q,0,state=r/'local-transaction')
 assert connection.execute('SELECT id FROM data ORDER BY id').fetchall()==[(1,),(2,)]
 connection.execute('DELETE FROM data');connection.commit()
 q=copy.deepcopy(p);q['variables']['huge']={'type':'int64','default':9223372036854775807}
 failed=copy.deepcopy(txn)
 failed['children']['tasks'].append({'id':'overflow','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'huge'},{'literal':1,'type':'int64'}]}}})
 failed['children']['constraints'].append({'from':'insert2','to':'overflow','on':'success'})
 q['controlFlow']['tasks']=[failed]
 run(q,7,state=r/'local-transaction-rollback')
 assert connection.execute('SELECT COUNT(*) FROM data').fetchone()[0]==0
 q=copy.deepcopy(p);q['controlFlow']['tasks']=[{'id':'items','kind':'foreach','config':{'variable':'statement','items':['INSERT INTO data VALUES(1)','INSERT INTO data VALUES(2)']},
   'children':{'tasks':[{'id':'execute','kind':'sql.execute','config':{'connection':'db','statement':{'$variable':'statement'}}}],'constraints':[]}}]
 result=run(q);assert result['connections']['poolHits']>=1 and result['connections']['activeLeases']==0
 q=copy.deepcopy(p);q['variables']['item']={'type':'int64','default':0};q['variables']['items']={'type':'list','children':[{'name':'item','type':'int64','nullable':False}],'default':[1,2,3]}
 add={'id':'sum','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'total'},{'ref':'item'}]}}}
 q['controlFlow']['tasks']=[{'id':'variable','kind':'foreach','config':{'variable':'item','enumerator':'core.variable','fromVariable':'items'},'children':{'tasks':[add],'constraints':[]}}];run(q,total=6)
 # Replacing the source list must not change or invalidate the enumerator's snapshot.
 q['variables']['replacement']=copy.deepcopy(q['variables']['items']);q['variables']['replacement']['default']=[40,50]
 replace={'id':'replace','kind':'set','config':{'name':'items','value':{'ref':'replacement'}}}
 q['controlFlow']['tasks'][0]['children']={'tasks':[replace,copy.deepcopy(add)],'constraints':[{'from':'replace','to':'sum','on':'success'}]}
 run(q,total=6)
 # Exercise owned text/nested values, not only the list's child array.
 q['variables']['items']={'type':'list','children':[{'name':'item','type':'utf8','nullable':False}],'default':['INSERT INTO data VALUES(10)','INSERT INTO data VALUES(11)','INSERT INTO data VALUES(12)']}
 q['variables']['replacement']=copy.deepcopy(q['variables']['items']);q['variables']['replacement']['default']=['SELECT 1']
 q['variables']['item']={'type':'utf8','default':''}
 q['controlFlow']['tasks'][0]['children']={'tasks':[replace,{'id':'execute','kind':'sql.execute','config':{'connection':'db','statement':{'$variable':'item'}}}],
   'constraints':[{'from':'replace','to':'execute','on':'success'}]}
 run(q);assert connection.execute('SELECT id FROM data WHERE id>=10 ORDER BY id').fetchall()==[(10,),(11,),(12,)]
 connection.execute('DELETE FROM data WHERE id>=10');connection.commit()
 # A full pool may retire idle sessions for other resources; active leases remain pinned.
 q=copy.deepcopy(p);q['policy']['connections']={'maxSessions':1}
 q['connections'].append(dict(q['connections'][0],id='other',resource='other'))
 q['controlFlow']={'tasks':[{'id':str(i),'kind':'sql.execute','config':{'connection':id,'statement':'UPDATE data SET id=id WHERE 0'}} for i,id in enumerate(['db','other','db','db'])],
   'constraints':[{'from':str(i),'to':str(i+1),'on':'success'} for i in range(3)]}
 result=run(q);assert result['connections']['sessions']==1 and result['connections']['activeLeases']==0 and result['connections']['poolHits']==1
 q['variables']['id']={'type':'int64','default':0}
 q['controlFlow']={'tasks':[{'id':'pinned','kind':'foreach','config':{'variable':'id','enumerator':'core.query','connection':'db','query':'SELECT id FROM data ORDER BY id'},
   'children':{'tasks':[{'id':'other','kind':'sql.execute','config':{'connection':'other','statement':'UPDATE data SET id=id WHERE 0'}}],'constraints':[]}}],'constraints':[]}
 result=run(q,15);assert result['connections']['activeLeases']==0 and result['connections']['sessions']==1
 q=copy.deepcopy(p);q['variables']['id']={'type':'int64','default':0};add['config']['value']['args'][1]={'ref':'id'}
 q['controlFlow']['tasks']=[{'id':'query','kind':'foreach','config':{'variable':'id','enumerator':'core.query','connection':'db','query':'SELECT id FROM data ORDER BY id'},'children':{'tasks':[add],'constraints':[]}}];run(q,total=3)
 q=copy.deepcopy(p);q['variables']['name']={'type':'utf8','default':''};counter={'id':'count','kind':'set','config':{'name':'total','value':{'op':'add','args':[{'ref':'total'},{'literal':1,'type':'int64'}]}}}
 q['controlFlow']['tasks']=[{'id':'schema','kind':'foreach','config':{'variable':'name','enumerator':'core.schema','connection':'db','table':'data'},'children':{'tasks':[counter],'constraints':[]}}];run(q,total=1)
 (r/'a.csv').write_text('x\n1\n');(r/'b.csv').write_text('x\n2\n');q=copy.deepcopy(p);q['variables']['name']={'type':'utf8','default':''};q['connections'].append({'id':'files','driver':'file','resource':str(r)})
 q['controlFlow']['tasks']=[{'id':'files','kind':'foreach','config':{'variable':'name','enumerator':'core.files','connection':'files','suffix':'.csv'},'children':{'tasks':[counter],'constraints':[]}}];run(q,total=2)
 q=copy.deepcopy(p);q['controlFlow']['tasks']=[{'id':'timeout','kind':'sql.execute','timeoutMs':10,'config':{'connection':'db','statement':'WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT SUM(x) FROM n'}}]
 result=run(q,18);assert result['connections']['activeLeases']==0 and result['connections']['sessions']==0
 q=copy.deepcopy(p);q['controlFlow']['tasks']=[{'id':'session','kind':'sql.execute','config':{'connection':'db','statement':'PRAGMA foreign_keys=OFF'}}]
 run(q,13)
 connection.close()
print('Native pooled session reuse, local SQLite transaction, Collection/Variable/Query/Schema/File enumerators passed')

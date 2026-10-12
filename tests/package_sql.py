import json,pathlib,sqlite3,subprocess,sys,tempfile
with tempfile.TemporaryDirectory(prefix='ud-package-sql-') as folder:
 r=pathlib.Path(folder);source=r/'source.db';target=r/'target.db'
 lit=lambda v:{'literal':v,'type':'int64'}
 tasks=[{'id':'createSource','kind':'sql.execute','config':{'connection':'sourceDB','statement':'CREATE TABLE src(x INTEGER)'}},
 {'id':'populate','kind':'sql.execute','config':{'connection':'sourceDB','statement':'INSERT INTO src VALUES '+','.join('('+str(i)+')' for i in range(31))}},
 {'id':'createTarget','kind':'sql.execute','config':{'connection':'targetDB','statement':'CREATE TABLE dest(x INTEGER,twice INTEGER)'}},
 {'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}]
 package={'version':5,'id':'sqlFixture','connections':[{'id':'sourceDB','driver':str(pathlib.Path(sys.argv[2]).resolve()),'resource':'source-db','credentialReference':'source'},
 {'id':'targetDB','driver':str(pathlib.Path(sys.argv[2]).resolve()),'resource':'target-db','credentialReference':'target'},
 {'id':'rejected','driver':'file','resource':str(r/'rejected.ndjson')}],
 'controlFlow':{'tasks':tasks,'constraints':[{'from':a['id'],'to':b['id'],'on':'success'} for a,b in zip(tasks,tasks[1:])]},
 'dataFlows':[{'id':'p','components':[{'id':'source','component':'sql.source','config':{'connection':'sourceDB','schema':{'version':5,'fields':[{'name':'x','type':'int64','nullable':True}]},'batchSize':11,'sql':'SELECT x FROM src ORDER BY x'}},
 {'id':'derive','component':'derive','config':{'columns':[{'name':'twice','expression':{'op':'mul','args':[{'column':'x'},lit(2)]}}]}},
 {'id':'split','component':'split','config':{'condition':{'op':'lt','args':[{'column':'x'},lit(20)]}}},
 {'id':'target','component':'sql.destination','config':{'connection':'targetDB','table':'dest','mode':'append','keys':[]}},
 {'id':'reject','component':'file.ndjson.destination','config':{'connection':'rejected'}}],
 'paths':[{'from':a,'output':port,'to':b,'input':0} for a,port,b in [('source',0,'derive'),('derive',0,'split'),('split',0,'target'),('split',1,'reject')]]}]}
 p=r/'package.json';p.write_text(json.dumps(package));subprocess.run([sys.argv[1],str(p),str(source),str(target)],check=True)
 c=sqlite3.connect(target);assert c.execute('SELECT x,twice FROM dest ORDER BY x').fetchall()==[(i,i*2) for i in range(20)]
 assert c.execute('SELECT count(*) FROM ud_sdk_receipts_v2').fetchone()[0]>=2;c.close()
 rows=[json.loads(line) for line in (r/'rejected.ndjson').read_text().splitlines()];assert rows==[{'x':i,'twice':i*2} for i in range(20,31)]
 print('Package: SQL task/source share Connection Manager, bounded SQL cursor, derive/split, transaction receipts and file error path passed')

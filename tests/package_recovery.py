"""Real process termination across task, target commit and journal boundaries."""
import copy,json,pathlib,sqlite3,subprocess,sys,tempfile
runner,plugin=map(lambda p:str(pathlib.Path(p).resolve()),sys.argv[1:])
with tempfile.TemporaryDirectory(prefix='ud-package-recovery-') as folder:
 root=pathlib.Path(folder)
 def fixture(name):
  r=root/name;r.mkdir();source=r/'source.ndjson';source.write_text(''.join(json.dumps({'x':i})+'\n' for i in range(31)))
  database=r/'target.db';c=sqlite3.connect(database);c.execute('CREATE TABLE dest(x INTEGER UNIQUE)');c.commit();c.close()
  lit=lambda v:{'literal':v,'type':'int64'}
  p={'version':5,'id':'recovery','variables':{'i':{'type':'int64','default':0},'n':{'type':'int64','default':0}},
   'connections':[{'id':'source','driver':'file','resource':str(source)},{'id':'target','driver':plugin,'resource':'target-db','credentialReference':'target'},
                  {'id':'file','driver':'file','resource':str(r/'out.ndjson')}],
   'controlFlow':{'tasks':[{'id':'loop','kind':'for','config':{'initialize':{'name':'i','value':lit(0)},'condition':{'op':'lt','args':[{'ref':'i'},lit(4)]},'increment':{'name':'i','value':{'op':'add','args':[{'ref':'i'},lit(1)]}},'maxIterations':4},
               'children':{'tasks':[{'id':'increment','kind':'set','config':{'name':'n','value':{'op':'add','args':[{'ref':'n'},lit(1)]}}}],'constraints':[]}},
               {'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}], 'constraints':[{'from':'loop','to':'flow','on':'success'}]},
   'dataFlows':[{'id':'p','components':[{'id':'source','component':'file.ndjson.source','config':{'connection':'source','schema':{'version':5,'fields':[{'name':'x','type':'int64','nullable':False}]},'batchSize':7}},
              {'id':'multicast','component':'multicast','config':{}},{'id':'db','component':'sql.destination','config':{'connection':'target','table':'dest','mode':'append','keys':[]}},
              {'id':'file','component':'file.ndjson.destination','config':{'connection':'file'}}],
              'paths':[{'from':a,'output':port,'to':b,'input':0} for a,port,b in [('source',0,'multicast'),('multicast',0,'db'),('multicast',1,'file')]]}]}
  path=r/'package.json';path.write_text(json.dumps(p));return r,p
 def args(r,resume=0,mode=0,status=0):return [runner,str(r/'package.json'),str(r/'target.db'),str(r/'out.ndjson'),str(r/'effect.txt'),str(r/'state'),str(resume),str(mode),str(status)]
 def run(r,resume=0,mode=0,status=0,exit=0):
  result=subprocess.run(args(r,resume,mode,status),capture_output=True,text=True,timeout=30)
  assert result.returncode==exit,(r.name,result.returncode,result.stdout,result.stderr)
 def rows(r):
  c=sqlite3.connect(r/'target.db');values=c.execute('SELECT x FROM dest ORDER BY x').fetchall();c.close();return [v[0] for v in values]
 def verify(r):
  assert rows(r)==list(range(31));assert [json.loads(x) for x in (r/'out.ndjson').read_text().splitlines()]==[{'x':i} for i in range(31)]
  before=len(list((r/'state').glob('*.udg')));run(r,resume=1);assert len(list((r/'state').glob('*.udg')))==before
 for name,mode in [('loop',1),('db-commit-before-journal',2),('file-publish-before-journal',3)]:
  r,p=fixture(name);run(r,mode=mode,exit=91)
  if mode==2: assert len(rows(r))==7 and not (r/'out.ndjson').exists()
  run(r,resume=1);verify(r)
 r,p=fixture('changed-source');run(r,mode=2,exit=91);(r/'source.ndjson').write_text('{"x":999}\n');run(r,resume=1,status=9);assert len(rows(r))==7
 r,p=fixture('changed-package');run(r,mode=2,exit=91);p['id']='changed';(r/'package.json').write_text(json.dumps(p));run(r,resume=1,status=10);assert len(rows(r))==7
 r,p=fixture('corrupt');run(r);record=sorted((r/'state').glob('*.udg'))[-1];b=bytearray(record.read_bytes());b[-1]^=1;record.write_bytes(b);run(r,resume=1,status=9)
 r,p=fixture('gap');run(r);sorted((r/'state').glob('*.udg'))[-1].unlink();run(r,resume=1,status=9)
 r,p=fixture('fresh-on-existing');run(r);run(r,status=15)
 r,p=fixture('lock');child=subprocess.Popen(args(r,mode=5),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 try:
  assert child.stdout.readline().strip()=='paused';run(r,resume=1,status=15);child.communicate('continue\n',timeout=30);assert child.returncode==0;verify(r)
 finally:
  if child.poll() is None: child.kill();child.wait()
 r,p=fixture('unsafe-effect');p['controlFlow']={'tasks':[{'id':'effect','kind':'custom.effect','config':{}}],'constraints':[]};p['dataFlows']=[];(r/'package.json').write_text(json.dumps(p));run(r,mode=4,exit=91);run(r,resume=1,status=16);assert (r/'effect.txt').read_text()=='effect\n'
 print('Recovery: real process kills, loop/variable/task restoration, SQL receipt replay, file publish verification, repeatable resume, source/package change, corruption/gap, writer exclusion and unknown custom effect passed')

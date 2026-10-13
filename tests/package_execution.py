import json,pathlib,subprocess,sys,tempfile
with tempfile.TemporaryDirectory(prefix='ud-package-') as folder:
 r=pathlib.Path(folder);source=r/'source.ndjson';source.write_text(''.join(json.dumps({'x':i})+'\n' for i in range(31)))
 schema={'version':10,'fields':[{'name':'x','type':'int64','nullable':False}]}
 lit=lambda v:{'literal':v,'type':'int64'}
 body={'tasks':[{'id':'increment','kind':'set','config':{'name':'i','value':{'op':'add','args':[{'ref':'i'},lit(1)]}}}], 'constraints':[]}
 flow={'id':'p','components':[{'id':'source','component':'file.ndjson.source','config':{'connection':'input','schema':schema,'batchSize':7}},
 {'id':'derive','component':'derive','config':{'columns':[{'name':'twice','expression':{'op':'mul','args':[{'column':'x'},lit(2)]}}]}},
 {'id':'split','component':'split','config':{'condition':{'op':'lt','args':[{'column':'x'},lit(10)]}}},
 {'id':'yes','component':'file.ndjson.destination','config':{'connection':'yes'}},
 {'id':'no','component':'file.ndjson.destination','config':{'connection':'no'}}],
 'paths':[{'from':a,'output':port,'to':b,'input':0} for a,port,b in [('source',0,'derive'),('derive',0,'split'),('split',0,'yes'),('split',1,'no')]]}
 package={'version':10,'id':'fixture','variables':{'i':{'type':'int64','default':0},'n':{'type':'int64','default':0}},'parameters':{},
 'connections':[{'id':k,'driver':'file','resource':str(v)} for k,v in [('input',source),('yes',r/'yes.ndjson'),('no',r/'no.ndjson')]],
 'controlFlow':{'tasks':[{'id':'sequence','kind':'sequence','config':{},'children':{'tasks':[{'id':'loop','kind':'for','config':{'initialize':{'name':'i','value':lit(0)},'condition':{'op':'lt','args':[{'ref':'i'},lit(3)]},'increment':{'name':'i','value':{'op':'add','args':[{'ref':'i'},lit(1)]}},'maxIterations':3},'children':{'tasks':[{'id':'count','kind':'set','config':{'name':'n','value':{'op':'add','args':[{'ref':'n'},lit(1)]}}}],'constraints':[]}}],'constraints':[]}},
 {'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}], 'constraints':[{'from':'sequence','to':'flow','on':'success','expression':{'op':'eq','args':[{'ref':'i'},lit(3)]}}]},'dataFlows':[flow]}
 path=r/'package.json';path.write_text(json.dumps(package));subprocess.run([sys.argv[1],str(path)],check=True)
 a=[json.loads(x) for x in (r/'yes.ndjson').read_text().splitlines()];b=[json.loads(x) for x in (r/'no.ndjson').read_text().splitlines()]
 assert [x['x'] for x in a]==list(range(10)) and [x['x'] for x in b]==list(range(10,31))
 assert all(x['twice']==x['x']*2 for x in a+b)
 print('Package: nested control loop, expression precedence, native DataFlow, derive/split, bounded multi-output and atomic file destinations passed')

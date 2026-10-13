import copy,json,pathlib,subprocess,sys,tempfile
with tempfile.TemporaryDirectory(prefix='ud-package-failures-') as folder:
 r=pathlib.Path(folder);source=r/'in.ndjson';source.write_text('{"x":1}\n{"x":2}\n')
 base={'version':9,'id':'failures','parameters':{},'variables':{},'connections':[{'id':'in','driver':'file','resource':str(source)},{'id':'out','driver':'file','resource':str(r/'out.ndjson')}],
 'controlFlow':{'tasks':[{'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}],'constraints':[]},
 'dataFlows':[{'id':'p','components':[{'id':'source','component':'file.ndjson.source','config':{'connection':'in','schema':{'version':9,'fields':[{'name':'x','type':'int64','nullable':False}]}}},
 {'id':'sink','component':'file.ndjson.destination','config':{'connection':'out'}}],'paths':[{'from':'source','output':0,'to':'sink','input':0}]}]}
 def run(p,permit,create,result):
  path=r/'package.json';path.write_text(json.dumps(p));subprocess.run([sys.argv[1],str(path),str(permit),str(create),str(result)],check=True)
 run(base,0,0,13);assert not (r/'out.ndjson').exists()
 run(base,2,0,8);assert not (r/'out.ndjson').exists()
 p=copy.deepcopy(base);p['dataFlows'][0]['components'][0]['component']='missing';run(p,1,12,0)
 p=copy.deepcopy(base);p['dataFlows'][0]['paths'][0]['input']=3;run(p,1,5,0)
 p=copy.deepcopy(base);p['version']=2;run(p,1,10,0)
 p=copy.deepcopy(base);p['parameters']={'required':{'type':'int64'}};run(p,1,5,0)
 source.write_text('{"x":1}\n{"x":"bad"}\n');run(base,1,0,6);assert not (r/'out.ndjson').exists()
 # Failure/Completion branches run without turning the original failure into success.
 source.write_text('{"x":1}\n');p=copy.deepcopy(base);p['variables']={'ran':{'type':'bool','default':False}}
 p['controlFlow']['tasks'].append({'id':'onFailure','kind':'set','config':{'name':'ran','value':{'literal':True,'type':'bool'}}})
 p['controlFlow']['constraints']=[{'from':'flow','to':'onFailure','on':'failure'}];run(p,0,0,13)
 print('Package: default-deny, cancellation, missing capability, invalid ports/version/parameters, parse failure and failure constraints passed')

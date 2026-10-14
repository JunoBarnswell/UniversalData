"""C/.NET/Java Package execution and datatype/wire parity for the current ABI.
Args: C package consumer, C data runner, dotnet, package.dll, data.dll, java, classes, jar, native lib dir.
"""
import argparse,json,os,pathlib,subprocess,sys,tempfile
parser=argparse.ArgumentParser()
parser.add_argument('paths',nargs=9)
parser.add_argument('--managed-native-local',action='store_true',help='Do not add the SDK directory to the .NET process loader path.')
arguments=parser.parse_args()
cli,data,dotnet,package_dll,data_dll,java,classes,jar,native=map(lambda x:str(pathlib.Path(x).resolve()),arguments.paths)
managed_env=dict(os.environ)
env=dict(os.environ);env['LD_LIBRARY_PATH']=native+os.pathsep+env.get('LD_LIBRARY_PATH','');env['PATH']=native+os.pathsep+env.get('PATH','')
with tempfile.TemporaryDirectory(prefix='ud-abi8-language-') as folder:
 r=pathlib.Path(folder);source=r/'in.ndjson';source.write_text(''.join(json.dumps({'x':i})+'\n' for i in range(10003)))
 lit=lambda v:{'literal':v,'type':'int64'}
 p={'version':11,'id':'parity','parameters':{'a':{'type':'utf8'},'b':{'type':'utf8'}},'variables':{'i':{'type':'int64','default':0},'n':{'type':'int64','default':0}},
 'project':{'id':'project','parameters':{'factor':{'type':'int64','default':2},'input':{'type':'utf8','default':str(source)}},'variables':{'n':{'type':'int64','default':99}},'connections':[{'id':'source','driver':'file','resource':{'$parameter':'input'}}]},
 'connections':[{'id':'a','driver':'file','resource':{'$parameter':'a'}},{'id':'b','driver':'file','resource':{'$parameter':'b'}}],
 'controlFlow':{'tasks':[{'id':'loop','kind':'for','config':{'initialize':{'name':'i','value':lit(0)},'condition':{'op':'lt','args':[{'ref':'i'},lit(3)]},'increment':{'name':'i','value':{'op':'add','args':[{'ref':'i'},lit(1)]}},'maxIterations':3},
 'children':{'tasks':[{'id':'count','kind':'set','config':{'name':'n','value':{'op':'add','args':[{'ref':'n'},lit(1)]}}}],'constraints':[]}},
 {'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}],'constraints':[{'from':'loop','to':'flow','on':'success','expression':{'op':'eq','args':[{'ref':'n'},lit(3)]}}]},
 'dataFlows':[{'id':'p','components':[{'id':'source','component':'file.ndjson.source','config':{'connection':'source','schema':{'version':11,'fields':[{'name':'x','type':'int64','nullable':False}]},'batchSize':19}},
 {'id':'derive','component':'derive','config':{'columns':[{'name':'twice','expression':{'op':'mul','args':[{'column':'x'},{'ref':'factor'}]}}]}},
 {'id':'split','component':'split','config':{'condition':{'op':'lt','args':[{'column':'x'},lit(5000)]}}},
 {'id':'a','component':'file.ndjson.destination','config':{'connection':'a'}},{'id':'b','component':'file.ndjson.destination','config':{'connection':'b'}}],
 'paths':[{'from':a,'output':port,'to':b,'input':0} for a,port,b in [('source',0,'derive'),('derive',0,'split'),('split',0,'a'),('split',1,'b')]]}]}
 child={'version':11,'id':'parity-child','parameters':p['parameters'],'project':p['project'],'connections':p['connections'],
 'controlFlow':{'tasks':[{'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}],'constraints':[]},'dataFlows':p['dataFlows']}
 p['controlFlow']['tasks'][-1]={'id':'flow','kind':'executePackage','config':{'package':child,'parameters':{'a':{'ref':'a'},'b':{'ref':'b'}}}}
 p['dataFlows']=[]
 p['parameters']=dict(p['parameters'],working={'type':'utf8'})
 p['connections']=[{'id':'original','driver':'file','resource':str(source)},{'id':'working','driver':'file','resource':{'$parameter':'working'}}]
 p['controlFlow']['tasks'].insert(0,{'id':'copy','kind':'file.system','config':{'operation':'copy','source':'original','target':'working'}})
 p['variables'].update({name:{'type':'int64','default':0} for name in ['profileRows','profileNulls','profileMin','profileMax']})
 p['dataFlows']=[{'id':'profile-flow','components':[{'id':'source','component':'file.ndjson.source','config':{'connection':'original','schema':{'version':11,'fields':[{'name':'x','type':'int64','nullable':False}]},'batchSize':17}},
 {'id':'statistics','component':'profile.statistics','config':{'statistics':[{'kind':'rows','variable':'profileRows'},{'kind':'nulls','column':0,'variable':'profileNulls'},{'kind':'min','column':0,'variable':'profileMin'},{'kind':'max','column':0,'variable':'profileMax'}]}}],
 'paths':[{'from':'source','output':0,'to':'statistics','input':0}]}]
 p['controlFlow']['tasks'].insert(1,{'id':'profile','kind':'data.profile','config':{'pipeline':'profile-flow','checks':[{'id':name,'expression':{'op':'eq','args':[{'ref':name},lit(value)]}} for name,value in [('profileRows',10003),('profileNulls',0),('profileMin',0),('profileMax',10002)]]}})
 p['controlFlow']['constraints'].extend([{'from':'copy','to':'profile','on':'success'},{'from':'profile','to':'loop','on':'success'}])
 definition=r/'package.json';definition.write_text(json.dumps(p));metrics=[]
 for index in range(3):
  params={'a':str(r/f'{index}-a.ndjson'),'b':str(r/f'{index}-b.ndjson'),'working':str(r/f'{index}-source.ndjson')};values=r/f'{index}.params.json';values.write_text(json.dumps(params))
  if index==0: command=[cli,str(definition),json.dumps(params),'fixture','same-id',str(r)]
  elif index==1: command=[dotnet,package_dll,str(definition),str(values),'same-id',str(r)]
  else: command=[java,'-Djava.library.path='+native,'-cp',classes+os.pathsep+jar,'Consumer',str(definition),str(values),'same-id',str(r)]
  result=subprocess.run(command,env=managed_env if index==1 and arguments.managed_native_local else env,capture_output=True,text=True,check=True,timeout=120);m=json.loads(result.stdout);assert m['state']==5 and m['result']==0 and m['inputRows']==20006 and m['outputRows']==20006 and m['executedTasks']==8;metrics.append(m)
  assert (r/f'{index}-source.ndjson').read_bytes()==source.read_bytes()
  for port in ['a','b']:
   actual=(r/f'{index}-{port}.ndjson').read_bytes()
   if index: assert actual==(r/f'0-{port}.ndjson').read_bytes()
   rows=[json.loads(x) for x in actual.decode().splitlines()];expected=range(5000) if port=='a' else range(5000,10003);assert rows==[{'x':i,'twice':i*2} for i in expected]
 for key in ['state','result','inputRows','outputRows','executedTasks','durableTasks','recoveryTasks','resumedTasks','pipelineTrees']: assert len({m[key] for m in metrics})==1
 schema={'version':11,'fields':[{'name':'id','type':'int64','nullable':False},{'name':'amount','type':'decimal128','precision':38,'scale':2,'nullable':False},
 {'name':'at','type':'timestamp','timezone':'Asia/Shanghai','nullable':False},{'name':'raw','type':'binary','nullable':False},
 {'name':'profile','type':'struct','nullable':False,'children':[{'name':'name','type':'utf8','nullable':False},{'name':'missing','type':'int64','nullable':True}]},
 {'name':'tags','type':'list','nullable':False,'children':[{'name':'item','type':'utf8','nullable':True}]},
 {'name':'floats','type':'list','nullable':False,'children':[{'name':'item','type':'float64','nullable':True}]}]}
 rows=[{'id':9223372036854775807,'amount':'123456789012345678901234567890123456.78','at':'2024-02-29T08:00:00+08:00','raw':'AP9B','profile':{'name':'中文\0'},'tags':['',None,'中文'],'floats':['NaN','Infinity','-Infinity',-0.0,0.0,None]}]
 sc=r/'schema.json';sc.write_text(json.dumps(schema));rs=r/'rows.json';rs.write_text(json.dumps(rows,ensure_ascii=False))
 for index,cmd in enumerate([[data],[dotnet,data_dll],[java,'-Djava.library.path='+native,'-cp',classes+os.pathsep+jar,'DataConsumer']]):
  subprocess.run(cmd+[str(sc),str(rs),str(r/f'{index}.data.json'),str(r/f'{index}.wire')],env=managed_env if index==1 and arguments.managed_native_local else env,check=True,timeout=30)
 for suffix in ['data.json','wire']:
  expected=(r/f'0.{suffix}').read_bytes();assert all((r/f'{i}.{suffix}').read_bytes()==expected for i in [1,2])
 actual=json.loads((r/'0.data.json').read_text());assert actual[0]['at']=='2024-02-29T00:00:00.000000Z' and actual[0]['profile']['missing'] is None and actual[0]['floats']==rows[0]['floats']
 print('C/.NET/Java current ABI: same Package/Project parameters/scopes/File System/Profiling/child/control loops/precedence/10003-row outputs and metrics; Decimal/UTC/UTF8/NUL/nested/NaN/Infinity/zeros/NULL and binary wire parity passed')

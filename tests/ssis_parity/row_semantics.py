"""Native row atomicity, conversion/truncation policies and fault boundaries."""
import json,pathlib,subprocess,sys,tempfile
runner=pathlib.Path(sys.argv[1]).resolve()
version=json.loads((pathlib.Path(__file__).resolve().parents[2]/'metadata/product.json').read_text())['product']['protocolVersion']
def node(id,kind,**config):return dict(id=id,component=kind,config=config)
def edge(a,b,output=0):return dict(**{'from':a,'to':b},output=output,input=0)
with tempfile.TemporaryDirectory(prefix='ud-ssis-row-policies-') as tmp:
 root=pathlib.Path(tmp);(root/'state').mkdir();(root/'spill').mkdir()
 source=root/'in.ndjson';source.write_text(''.join(json.dumps(dict(id=i,label=' a ',raw='invalid',n=1))+'\n' for i in range(1,50001)))
 fields=[dict(name=n,type='int64' if n in ('id','n') else 'utf8',nullable=False) for n in ('id','label','raw','n')];fields[1]['scalar']=dict(type='DT_WSTR',length=3)
 components=[node('source','file.ndjson.source',connection='in',schema=dict(version=version,fields=fields),batchSize=64),node('derive','derive',includeInput=True,columns=[
  dict(name='label',action='replace',input='label',expression='"abcdef"',truncationDisposition='ignore'),
  dict(name='originalLength',expression='LEN([label])'),
  dict(name='ignored',expression='(DT_I4)[raw]',errorDisposition='ignore'),
  dict(name='calc',expression='[n]/([id]-17000L)',errorDisposition='redirect')]),node('normal','file.ndjson.destination',connection='out',fileMode='overwrite'),node('errors','file.ndjson.destination',connection='errors',fileMode='overwrite')]
 document=dict(version=version,id='row-policies',policy=dict(buffers=dict(connection='spill',residentBytes=1024,workingBytes=4194304,spillBytes=50000000,maxBlocks=10000)),connections=[dict(id=id,driver='file',resource=str(root/file)) for id,file in [('in','in.ndjson'),('out','out.ndjson'),('errors','errors.ndjson'),('spill','spill')]],controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='rows'))],constraints=[]),dataFlows=[dict(id='rows',components=components,paths=[edge('source','derive'),edge('derive','normal'),edge('derive','errors',1)])])
 package=root/'package.json';package.write_text(json.dumps(document))
 def run(resume=False,kill=False,workers=4,id='row-policies'):
  result=subprocess.run([str(runner),str(package),str(root/'state'),str(int(resume)),str(workers),'3' if kill else '0','1','source','134217728','','64',id],capture_output=True,text=True,timeout=120)
  if kill:assert result.returncode==91,(result.returncode,result.stderr);return
  assert result.returncode==0,(result.returncode,result.stderr);return json.loads(result.stdout)
 facts=run();assert facts['result']==0,dict(result=facts['result'])
 normal=[json.loads(line) for line in (root/'out.ndjson').read_text().splitlines()];errors=[json.loads(line) for line in (root/'errors.ndjson').read_text().splitlines()]
 assert len(normal)==49999 and {row['id'] for row in normal}==set(range(1,50001))-{17000}
 assert all(row['label']=='abc' and row['originalLength']==3 and row['ignored'] is None for row in normal)
 assert len(errors)==1 and errors[0]['inputValues']['id']==17000 and errors[0]['inputValues']['label']==' a ' and errors[0]['errorKind']=='conversion' and int(errors[0]['sourcePosition'])==16999
 assert run(resume=True)['result']==0
 # A partial snapshot must not accept a changed source under the former identity.
 (root/'state2').mkdir();(root/'state').rename(root/'completed');(root/'state2').rename(root/'state')
 run(kill=True,id='fingerprint')
 with source.open('a') as stream:stream.write('{"id":50001,"label":" a ","raw":"invalid","n":1}\n')
 rejected=run(resume=True,id='fingerprint');assert rejected['result'] in (9,10,17),rejected['result']
 print('Native row 17000 redirect, ignored column, character truncation, original-input reads, same-ID resume and changed-source refusal passed')

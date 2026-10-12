"""Fixed-data Linux ETL benchmark; reports results instead of assuming speedup."""
import argparse,hashlib,json,pathlib,resource,subprocess,tempfile,time
parser=argparse.ArgumentParser();parser.add_argument('--cli',required=True,type=pathlib.Path)
parser.add_argument('--rows',type=int,default=1000000);parser.add_argument('--output',required=True,type=pathlib.Path)
a=parser.parse_args();cli=a.cli.resolve()
version=json.loads((pathlib.Path(__file__).resolve().parents[1]/'metadata/product.json').read_text())['product']['protocolVersion']
records=[]
with tempfile.TemporaryDirectory(prefix='ud-four-engine-benchmark-') as work:
 r=pathlib.Path(work);source=r/'input.csv';spill=r/'spill';spill.mkdir()
 with source.open('w') as f:
  f.write('key,value\n')
  for i in range(a.rows-1,-1,-1):f.write(f'{i},{i%17}\n')
 for workers in (1,2,4,8,16):
  target=r/f'{workers}.csv'
  p={'version':version,'id':'benchmark','policy':{'buffers':{'connection':'spill','residentBytes':4194304,'workingBytes':33554432,'spillBytes':2147483648,'maxBlocks':100000}},
     'connections':[{'id':'source','driver':'file','resource':str(source)},{'id':'target','driver':'file','resource':str(target)},{'id':'spill','driver':'file','resource':str(spill)}],
     'controlFlow':{'tasks':[{'id':'flow','kind':'dataFlow','config':{'pipeline':'p'}}],'constraints':[]},
     'dataFlows':[{'id':'p','components':[{'id':'source','component':'file.csv.source','config':{'connection':'source','schema':{'version':version,'fields':[{'name':'key','type':'int64'},{'name':'value','type':'int64'}]}}},
        {'id':'sort','component':'sort','config':{'key':0,'partitions':workers}},{'id':'target','component':'file.csv.destination','config':{'connection':'target'}}],
        'paths':[{'from':'source','output':0,'to':'sort','input':0},{'from':'sort','output':0,'to':'target','input':0}]}]}
  package=r/'package.json';package.write_text(json.dumps(p));start=time.monotonic();before=resource.getrusage(resource.RUSAGE_CHILDREN)
  child=subprocess.Popen([str(cli),'execute',str(package),'{}','benchmark','benchmark',str(r),str(workers)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  peak=0
  while child.poll() is None:
   try:
    status=pathlib.Path(f'/proc/{child.pid}/status').read_text()
    for line in status.splitlines():
     if line.startswith('VmRSS:'):peak=max(peak,int(line.split()[1])*1024)
   except FileNotFoundError:pass
   time.sleep(.01)
  stdout,stderr=child.communicate();after=resource.getrusage(resource.RUSAGE_CHILDREN);elapsed=time.monotonic()-start
  if child.returncode:raise RuntimeError(stderr)
  metrics=json.loads(stdout);digest=hashlib.sha256(target.read_bytes()).hexdigest()
  assert metrics['inputRows']==a.rows and metrics['outputRows']==a.rows and metrics['workerHighWater']<=workers
  with target.open() as f:
   next(f)
   for expected,line in enumerate(f):assert int(line.split(',')[0])==expected
   assert expected+1==a.rows
  records.append({'workers':workers,'rows':a.rows,'seconds':elapsed,'rowsPerSecond':a.rows/elapsed,'cpuSeconds':after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,'peakRssBytes':peak,'outputSha256':digest,'metrics':metrics})
 assert len({record['outputSha256'] for record in records})==1
 a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps({'platform':'Linux','rows':a.rows,'results':records},indent=2)+'\n')
 print(json.dumps(records))

"""Sealed six-workload Linux matrix. Native driver reports CPU/RSS/owner metrics."""
import argparse, hashlib, json, os, pathlib, platform, sqlite3, subprocess, tempfile, time

def field(name,kind='int64'): return dict(name=name,type=kind,nullable=False)
def component(id,kind,**config): return dict(id=id,component=kind,config=config)
def edge(a,b,output=0,input=0): return {'from':a,'to':b,'output':output,'input':input}
def schema(fields):return dict(version=7,fields=fields)
def digest(path):
    total=0;count=0
    with path.open() as stream:
        for line in stream:
            row=json.loads(line);total^=int.from_bytes(hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).digest(),'big');count+=1
    return dict(rows=count,semanticSha256Xor=f'{total:064x}')
def run(driver,document,root,workers,resume=False,kill=False):
    package=root/'package.json';package.write_text(json.dumps(document));start=time.perf_counter()
    command=[driver,str(package),str(root/'state') if document.get('durable') else '',str(int(resume)),str(workers),'4' if kill else '0','24','output']
    # durable is a benchmark orchestration flag, never part of the Package.
    doc=dict(document);doc.pop('durable',None);package.write_text(json.dumps(doc))
    result=subprocess.run(command,capture_output=True,text=True,timeout=1200)
    if kill:
        assert result.returncode==91,(result.stdout,result.stderr);return {'seconds':time.perf_counter()-start,'killed':True,**json.loads(result.stderr)}
    assert result.returncode==0,(result.stdout,result.stderr)
    facts=json.loads(result.stdout);assert facts['result']==0,facts
    facts['seconds']=time.perf_counter()-start;return facts

def main():
    parser=argparse.ArgumentParser();parser.add_argument('driver');parser.add_argument('sqlite');parser.add_argument('--rows',type=int,default=1000000);parser.add_argument('--references',type=int,default=100000);parser.add_argument('--output',type=pathlib.Path,required=True);args=parser.parse_args()
    matrix=[]
    with tempfile.TemporaryDirectory(prefix='ud-r5-matrix-') as folder:
        base=pathlib.Path(folder);source=base/'facts.ndjson';reference=base/'reference.ndjson'
        with source.open('w') as stream:
            for id in range(args.rows):stream.write(json.dumps(dict(id=id,fk=id%args.references),separators=(',',':'))+'\n')
        with reference.open('w') as stream:
            for key in range(args.references):stream.write(json.dumps(dict(ref_key=key,label=str(key)),separators=(',',':'))+'\n')
        sources=schema([field('id'),field('fk')]);refs=schema([field('ref_key'),field('label','utf8')])
        widths={'streaming':[1,2,4,8],'sortSpill':[1,2,4,8],'join':[1,2,4,8],'errorDualSink':[1,2,4],'incrementalResume':[1,4],'cacheLookup':[1,4]}
        golden={}
        for workload,workers_list in widths.items():
            for workers in workers_list:
                root=base/f'{workload}-{workers}';root.mkdir();(root/'spill').mkdir();(root/'state').mkdir()
                connections=[dict(id=id,driver='file',resource=str(path)) for id,path in [('facts',source),('reference',reference),('spill',root/'spill'),('output',root/'output.ndjson'),('errors',root/'errors.ndjson'),('second',root/'second.ndjson')]]
                nodes=[component('source','file.ndjson.source',connection='facts',schema=sources,batchSize=256)];paths=[];caches=[];build=None
                if workload=='streaming':
                    nodes.extend([component('derive','derive',columns=[dict(name='doubled',expression={'op':'mul','args':[{'column':'id'},{'literal':2,'type':'int64'}]})]),component('filter','split',condition={'op':'ge','args':[{'column':'id'},{'literal':0,'type':'int64'}]}),component('output','file.ndjson.destination',connection='output'),component('errors','file.ndjson.destination',connection='errors')]);paths=[edge('source','derive'),edge('derive','filter'),edge('filter','output'),edge('filter','errors',1)]
                elif workload=='sortSpill':
                    nodes.extend([component('sort','sort',keys=[0],descending=True,partitions=workers),component('output','file.ndjson.destination',connection='output')]);paths=[edge('source','sort'),edge('sort','output')]
                elif workload=='join':
                    nodes.extend([component('reference','file.ndjson.source',connection='reference',schema=refs,batchSize=256),component('join','join',leftKeys=[1],rightKeys=[0],partitions=workers),component('output','file.ndjson.destination',connection='output')]);paths=[edge('source','join'),edge('reference','join',input=1),edge('join','output')]
                elif workload=='errorDualSink':
                    nodes.extend([component('quality','quality',errorPolicy='redirect',rules=[dict(id='range',kind='range',columns=['id'],maximum=max(0,args.rows-min(args.rows//10,100000)-1))]),component('output','file.ndjson.destination',connection='output'),component('errors','file.ndjson.destination',connection='errors')]);paths=[edge('source','quality'),edge('quality','output'),edge('quality','errors',1)]
                elif workload=='incrementalResume':
                    nodes[0]['config']['incremental']=dict(mode='watermark',keys=['id'],after=[-1],unique=True,monotonic=True)
                    with sqlite3.connect(root/'target.db') as db:db.execute('CREATE TABLE output(id INTEGER NOT NULL PRIMARY KEY,fk INTEGER NOT NULL)')
                    connections.append(dict(id='target',driver=args.sqlite,resource=str(root/'target.db'),credentialReference='fixture'));nodes.append(component('output','sql.destination',connection='target',table='output',mode='upsert',keys=[0]));paths=[edge('source','output')]
                else:
                    caches=[dict(id='ref',sourceConnection='reference',schema=refs,keys=[0],duplicates='first',scope='execution')]
                    build=dict(id='build',components=[component('reference','file.ndjson.source',connection='reference',schema=refs,batchSize=256),component('build','cacheBuild',cache='ref',mapping=[0,1])],paths=[edge('reference','build')])
                    nodes.extend([component('fan','multicast'),component('lookup','cacheLookup',cache='ref',keys=[1],errorPolicy='fail'),component('lookup2','cacheLookup',cache='ref',keys=[1],errorPolicy='fail'),component('output','file.ndjson.destination',connection='output'),component('second','file.ndjson.destination',connection='second')]);paths=[edge('source','fan'),edge('fan','lookup'),edge('fan','lookup2',1),edge('lookup','output'),edge('lookup2','second')]
                tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='flow'))];constraints=[]
                if build:tasks.insert(0,dict(id='build',kind='dataFlow',config=dict(pipeline='build')));constraints=[{'from':'build','to':'flow','on':'success'}]
                doc=dict(version=7,id='r5',connections=connections,caches=caches,policy=dict(maxConcurrency=workers,buffers=dict(connection='spill',residentBytes=65536,workingBytes=4194304,spillBytes=1000000000,maxBlocks=50000)),controlFlow=dict(tasks=tasks,constraints=constraints),dataFlows=([build] if build else [])+[dict(id='flow',components=nodes,paths=paths)])
                if workload=='incrementalResume':doc['durable']=True;interrupted=run(args.driver,doc,root,workers,kill=True);facts=run(args.driver,doc,root,workers,resume=True);facts['interruption']=interrupted;facts['totalSeconds']=interrupted['seconds']+facts['seconds'];db=sqlite3.connect(root/'target.db');assert db.execute('SELECT count(*),min(id),max(id) FROM output').fetchone()==(args.rows,0,args.rows-1);assert db.execute('SELECT count(*) FROM output WHERE fk != id % ?', (args.references,)).fetchone()[0]==0;db.close();signature={'rows':args.rows,'correctUpsertKeys':True,'correctForeignKeys':True}
                else:
                    facts=run(args.driver,doc,root,workers);signature=digest(root/'output.ndjson')
                    if workload=='errorDualSink':assert digest(root/'errors.ndjson')['rows']==min(args.rows//10,100000);assert facts['committedErrors']==min(args.rows//10,100000)
                    if workload=='cacheLookup':assert digest(root/'second.ndjson')==signature;assert facts['diagnostics']['caches'][0]['loaded']==args.references
                expected_rows=args.rows-min(args.rows//10,100000) if workload=='errorDualSink' else args.rows
                assert signature['rows']==expected_rows,(workload,signature,expected_rows)
                if workload in golden:assert golden[workload]==signature,(workload,workers,signature,golden[workload])
                else:golden[workload]=signature
                matrix.append(dict(workload=workload,workers=workers,rows=args.rows,references=args.references,rowsPerSecond=args.rows/facts['seconds'],signature=signature,**facts));print(workload,workers,facts['seconds'],flush=True)
                assert not list((root/'spill').glob('*.udw'))
    artifact={'machine':{'platform':platform.platform(),'cpuCount':os.cpu_count(),'cpuQuota':pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip() if pathlib.Path('/sys/fs/cgroup/cpu.max').exists() else None},'rows':args.rows,'references':args.references,'samples':matrix}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(artifact,indent=2));print(args.output)
if __name__=='__main__':main()

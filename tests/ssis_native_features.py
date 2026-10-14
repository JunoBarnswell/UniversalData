"""Independent SQLite/ZIP references, native pipeline receipts and crash acceptance."""
import argparse, collections, copy, hashlib, json, pathlib, sqlite3, subprocess, tempfile, time, zipfile
P=argparse.ArgumentParser();P.add_argument('runner');P.add_argument('sqlite');P.add_argument('--evidence');P.add_argument('--million',action='store_true');A=P.parse_args();results=[]
def field(name,type='int64',nullable=False,**extra):return dict(name=name,type=type,nullable=nullable,**extra)
def schema(*fields):return dict(version=10,fields=list(fields))
def node(id,component,**config):return dict(id=id,component=component,config=config)
def edge(a,b,output=0,input=0):return dict(**{'from':a,'to':b,'output':output,'input':input})
def rows(path):return [json.loads(l) for l in path.read_text().splitlines()]
def source(root,name,records,s,batch=37,ordering=None):
 p=root/(name+'.ndjson');p.write_text(''.join(json.dumps(x,ensure_ascii=False)+'\n' for x in records));n=node(name,'file.ndjson.source',connection=name,schema=s,batchSize=batch)
 if ordering:n['config']['ordering']=ordering
 return n,dict(id=name,driver='file',resource=str(p))
def package(root,nodes,paths,connections):
 (root/'spill').mkdir(exist_ok=True);(root/'state').mkdir(exist_ok=True)
 return dict(version=10,id='ssis-native',policy=dict(maxConcurrency=8,buffers=dict(connection='spill',residentBytes=8192,workingBytes=4194304,spillBytes=1000000000,maxBlocks=20000)),connections=connections+[dict(id='spill',driver='file',resource=str(root/'spill'))],controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='flow'))],constraints=[]),dataFlows=[dict(id='flow',components=nodes,paths=paths)])
def sink(root,id):return node(id,'file.ndjson.destination',connection=id),dict(id=id,driver='file',resource=str(root/(id+'.ndjson')))
def run(root,p,name,expected=0,workers=4,resume=False,kill=0,after=1,component='',memory=67108864):
 path=root/'package.json';path.write_text(json.dumps(p));start=time.monotonic();command=[A.runner,str(path),str(root/'state'),str(int(resume)),str(workers),str(kill),str(after),component,str(memory)]
 r=subprocess.run(command,text=True,capture_output=True,timeout=600)
 if kill:assert r.returncode==91,(name,r.stdout,r.stderr);facts={'killed':True}
 else:
  assert r.returncode==0,(name,r.stdout,r.stderr);facts=json.loads(r.stdout);assert facts['result']==expected,(name,facts,r.stderr)
 results.append(dict(case=name,workers=workers,seconds=round(time.monotonic()-start,3),result=facts.get('result'),metrics={k:v for k,v in facts.items() if k in ('committedRows','spillBytes','maxRssKiB','cpuSeconds')},killed=bool(kill)))
 print(name,'passed',flush=True);return facts
with tempfile.TemporaryDirectory(prefix='ud-ssis-native-') as folder:
 root=pathlib.Path(folder)
 # SQL supplies an independent Cartesian/outer join and stable ordering oracle.
 for jt in ('inner','left','full'):
  d=root/jt;d.mkdir();left=[dict(lk=k,lv='L'+str(i)) for i,k in enumerate([None,1,1,2,4])];right=[dict(rk=k,rv='R'+str(i)) for i,k in enumerate([None,1,1,1,3,4])]
  ls=schema(field('lk',nullable=True),field('lv','utf8'));rs=schema(field('rk',nullable=True),field('rv','utf8'));l,lc=source(d,'left',left,ls,batch=1);r,rc=source(d,'right',right,rs,batch=3);out,oc=sink(d,'out')
  p=package(d,[l,r,node('ls','sort',keys=[0]),node('rs','sort',keys=[0]),node('join','mergeJoin',leftKeys=['lk'],rightKeys=['rk'],joinType=jt),out],[edge('left','ls'),edge('right','rs'),edge('ls','join'),edge('rs','join',input=1),edge('join','out')],[lc,rc,oc]);run(d,p,'mergeJoin-'+jt)
  with sqlite3.connect(':memory:') as db:
   db.executescript('CREATE TABLE l(k,v);CREATE TABLE r(k,v);');db.executemany('INSERT INTO l VALUES(?,?)',[(x['lk'],x['lv']) for x in left]);db.executemany('INSERT INTO r VALUES(?,?)',[(x['rk'],x['rv']) for x in right]);expected=db.execute('SELECT l.k,l.v,r.k,r.v FROM l '+{'inner':'INNER','left':'LEFT','full':'FULL'}[jt]+' JOIN r ON l.k=r.k').fetchall()
  actual=[(x['lk'],x['lv'],x['rk'],x['rv']) for x in rows(d/'out.ndjson')];assert collections.Counter(actual)==collections.Counter(expected),(jt,actual,expected)
 # Merge stable-left tie, byte-wise UTF8, false source declaration and independent EOF.
 d=root/'merge';d.mkdir();s=schema(field('key','utf8',True),field('value','utf8'));left=[dict(key=k,value='L'+str(i)) for i,k in enumerate([None,'a','a','z','é'])];right=[dict(key=k,value='R'+str(i)) for i,k in enumerate([None,'a','b','中'])];l,lc=source(d,'left',left,s,1);r,rc=source(d,'right',right,s,2);out,oc=sink(d,'out');p=package(d,[l,r,node('ls','sort',keys=[0]),node('rs','sort',keys=[0]),node('merge','merge',keys=['key']),out],[edge('left','ls'),edge('right','rs'),edge('ls','merge'),edge('rs','merge',input=1),edge('merge','out')],[lc,rc,oc]);run(d,p,'merge-UTF8-stability');assert rows(d/'out.ndjson')==sorted(left+right,key=lambda x:(x['key'] is not None,(x['key'] or '').encode()))
 d=root/'false-order';d.mkdir();l,lc=source(d,'left',[dict(key='z',value='x'),dict(key='a',value='y')],s,1,dict(keys=[0],stable=True));r,rc=source(d,'right',[],s,1,dict(keys=[0],stable=True));out,oc=sink(d,'out');p=package(d,[l,r,node('merge','merge',keys=['key']),out],[edge('left','merge'),edge('right','merge',input=1),edge('merge','out')],[lc,rc,oc]);run(d,p,'false-IsSorted-rejected',5);assert not (d/'out.ndjson').exists()
 # Fixed output pivot columns, aggregate kernels, crossing tiny batches and unpivot.
 for duplicate in ('reject','sum','count','min','max','first','last'):
  d=root/('pivot-'+duplicate);d.mkdir();data=[dict(group=g,p='AB'[i%2],v=i) for g in range(21) for i in range(6 if duplicate!='reject' else 2)];s=schema(field('group'),field('p','utf8'),field('v'));src,sc=source(d,'in',data,s,1);out,oc=sink(d,'out');p=package(d,[src,node('sort','sort',keys=[0]),node('pivot','pivot',setKeys=['group'],pivotKey='p',valueColumn='v',values=[dict(value='A',name='A'),dict(value='B',name='B')],duplicate=duplicate),out],[edge('in','sort'),edge('sort','pivot'),edge('pivot','out')],[sc,oc]);run(d,p,'pivot-'+duplicate);actual=rows(d/'out.ndjson');assert len(actual)==21
  for row in actual:
   for letter in 'AB':
    values=[x['v'] for x in data if x['group']==row['group'] and x['p']==letter];expected={'reject':lambda v:v[0],'sum':sum,'count':len,'min':min,'max':max,'first':lambda v:v[0],'last':lambda v:v[-1]}[duplicate](values);assert row[letter]==expected,(row,expected)
  if duplicate=='sum':
   (d/'unpivot-state').mkdir();p['connections'].append(dict(id='long',driver='file',resource=str(d/'long.ndjson')));p['dataFlows'][0]['components'][-1]=node('unpivot','unpivot',retain=['group'],columns=[dict(column='A',key='A'),dict(column='B',key='B')],keyName='p',valueName='v');p['dataFlows'][0]['components'].append(node('long','file.ndjson.destination',connection='long'));p['dataFlows'][0]['paths'][-1]=edge('pivot','unpivot');p['dataFlows'][0]['paths'].append(edge('unpivot','long'));p['id']='pivot-roundtrip';(d/'state').rename(d/'old-state');(d/'state').mkdir();run(d,p,'pivot-unpivot-roundtrip');assert len(rows(d/'long.ndjson'))==42
 # Complementary decisions are independent of worker scheduling and batch size.
 sampling_sets={}
 for mode in ('rowSampling','percentageSampling'):
  d=root/mode;d.mkdir();src,sc=source(d,'in',[dict(id=i) for i in range(10000)],schema(field('id')),113)
  for workers in (1,2,4,8):
   work=d/str(workers);work.mkdir();ss=copy.deepcopy(src);selected,cc=sink(work,'selected');complement,co=sink(work,'complement');config=dict(seed=123,count=500,whenShort='all') if mode=='rowSampling' else dict(seed=123,percentage=10)
   p=package(work,[ss,node('sample',mode,**config),selected,complement],[edge('in','sample'),edge('sample','selected'),edge('sample','complement',output=1)],[sc,cc,co]);run(work,p,mode+'-'+str(workers),workers=workers);yes={x['id'] for x in rows(work/'selected.ndjson')};no={x['id'] for x in rows(work/'complement.ndjson')};assert not yes&no and yes|no==set(range(10000));assert len(yes)==500 if mode=='rowSampling' else 850<len(yes)<1150
   if workers==1:sampling_sets[mode]=yes
   else:assert yes==sampling_sets[mode]
  assert sampling_sets[mode]!=set(range(len(sampling_sets[mode])))
 # Excel output is independently inspected by Python ZIP/XML, then read natively.
 d=root/'excel';d.mkdir();s=schema(field('id'),field('text','utf8',True),field('money','decimal128',precision=38,scale=2),field('stamp','timestamp',timezone='UTC'),field('flag','bool'),field('binary','binary'))
 data=[dict(id=i,text='中<&> _x0041_' if i%2 else None,money='123456789012345678901234567890.12',stamp='2026-10-13T10:11:12.123456Z',flag=i%2==1,binary='AAECAw==') for i in range(257)];src,sc=source(d,'in',data,s,31);p=package(d,[src,node('excel','file.xlsx.destination',connection='excel',sheet='事实',header=True,datePolicy='1904')],[edge('in','excel')],[sc,dict(id='excel',driver='excel',resource=str(d/'book.xlsx'))]);run(d,p,'xlsx-write')
 with zipfile.ZipFile(d/'book.xlsx') as z:
  assert z.testzip() is None;xml=z.read('xl/worksheets/sheet1.xml').decode();assert xml.count('<row ')==258 and '<f>' not in xml and 'inlineStr' in xml
 d2=root/'excel-read';d2.mkdir();out,oc=sink(d2,'out');p=package(d2,[node('excel','file.xlsx.source',connection='excel',sheet='事实',range='A1:F258',header=True,datePolicy='1904',schema=s,batchSize=17),out],[edge('excel','out')],[dict(id='excel',driver='excel',resource=str(d/'book.xlsx')),oc]);run(d2,p,'xlsx-read-roundtrip');actual=rows(d2/'out.ndjson');assert actual==data,(len(actual),len(data),next(((a,b) for a,b in zip(actual,data) if a!=b),None))
 # Independent fixtures: shared strings, sparse cells, cached formula, calendar systems.
 def workbook(path,sheet_xml,shared='',date1904=False,extra=None,rel=None):
  with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
   z.writestr('[Content_Types].xml','<Types/>');z.writestr('xl/workbook.xml','<workbook xmlns:r="urn:rel"><workbookPr date1904="'+str(int(date1904))+'"/><sheets><sheet name="Data" r:id="rId1"/></sheets></workbook>');z.writestr('xl/_rels/workbook.xml.rels',rel or '<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>');z.writestr('xl/worksheets/sheet1.xml',sheet_xml)
   if shared:z.writestr('xl/sharedStrings.xml',shared)
   for name,content in (extra or {}).items():z.writestr(name,content)
 tests=[('shared-sparse','<worksheet><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="C1"><f>1+1</f><v>2</v></c></row></sheetData></worksheet>',0,'<sst><si><r><t>中</t></r><r><t>文</t></r></si></sst>',{}),('missing-formula','<worksheet><sheetData><row r="1"><c r="A1"><f>1+1</f></c></row></sheetData></worksheet>',14,'',{}),('DTD','<!DOCTYPE worksheet [<!ENTITY e SYSTEM "file:///etc/passwd">]><worksheet><sheetData/></worksheet>',12,'',{}),('traversal','<worksheet/>',9,'',{'../escape':'x'}),('macro','<worksheet/>',12,'',{'xl/vbaProject.bin':'x'})]
 for name,xml,expected,shared,extra in tests:
  d=root/name;d.mkdir();workbook(d/'book.xlsx',xml,shared,extra=extra);out,oc=sink(d,'out');p=package(d,[node('source','file.xlsx.source',connection='excel',schema=schema(field('a','utf8',True),field('b',nullable=True),field('c',nullable=True)),sheet='Data',header=False),out],[edge('source','out')],[dict(id='excel',driver='excel',resource=str(d/'book.xlsx')),oc]);run(d,p,'xlsx-'+name,expected)
  if not expected:assert rows(d/'out.ndjson')==[dict(a='中文',b=None,c=2)]
 # Atomic SCD: actual dimension SQL table, current Cache source and typed writes.
 def scd_fixture(d,records):
  d.mkdir();dbpath=d/'dimension.db'
  with sqlite3.connect(dbpath) as db:db.execute('CREATE TABLE dim(sk TEXT NOT NULL PRIMARY KEY,bk INTEGER NOT NULL,t1 TEXT NOT NULL,t2 TEXT NOT NULL,fixed TEXT NOT NULL,current BOOLEAN NOT NULL,valid_from TIMESTAMP_UTC NOT NULL,valid_to TIMESTAMP_UTC,version INTEGER NOT NULL)');db.execute('INSERT INTO dim VALUES(?,?,?,?,?,?,?,?,?)',('old',1,'old-name','old-history','fixed',1,1767225600000000,None,1))
  ds=schema(field('sk','utf8',keyOrder=1),field('bk'),field('t1','utf8'),field('t2','utf8'),field('fixed','utf8'),field('current','bool'),field('valid_from','timestamp',timezone='UTC'),field('valid_to','timestamp',True,timezone='UTC'),field('version'))
  fs=schema(field('bk'),field('name','utf8'),field('history','utf8'),field('fixed','utf8'),field('time','timestamp',timezone='UTC'));src,sc=source(d,'facts',records,fs,1);unchanged,uc=sink(d,'unchanged');bad,bc=sink(d,'bad');connections=[sc,uc,bc,dict(id='dim',driver=A.sqlite,resource=str(dbpath),credentialReference='fixture',maxLeases=4)]
  ns=[src,node('sort','sort',keys=[0,4]),node('scd','scd',cache='dim',businessKeys=['bk'],dimensionKeys=['bk'],mapping=[dict(source='bk',target='bk',change='key'),dict(source='name',target='t1',change='type1'),dict(source='history',target='t2',change='type2'),dict(source='fixed',target='fixed',change='fixed')],current='current',validFrom='valid_from',validTo='valid_to',surrogate='sk',version='version',effectiveTime='time',errorPolicy='redirect'),node('target','scd.destination',connection='dim',table='dim',businessKeys=['bk'],current='current',provisionReceipts=True),unchanged,bad]
  paths=[edge('facts','sort'),edge('sort','scd'),edge('scd','unchanged'),edge('scd','bad',output=5)]+[edge('scd','target',output=i,input=i-1) for i in range(1,5)]
  p=package(d,ns,paths,connections);p['caches']=[dict(id='dim',sourceConnection='dim',schema=ds,keys=[1],duplicates='error',errorPolicy='fail',scope='execution')];p['controlFlow']['tasks'].insert(0,dict(id='build',kind='dataFlow',config=dict(pipeline='build')));p['controlFlow']['constraints']=[dict(**{'from':'build','to':'flow','on':'success'})];p['dataFlows'].insert(0,dict(id='build',components=[node('dimension','sql.source',connection='dim',table='dim',sql='SELECT * FROM dim WHERE current=1',schema=ds,batchSize=2,schemaPolicy=dict(policy='strict')),node('cache','cacheBuild',cache='dim',mapping=list(range(9)))],paths=[edge('dimension','cache')]));return p
 records=[dict(bk=1,name='old-name',history='old-history',fixed='fixed',time='2026-02-01T00:00:00Z'),dict(bk=1,name='new-name',history='old-history',fixed='fixed',time='2026-03-01T00:00:00Z'),dict(bk=1,name='latest-name',history='new-history',fixed='fixed',time='2026-04-01T00:00:00Z'),dict(bk=1,name='latest-name',history='new-history',fixed='violation',time='2026-05-01T00:00:00Z'),dict(bk=2,name='two',history='new',fixed='fixed',time='2026-04-01T00:00:00Z')]
 d=root/'scd';p=scd_fixture(d,records);run(d,p,'scd-type1-type2-new-fixed')
 with sqlite3.connect(d/'dimension.db') as db:
  actual=db.execute('SELECT bk,t1,t2,current,valid_from,valid_to,version FROM dim ORDER BY bk,version').fetchall();assert len(actual)==3 and actual[0][1]=='new-name' and actual[0][3]==0 and actual[0][5]==1775001600000000 and actual[1][1:4]==('latest-name','new-history',1) and actual[1][-1]==2,actual;receipts=db.execute('SELECT count(*) FROM ud_sdk_receipts_v10').fetchone()[0]
 assert len(rows(d/'unchanged.ndjson'))==1 and len(rows(d/'bad.ndjson'))==1
 run(d,p,'scd-receipt-resume',resume=True)
 with sqlite3.connect(d/'dimension.db') as db:assert db.execute('SELECT count(*) FROM dim').fetchone()[0]==3 and db.execute('SELECT count(*) FROM ud_sdk_receipts_v10').fetchone()[0]==receipts
 d=root/'scd-crash';p=scd_fixture(d,records);run(d,p,'scd-kill-after-commit',kill=4,after=1,component='target');run(d,p,'scd-crash-resume',resume=True)
 with sqlite3.connect(d/'dimension.db') as db:assert db.execute('SELECT count(*) FROM dim').fetchone()[0]==3
 if A.million:
  d=root/'million-merge';d.mkdir();s=schema(field('lk'),field('lv'));left=[dict(lk=7,lv=i) for i in range(1000000)];l,lc=source(d,'left',left,s,251,dict(keys=[0],stable=True));del left;r,rc=source(d,'right',[dict(rk=7,rv=1)],schema(field('rk'),field('rv')),1,dict(keys=[0],stable=True));out,oc=sink(d,'out');p=package(d,[l,r,node('join','mergeJoin',leftKeys=['lk'],rightKeys=['rk']),out],[edge('left','join'),edge('right','join',input=1),edge('join','out')],[lc,rc,oc]);facts=run(d,p,'million-duplicate-group-low-budget',workers=8,memory=16777216);assert sum(1 for _ in (d/'out.ndjson').open())==1000000
if A.evidence:pathlib.Path(A.evidence).write_text(json.dumps(dict(abi=9,platform='Linux',cases=results),ensure_ascii=False,indent=2)+'\n')
print('Native N1–N5 acceptance passed:',len(results),'cases',flush=True)

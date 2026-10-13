"""Native codecs and an actual TLS REST service; every flow executes in Native."""
import copy,http.server,json,pathlib,ssl,subprocess,sys,tempfile,threading,urllib.parse,collections
runner=str(pathlib.Path(sys.argv[1]).resolve())
field=lambda name,type='int64',nullable=False,**kw:dict(name=name,type=type,nullable=nullable,**kw)
node=lambda id,component,**config:dict(id=id,component=component,config=config)
def document(connections,nodes,paths):return dict(version=9,id='formats',connections=connections,controlFlow=dict(tasks=[dict(id='flow',kind='dataFlow',config=dict(pipeline='p'))],constraints=[]),dataFlows=[dict(id='p',components=nodes,paths=[{'from':a,'output':0,'to':b,'input':0} for a,b in paths])])
with tempfile.TemporaryDirectory(prefix='ud-advanced-formats-') as tmp:
 root=pathlib.Path(tmp);schema=dict(version=9,fields=[field('id'),field('text','utf8',True),field('amount','decimal128',True,precision=30,scale=6),field('payload','binary',True),field('stamp','timestamp',True,timezone='UTC'),field('items','list',False,children=[field('item','int64',True)])]);records=[dict(id=i,text=None if i%3==0 else '汉😀',amount=None if i%5==0 else '9007199254740993.123456',payload=None if i%7==0 else '' if i%2 else 'AP8B',stamp=None if i%11==0 else '2026-01-01T00:00:00.000001Z',items=[i,None]) for i in range(31)]
 (root/'in.ndjson').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in records))
 def execute(name,p):
  folder=root/name;folder.mkdir();(folder/'state').mkdir();(folder/'spill').mkdir();p=copy.deepcopy(p);p['connections'].append(dict(id='spill',driver='file',resource=str(folder/'spill')));p['policy']=dict(buffers=dict(connection='spill',residentBytes=1048576,workingBytes=8388608,spillBytes=100000000,maxBlocks=2048));path=folder/'package.json';path.write_text(json.dumps(p));r=subprocess.run([runner,str(path),str(folder/'state'),'0','4'],capture_output=True,text=True,timeout=60);assert r.returncode==0,(r.stdout,r.stderr);facts=json.loads(r.stdout);assert facts['result']==0,(name,facts,r.stderr);return folder,p
 for kind in ('parquet','arrowIpc','arrowStream'):
  target=root/(kind+'.file');output=root/(kind+'.ndjson');connections=[dict(id=id,driver='file',resource=str(path)) for id,path in [('input',root/'in.ndjson'),('format',target),('output',output)]]
  execute(kind+'-write',document(connections,[node('s','file.ndjson.source',connection='input',schema=schema,batchSize=7),node('d','file.'+kind+'.destination',connection='format')],[('s','d')]))
  execute(kind+'-read',document(connections,[node('s','file.'+kind+'.source',connection='format',schema=schema,batchSize=3),node('d','file.ndjson.destination',connection='output')],[('s','d')]))
  assert [json.loads(l) for l in output.read_text().splitlines()]==records,kind;print(kind,'typed roundtrip passed',flush=True)
 # HTTPS is a real external service. It owns its own target receipts; the client
 # computes and verifies Native digests, positions, typed metadata and replay.
 counts=collections.Counter();receipts={};written=[]
 class Service(http.server.BaseHTTPRequestHandler):
  def log_message(self,*args):pass
  def reply(self,code,value):
   payload=json.dumps(value,ensure_ascii=False).encode();self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
  def do_GET(self):
   url=urllib.parse.urlparse(self.path);params=urllib.parse.parse_qs(url.query)
   if url.path=='/source':
    cursor=int(params.get('cursor',['0'])[0]);counts['source']+=1
    if counts['source']==1:self.reply(429,dict(error='rate limit'));return
    subset=records[cursor:cursor+7];self.reply(200,dict(rows=subset,nextCursor=str(cursor+7) if cursor+7<len(records) else None));return
   key=tuple(params.get(k,[''])[0] for k in ('tenant','job','batch'));self.reply(200,receipts.get(key,dict(committed=False)))
  def do_POST(self):
   body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));assert len(body['rows'])==len(body['rowInfo']);assert body['schema']['version']==9
   key=tuple(body[k] for k in ('tenant','job','batchId'));receipt=dict(committed=True,digest=body['digest'],sourcePosition=body['sourcePosition'])
   if key in receipts:assert receipts[key]==receipt
   else:written.extend(body['rows']);receipts[key]=receipt
   counts['post']+=1;self.reply(200,receipt)
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Service)
 private=root/'tls';private.mkdir()
 subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(private/'server.key'),'-out',str(private/'server.pem'),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1','-addext','extendedKeyUsage=serverAuth'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
 (private/'server.key').chmod(0o600);(private/'ca.pem').write_bytes((private/'server.pem').read_bytes())
 tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(private/'server.pem',private/'server.key');server.socket=tls.wrap_socket(server.socket,server_side=True);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();base='https://localhost:'+str(server.server_port)
 try:
  connections=[dict(id='source',driver='http',resource=base+'/source'),dict(id='sink',driver='http',resource=base+'/sink'),dict(id='ca',driver='file',resource=str(private/'ca.pem'))]
  folder,p=execute('rest',document(connections,[node('s','rest.source',connection='source',schema=schema,caConnection='ca',batchSize=3,maxAttempts=3,retryDelayMs=1),node('d','rest.destination',connection='sink',table='target',mode='append',receiptUrl=base+'/receipt',caConnection='ca')],[('s','d')]))
  assert written==records and counts['source']==6 and receipts,(len(written),counts)
  requests=counts['post'];r=subprocess.run([runner,str(folder/'package.json'),str(folder/'state'),'1','4'],capture_output=True,text=True,timeout=30);assert r.returncode==0 and json.loads(r.stdout)['result']==0,(r.stdout,r.stderr);assert counts['post']==requests and written==records
  print('REST verified TLS, pagination, rate-limit retry, typed rows, receipts and durable resume passed',flush=True)
 finally:server.shutdown();server.server_close();thread.join()

import json,struct,subprocess,sys
package={'version':3,'id':'worker','variables':{'x':{'type':'int64','default':0}},'controlFlow':{'tasks':[{'id':'set','kind':'set','config':{'name':'x','value':{'literal':42,'type':'int64'}}}],'constraints':[]},'dataFlows':[]}
def request(p):
 b=json.dumps(p).encode();result=subprocess.run([sys.argv[1]],input=struct.pack('<I',len(b))+b,capture_output=True,timeout=10);assert result.returncode==0
 n,=struct.unpack_from('<I',result.stdout);body=result.stdout[4:];assert len(body)==n;return json.loads(body)
assert request(package)=={'version':3,'state':5,'result':0}
package['version']=2;assert request(package)['result']==10
r=subprocess.run([sys.argv[1]],input=struct.pack('<I',64*1024*1024+1),capture_output=True,timeout=10);assert r.returncode==2
print('Worker: only Package protocol 3, old model and oversized frame rejection passed')

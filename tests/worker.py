"""Actual product Worker starts no business I/O before the Native-ID handshake."""
import json, pathlib, subprocess, sys, tempfile, uuid
with tempfile.TemporaryDirectory(prefix='ud-worker-v10-') as folder:
    root=pathlib.Path(folder);policy=root/'policy.json'
    policy.write_text(json.dumps(dict(dataDirectory=str(root/'state'),fileRoots=[],networkOrigins=[],pluginRoots=[],credentialFiles={})))
    package=dict(version=10,id='worker',controlFlow=dict(tasks=[],constraints=[]),dataFlows=[])
    process=subprocess.Popen([sys.argv[1],str(policy)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    ticket=uuid.uuid4().hex
    process.stdin.write(json.dumps(dict(version=10,ticket=ticket,package=json.dumps(package)))+'\n');process.stdin.flush()
    ready=json.loads(process.stdout.readline());assert ready['kind']=='ready' and ready['ticket']==ticket
    identity=uuid.UUID(ready['executionId']);assert identity.version==4
    process.stdin.write('start\n');process.stdin.flush()
    terminal=json.loads(process.stdout.readline());assert terminal['kind']=='terminal' and terminal['executionId']==ready['executionId']
    assert terminal['state']=='Succeeded' and terminal['result']==0
    process.stdin.close();assert process.wait(timeout=30)==0
    # A pipe loss before the handshake authorizes no execution.
    request=json.dumps(dict(version=10,ticket=uuid.uuid4().hex,package=json.dumps(package)))+'\n'
    abandoned=subprocess.run([sys.argv[1],str(policy)],input=request,capture_output=True,text=True,timeout=30)
    assert abandoned.returncode==2 and all(json.loads(line)['kind']=='ready' for line in abandoned.stdout.splitlines())
    stale=subprocess.run([sys.argv[1],str(policy)],input=json.dumps(dict(version=9,ticket=uuid.uuid4().hex,package=json.dumps(package)))+'\n',capture_output=True,text=True,timeout=30)
    assert stale.returncode!=0 and not stale.stdout

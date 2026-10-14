import json, pathlib, subprocess, sys, tempfile
for cut,code in ((1,86),(2,87)):
    with tempfile.TemporaryDirectory(prefix='ud-file-generation-') as folder:
        root=pathlib.Path(folder);target=root/'data.txt';target.write_bytes(b'original\n')
        package=root/'package.json';package.write_text(json.dumps(dict(version=11,id='file-generation',connections=[dict(id='target',driver='file',resource=str(target))],controlFlow=dict(tasks=[dict(id='write',kind='test.generation',config={})],constraints=[]),dataFlows=[])))
        state=root/'journal';state.mkdir()
        def run(resume,interrupt):return subprocess.run([sys.argv[1],str(package),str(state),str(resume),str(interrupt)],capture_output=True,text=True,timeout=30)
        first=run(0,cut);assert first.returncode==code,(first.returncode,first.stdout,first.stderr)
        assert target.read_bytes()==(b'original\n' if cut==1 else b'new\n')
        recovered=run(1,0);assert recovered.returncode==0,(recovered.stdout,recovered.stderr)
        assert target.read_bytes()==b'new\n'
        originals=list(root.glob('data.txt.ud-original-*'));assert len(originals)==1 and originals[0].read_bytes()==b'original\n'

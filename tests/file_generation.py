import json, pathlib, subprocess, sys, tempfile
from product_version import PROTOCOL_VERSION
cases = [(1, 86), (2, 87)]
if sys.platform.startswith('linux'):
    cases.append((3, 88))
for cut,code in cases:
    with tempfile.TemporaryDirectory(prefix='ud-file-generation-') as folder:
        root=pathlib.Path(folder);target=root/'data.txt';target.write_bytes(b'original\n')
        package=root/'package.json';package.write_text(json.dumps(dict(version=PROTOCOL_VERSION,id='file-generation',connections=[dict(id='target',driver='file',resource=str(target))],controlFlow=dict(tasks=[dict(id='write',kind='test.generation',config={})],constraints=[]),dataFlows=[])))
        state=root/'journal';state.mkdir()
        def run(resume,interrupt):return subprocess.run([sys.argv[1],str(package),str(state),str(resume),str(interrupt)],capture_output=True,text=True,timeout=30)
        first=run(0,cut);assert first.returncode==code,(first.returncode,first.stdout,first.stderr)
        assert target.read_bytes()==(b'original\n' if cut==1 else b'new\n')
        if cut == 3:
            pending = run(1, 3)
            assert pending.returncode == 88, (pending.returncode, pending.stdout, pending.stderr)
        recovered=run(1,0);assert recovered.returncode==0,(recovered.stdout,recovered.stderr)
        assert target.read_bytes()==b'new\n'
        originals=list(root.glob('data.txt.ud-original-*'));assert len(originals)==1 and originals[0].read_bytes()==b'original\n'
        if cut == 1:
            tail = state / 'journal.tail'
            damaged = bytearray(tail.read_bytes())
            damaged[-1] ^= 1
            tail.write_bytes(damaged)
            unresolved = run(1, 4)
            assert unresolved.returncode == 88, (unresolved.returncode, unresolved.stdout, unresolved.stderr)
            assert target.read_bytes() == b'new\n'

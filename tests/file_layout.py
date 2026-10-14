import pathlib, subprocess, sys, tempfile
with tempfile.TemporaryDirectory(prefix='ud-file-layout-') as folder:
    root=pathlib.Path(folder);bad=root/'bad.csv'
    bad.write_bytes(('id;name\n1;ok\nbad;value\n').encode('utf-16'))
    result=subprocess.run([sys.argv[1],str(root/'delimited.csv'),str(root/'fixed.csv'),str(bad)],capture_output=True,text=True,timeout=30)
    assert result.returncode==0,(result.stdout,result.stderr)
    assert (root/'delimited.csv').read_bytes().startswith(b'\xff\xfe')
    assert (root/'fixed.csv').read_bytes().startswith(b'\xfe\xff')

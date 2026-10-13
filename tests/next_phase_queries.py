"""Installed C/.NET/Java query parity for the one native Schema/RowInfo owner."""
import argparse, copy, json, os, pathlib, sqlite3, subprocess, tempfile
p=argparse.ArgumentParser()
for name in ('query','data','dotnet','data-dll','java','classes','jar','native','sqlite'):
    p.add_argument('--'+name,required=True)
a=p.parse_args();env=dict(os.environ);env['LD_LIBRARY_PATH']=a.native
commands=[[a.query],[a.dotnet,a.data_dll],[a.java,'-Djava.library.path='+a.native,'-cp',a.classes+os.pathsep+a.jar,'DataConsumer']]
def run(index,args):
    # The NuGet consumer must use the native asset delivered by NuGet itself.
    local=dict(os.environ) if index==1 else env
    if index==1:local.pop('LD_LIBRARY_PATH',None)
    subprocess.run(commands[index]+list(map(str,args)),env=local,check=True,timeout=30)
def write(path,value):path.write_text(json.dumps(value,ensure_ascii=False))
with tempfile.TemporaryDirectory(prefix='ud-native-queries-') as folder:
    root=pathlib.Path(folder)
    db=root/'source.db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE source(id INTEGER PRIMARY KEY,name TEXT NOT NULL)')
    package=root/'package.json'
    write(package,dict(version=10,id='query-parity',connections=[dict(id='db',driver=a.sqlite,resource=str(db),credentialReference='fixture')],controlFlow=dict(tasks=[],constraints=[]),dataFlows=[]))
    for round in (0,1):
        if round:
            with sqlite3.connect(db) as connection:connection.execute('ALTER TABLE source ADD COLUMN extra TEXT')
        for index in range(3):run(index,['--discover',package,'db','source',root/f'{index}.schema.json'])
        canonical=(root/'0.schema.json').read_bytes()
        assert all((root/f'{i}.schema.json').read_bytes()==canonical for i in (1,2))
        discovered=json.loads(canonical);assert len(discovered['fields'])==2+round
        assert discovered['fields'][0]['keyOrder']==1 and not discovered['fields'][0]['nullable']
    expected=copy.deepcopy(discovered);expected['fields'].pop()
    policy=root/'policy.json';actual=root/'actual.json';wanted=root/'expected.json'
    write(actual,discovered);write(wanted,expected)
    for mode in ('strict','compatible'):
        write(policy,dict(policy=mode))
        for index in range(3):run(index,['--compare',wanted,actual,policy,root/f'{index}.diff.json'])
        result=(root/'0.diff.json').read_bytes()
        assert all((root/f'{i}.diff.json').read_bytes()==result for i in (1,2))
        assert json.loads(result)['compatible']==(mode=='compatible')
    renamed=copy.deepcopy(expected);renamed['fields'][1]['name']='display'
    write(actual,renamed);write(policy,dict(policy='explicitMapping',mapping=[dict(expected=f['identity'],actual=f['identity']) for f in expected['fields']]))
    for index in range(3):run(index,['--compare',wanted,actual,policy,root/f'{index}.diff.json'])
    result=(root/'0.diff.json').read_bytes();assert json.loads(result)['compatible']
    assert all((root/f'{i}.diff.json').read_bytes()==result for i in (1,2))
    schema=root/'wire.schema.json';rows=root/'rows.json';wire=root/'wire.udw';identified=root/'identified.udw'
    write(schema,dict(version=10,fields=[dict(name='name',type='utf8',nullable=False)]));write(rows,[{'name':'中文\u0000identity'}])
    subprocess.run([a.data,str(schema),str(rows),str(root/'rows.out.json'),str(wire)],env=env,check=True)
    run(0,['--identify',wire,identified,root/'0.row.json'])
    for index in (1,2):run(index,['--row-info',identified,root/f'{index}.row.json'])
    result=(root/'0.row.json').read_bytes()
    assert all((root/f'{i}.row.json').read_bytes()==result for i in (1,2))
    assert len(json.loads(result)['id'])==32
print('C/.NET8/Java17: actual SQLite discovery, metadata refresh, Strict/Compatible/ExplicitMapping and identified Wire row metadata are byte-identical')

"""Every shared type exercises native Package/Pipeline comparison/group/join."""
import collections
import json
import os
import pathlib
import subprocess
import sys
import tempfile
from semantic_cases import CASES, semantic_key

cli = str(pathlib.Path(sys.argv[1]).resolve())
assert len(sys.argv) in [2, 8], 'cli [dotnet package.dll java classes jar native-dir]'
languages = 3 if len(sys.argv) == 8 else 1
with tempfile.TemporaryDirectory(prefix='ud-value-pipeline-') as folder:
    root = pathlib.Path(folder)
    for metadata, values in CASES:
        directory = root / metadata['type']; directory.mkdir()
        source = directory / 'input.ndjson'; right = directory / 'right.ndjson'
        left_rows = [dict(key=key, peer=key, id=i) for i, key in enumerate(values)]
        right_rows = [dict(rkey=key, rid=i) for i, key in enumerate(reversed(values))]
        for path, rows in [(source, left_rows), (right, right_rows)]:
            path.write_text(''.join(json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n' for row in rows), encoding='utf-8')
        fields = [metadata, dict(metadata, name='peer'), dict(name='id', type='int64', nullable=False)]
        rfields = [dict(metadata, name='rkey'), dict(name='rid', type='int64', nullable=False)]
        schema = dict(version=4, fields=fields); rschema = dict(version=4, fields=rfields)
        def source_component(name='source', connection='input', schema=schema):
            return dict(id=name, component='file.ndjson.source', config=dict(connection=connection, schema=schema, batchSize=2))
        def sink(name):
            return dict(id=name, component='file.ndjson.destination', config=dict(connection=name))
        def edge(a, b, output=0, port=0):
            return dict(**{'from':a, 'to':b}, output=output, input=port)
        flows = [dict(id='compare', components=[source_component(),
            dict(id='derive', component='derive', config=dict(columns=[
                dict(name='copy', expression=dict(column='key')),
                dict(name='same', expression=dict(op='eq', args=[dict(column='key'), dict(column='peer')]))])),
            dict(id='split', component='split', config=dict(condition=dict(column='same'))), sink('yes'), sink('no')],
            paths=[edge('source','derive'), edge('derive','split'), edge('split','yes'), edge('split','no',1)]),
            dict(id='join', components=[source_component(), source_component('right','right',rschema),
                dict(id='join', component='join', config=dict(leftKey=0, rightKey=0)), sink('joined')],
                paths=[edge('source','join'), edge('right','join',port=1), edge('join','joined')]),
            dict(id='group', components=[source_component(),
                dict(id='aggregate', component='aggregate', config=dict(commands='group key\ncount * cnt\n')), sink('groups')],
                paths=[edge('source','aggregate'), edge('aggregate','groups')])]
        for language in range(languages):
            package = dict(version=4, id='semantics-'+metadata['type'], parameters={}, variables={},
                connections=[dict(id=name, driver='file', resource=str(path)) for name,path in
                    [('input',source),('right',right)] + [(name,directory/(str(language)+'-'+name+'.ndjson')) for name in ['yes','no','joined','groups']]],
                controlFlow=dict(tasks=[dict(id=flow['id'], kind='dataFlow', config=dict(pipeline=flow['id'])) for flow in flows], constraints=[]), dataFlows=flows)
            definition = directory / 'package.json'; definition.write_text(json.dumps(package,ensure_ascii=False),encoding='utf-8')
            command = [cli,'execute',str(definition),'{}','fixture','same-id',str(root)]
            if language:
                params = directory / 'parameters.json'; params.write_text('{}')
                if language == 1:
                    command = [sys.argv[2],sys.argv[3],str(definition),str(params),'same-id',str(root)]
                else:
                    command = [sys.argv[4],'-Djava.library.path='+str(pathlib.Path(sys.argv[7]).resolve()),'-cp',sys.argv[5]+os.pathsep+sys.argv[6],'Consumer',str(definition),str(params),'same-id',str(root)]
            result = subprocess.run(command,capture_output=True,text=True,timeout=30)
            assert result.returncode == 0, (metadata['type'],result.stdout,result.stderr)
            metrics = json.loads(result.stdout); assert metrics['state'] == 5 and metrics['completedTasks'] == 3
            read = lambda name: [json.loads(line) for line in (directory/(str(language)+'-'+name+'.ndjson')).read_text(encoding='utf-8').splitlines()]
            yes,no = read('yes'),read('no')
            assert [r['id'] for r in yes] == [r['id'] for r in left_rows if r['key'] is not None]
            assert [r['id'] for r in no] == [r['id'] for r in left_rows if r['key'] is None]
            for row in yes+no:
                assert semantic_key(metadata,row['copy']) == semantic_key(metadata,values[row['id']])
                assert row['same'] is (None if row['key'] is None else True)
            expected = collections.Counter((l['id'],r['rid']) for l in left_rows for r in right_rows
                if l['key'] is not None and r['rkey'] is not None and semantic_key(metadata,l['key']) == semantic_key(metadata,r['rkey']))
            assert collections.Counter((r['id'],r['rid']) for r in read('joined')) == expected, metadata['type']
            expected_groups = collections.Counter(semantic_key(metadata,key) for key in values)
            actual_groups = {semantic_key(metadata,row['key']): row['cnt'] for row in read('groups')}
            assert actual_groups == expected_groups, metadata['type']
    print(str(languages)+' native language consumers; Package Pipeline: all 12 types, NULL/NaN/zeros/Decimal/UTC/NUL/nested Derived metadata + comparison + Split + hash Join + Aggregate passed')

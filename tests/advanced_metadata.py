"""Native template/Catalog/Plan and ten real tables, optionally across SDKs."""
import argparse
import copy
import json
import os
import pathlib
import sqlite3
import subprocess
import tempfile
from contextlib import closing

ROOT = pathlib.Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser()
p.add_argument('--query', required=True)
p.add_argument('--sqlite', required=True)
for name in ('dotnet', 'data-dll', 'java', 'classes', 'jar', 'native'):
    p.add_argument('--' + name)
a = p.parse_args()
commands = [[a.query]]
if a.dotnet:
    assert all((a.data_dll, a.java, a.classes, a.jar, a.native))
    commands += [[a.dotnet, a.data_dll], [a.java, '-Djava.library.path=' + a.native,
                 '-cp', a.classes + os.pathsep + a.jar, 'DataConsumer']]

def invoke(index, mode, package, output, succeed=True):
    env = dict(os.environ)
    if a.native and index != 1:
        if os.name == 'nt':
            env['PATH'] = a.native + os.pathsep + env.get('PATH', '')
        else:
            env['LD_LIBRARY_PATH'] = a.native
    if index == 1:
        env.pop('LD_LIBRARY_PATH', None)
    result = subprocess.run(commands[index] + [mode, str(package), str(output)],
                            env=env, capture_output=True, text=True, timeout=60)
    assert (result.returncode == 0) == succeed, (index, mode, result.returncode, result.stderr)
    return json.loads(output.read_text()) if succeed else None

with tempfile.TemporaryDirectory(prefix='ud-advanced-metadata-') as folder:
    root = pathlib.Path(folder)
    database = root / 'ten.db'
    doc = json.loads((ROOT / 'examples/templates/ten-table-etl.json').read_text())
    doc['connections'][0].update(driver=str(pathlib.Path(a.sqlite).resolve()),
                                 resource=str(database), credentialReference='fixture')
    doc.pop('parameters')
    with closing(sqlite3.connect(database)) as db, db:
        for i in range(10):
            # Explicit TEXT storage proves lossless Decimal parsing in the
            # parameterized cursor; no SQLite NUMERIC affinity is involved.
            db.execute(f'CREATE TABLE table_{i:02d}(key_{i} INTEGER NOT NULL PRIMARY KEY,amount_{i} TEXT)')
            db.executemany(f'INSERT INTO table_{i:02d} VALUES(?,?)',
                           [(i * 10 + j, None if j == 1 else '90071992547409.93') for j in range(3)])
    package = root / 'template.json'
    expected_catalog = expected_expansion = expected_plan = None
    for index in range(len(commands)):
        for i in range(10):
            doc['connections'][i + 1]['resource'] = str(root / f'{index}-{i:02d}.ndjson')
        package.write_text(json.dumps(doc))
        expansion = invoke(index, '--instantiate', package, root / f'{index}.expanded.json')
        assert 'templates' not in expansion and 'instances' not in expansion
        assert len(expansion['dataFlows']) == len(expansion['controlFlow']['tasks']) == 10
        # Output resource paths intentionally differ; everything else must be
        # the exact same Native expansion.
        stable = copy.deepcopy(expansion)
        for i, connection in enumerate(stable['connections'][1:]):
            connection['resource'] = f'output-{i}'
        if expected_expansion is None:
            expected_expansion = stable
        else:
            assert stable == expected_expansion
        catalog = invoke(index, '--catalog', package, root / f'{index}.catalog.json')
        if expected_catalog is None:
            expected_catalog = catalog
        else:
            assert catalog == expected_catalog
        plan = invoke(index, '--explain', package, root / f'{index}.plan.json')
        if expected_plan is None:
            expected_plan = plan
        else:
            assert plan == expected_plan
        diagnostics = invoke(index, '--execute', package, root / f'{index}.diagnostics.json')
        assert diagnostics['outcome'] == 0 and len(diagnostics['pipelines']) == 10
        for i in range(10):
            rows = [json.loads(line) for line in (root / f'{index}-{i:02d}.ndjson').read_text().splitlines()]
            assert rows == [dict(**{f'key_{i}':i * 10 + j, f'amount_{i}':None if j == 1 else '90071992547409.93'},
                                canonicalKey=i * 10 + j, canonicalAmount=None if j == 1 else '90071992547409.93') for j in range(3)]
        measured = [node for pipeline in diagnostics['pipelines'] for tree in pipeline['trees']
                    for node in tree['nodes'] if node.get('batchLatency', {}).get('samples')]
        assert measured and all(n['batchLatency']['p95UpperNs'] > 0 for n in measured)
    # Binding rejects a missing required template value and a duplicate instance
    # ID before any output resource can be opened.
    bad = copy.deepcopy(doc)
    del bad['instances'][0]['parameters']['mapping']
    package.write_text(json.dumps(bad))
    for index in range(len(commands)):
        invoke(index, '--instantiate', package, root / f'bad-{index}.json', False)
    bad = copy.deepcopy(doc)
    bad['instances'][1]['parameters']['pipeline'] = bad['instances'][0]['parameters']['pipeline']
    package.write_text(json.dumps(bad))
    for index in range(len(commands)):
        invoke(index, '--instantiate', package, root / f'duplicate-{index}.json', False)
print('Native Catalog, fixed Plan, typed 10-table template, exact Decimal/NULL, invalid binding and SDK parity passed')

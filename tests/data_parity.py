"""C/.NET8/Java17 typed-data and DAG contract fixture, not full execution acceptance.

Arguments: native runner, dotnet, consumer.dll, java, classes directory, SDK jar.
SDK code must be complete before this script is executed.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

native, dotnet, consumer, java, classes, jar = map(lambda s: str(pathlib.Path(s).resolve()), sys.argv[1:])
schema = {'version': 2, 'fields': [
    {'name': 'id', 'type': 'int64', 'nullable': False},
    {'name': 'amount', 'type': 'decimal128', 'precision': 38, 'scale': 2, 'nullable': False},
    {'name': 'at', 'type': 'timestamp', 'timezone': 'Asia/Shanghai', 'nullable': False},
    {'name': 'raw', 'type': 'binary', 'nullable': False},
    {'name': 'profile', 'type': 'struct', 'nullable': False, 'children': [
        {'name': 'name', 'type': 'utf8', 'nullable': False}, {'name': 'missing', 'type': 'int64', 'nullable': True}]},
    {'name': 'tags', 'type': 'list', 'nullable': False, 'children': [
        {'name': 'item', 'type': 'utf8', 'nullable': True}]}]}
rows = [{'id': 9223372036854775807, 'amount': '123456789012345678901234567890123456.78',
         'at': '2024-02-29T08:00:00+08:00', 'raw': 'AP9B',
         'profile': {'name': '中文\0'}, 'tags': ['', None, '中文']}]
graph = {'version': 2, 'nodes': [
    {'id': 'read', 'kind': 'source', 'operation': 'json.source', 'config': {'path': {'$parameter': 'path'}}},
    {'id': 'write', 'kind': 'sink', 'operation': 'sqlite.sink', 'config': {}}],
    'edges': [{'from': 'read', 'to': 'write', 'port': 0}],
    'parameters': {'path': {'kind': 'string', 'required': True}}}
with tempfile.TemporaryDirectory(prefix='ud-data-parity-') as folder:
    root = pathlib.Path(folder)
    inputs = []
    for name, content in [('schema', schema), ('data', rows), ('dag', graph), ('parameters', {'path': '中文"/input.ndjson'})]:
        path = root / (name + '.json');path.write_text(json.dumps(content, ensure_ascii=False), encoding='utf-8');inputs.append(str(path))
    import os
    commands = [[native], [dotnet, consumer], [java, '-cp', classes + os.pathsep + jar, 'DataConsumer']]
    for index, command in enumerate(commands):
        subprocess.run(command + inputs + [str(root / f'{index}.json'), str(root / f'{index}.wire')], check=True)
    for suffix in ('json', 'wire', 'json.dag'):
        expected = (root / f'0.{suffix}').read_bytes()
        for index in (1, 2):
            if expected != (root / f'{index}.{suffix}').read_bytes():
                raise AssertionError(f'Native/managed {suffix} differs for language {index}')
    actual = json.loads((root / '0.json').read_text(encoding='utf-8'))
    assert actual[0]['id'] == rows[0]['id'] and actual[0]['amount'] == rows[0]['amount']
    assert actual[0]['at'] == '2024-02-29T00:00:00.000000Z' and actual[0]['profile']['missing'] is None
print('Typed data and DAG contract parity fixture passed; generic execution parity is still a separate mandatory gate')

"""Real typed spill operator fixtures, reserved for the unified acceptance run."""
import collections
import decimal
import json
import pathlib
import subprocess
import sys
import tempfile

runner = str(pathlib.Path(sys.argv[1]).resolve())
def write_rows(path, rows):
    with path.open('w', encoding='utf-8', newline='\n') as file:
        for row in rows: file.write(json.dumps(row, ensure_ascii=False) + '\n')
def run(root, operation, source, output, spill, right, left_schema, right_schema, mode='normal', expected=0):
    result = subprocess.run([runner, operation, str(source), str(output), str(spill),
                             str(right), str(left_schema), str(right_schema), mode], timeout=240)
    assert result.returncode == expected, (operation, mode, result.returncode)
    assert not list(spill.glob('*.ndjson')), 'invocation-owned spills were not cleaned'
    if expected: assert not output.exists(), 'failed operator published a dataset'

with tempfile.TemporaryDirectory(prefix='ud-external-') as folder:
    root = pathlib.Path(folder);spill = root / 'spill';spill.mkdir()
    source = root / 'aggregate.ndjson';output = root / 'groups.ndjson'
    rows = [{'k': i % 25000, 'v': None if i % 13 == 0 else '1.25'} for i in range(50000)]
    rows += [{'k': None, 'v': None}, {'k': None, 'v': None}, {'k': None, 'v': '1.25'}]
    write_rows(source, rows)
    run(root, 'aggregate', source, output, spill, source, source, source)
    expected = {}
    for row in rows:
        group = expected.setdefault(row['k'], {'cnt': 0, 'total': None});group['cnt'] += 1
        if row['v'] is not None: group['total'] = (group['total'] or decimal.Decimal(0)) + decimal.Decimal(row['v'])
    actual = {}
    for line in output.read_text(encoding='utf-8').splitlines():
        row = json.loads(line);assert row['k'] not in actual
        actual[row['k']] = {'cnt': row['cnt'], 'total': None if row['total'] is None else decimal.Decimal(row['total'])}
    assert actual == expected
    run(root, 'aggregate', source, root / 'quota.ndjson', spill, source, source, source, 'quota', 4)
    run(root, 'aggregate', source, root / 'cancel.ndjson', spill, source, source, source, 'cancel', 8)
    overflow = root / 'overflow.ndjson';max_value = '9' * 36 + '.99'
    write_rows(overflow, [{'k': 1, 'v': max_value}, {'k': 1, 'v': max_value}])
    run(root, 'aggregate', overflow, root / 'overflow-out.ndjson', spill, source, source, source, 'normal', 7)
    left = root / 'left.ndjson';right = root / 'right.ndjson';joined = root / 'joined.ndjson'
    left_rows = [{'lk': 0 if i < 20 else i % 7 + 1, 'left_id': i, 'tags': ['中文', None, str(i)]} for i in range(80)]
    right_rows = [{'rk': 0 if i < 25 else i % 7 + 1, 'right_id': i, 'amount': '2.50'} for i in range(90)]
    left_rows.append({'lk': None, 'left_id': 999, 'tags': []});right_rows.append({'rk': None, 'right_id': 999, 'amount': '2.50'})
    write_rows(left, left_rows);write_rows(right, right_rows)
    schemas = [
        {'version': 2, 'fields': [{'name': 'lk', 'type': 'int64', 'nullable': True},
            {'name': 'left_id', 'type': 'int64', 'nullable': False},
            {'name': 'tags', 'type': 'list', 'nullable': False, 'children': [{'name': 'item', 'type': 'utf8', 'nullable': True}]}]},
        {'version': 2, 'fields': [{'name': 'rk', 'type': 'int64', 'nullable': True},
            {'name': 'right_id', 'type': 'int64', 'nullable': False},
            {'name': 'amount', 'type': 'decimal128', 'precision': 38, 'scale': 2, 'nullable': False}]}]
    paths = [root / 'left-schema.json', root / 'right-schema.json']
    for path, schema in zip(paths, schemas): path.write_text(json.dumps(schema), encoding='utf-8')
    run(root, 'join', left, joined, spill, right, *paths)
    actual_rows = [json.loads(line) for line in joined.read_text(encoding='utf-8').splitlines()]
    pairs = collections.Counter((r['left_id'], r['right_id']) for r in actual_rows)
    expected_pairs = collections.Counter((l['left_id'], r['right_id']) for l in left_rows for r in right_rows if l['lk'] is not None and l['lk'] == r['rk'])
    assert pairs == expected_pairs
    for row in actual_rows:
        assert row['tags'] == left_rows[row['left_id']]['tags'] and row['amount'] == '2.50'
    run(root, 'join', left, root / 'join-quota.ndjson', spill, right, *paths, 'quota', 4)
print('typed external aggregation/join: adaptive quota, exact Decimal/NULL/nested/skew, cancellation and spill quota fixtures passed')

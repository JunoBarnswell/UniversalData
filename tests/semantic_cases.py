"""Typed fixtures and independent expected equality for real operator tests."""
import base64
import datetime
import decimal

def field(name, kind, **metadata):
    return dict(name=name, type=kind, nullable=True, **metadata)

CASES = [
    (field('key', 'bool'), [True, False, True, None]),
    (field('key', 'int64'), [9223372036854775807, -9223372036854775808, 1, 1, None]),
    (field('key', 'float64'), [-0.0, 0.0, 'NaN', 'NaN', 'Infinity', '-Infinity', 3.0, None]),
    (field('key', 'utf8'), ['中文\0a', '中文\0a', '', '中文\0b', None]),
    (field('key', 'decimal128', precision=38, scale=2),
     ['123456789012345678901234567890123456.78', '-0.00', '0.00', '2.50', '2.5', None]),
    (field('key', 'date32'), ['1969-12-31', '2000-02-29', '1969-12-31', None]),
    (field('key', 'time64'), ['23:59:59.999999', '00:00:00', '00:00:00.000000', None]),
    (field('key', 'timestamp', timezone='Asia/Shanghai'),
     ['2024-02-29T08:00:00+08:00', '2024-02-29T00:00:00Z', '1969-12-31T23:59:59.999999Z', None]),
    (field('key', 'binary'), ['AP9B', 'AP9B', 'AP9C', '', None]),
    (field('key', 'struct', children=[field('name', 'utf8'), field('number', 'float64')]),
     [{'name': '中文\0', 'number': 'NaN'}, {'name': '中文\0', 'number': 'NaN'},
      {'name': '', 'number': -0.0}, {'name': '', 'number': 0.0}, {'name': None, 'number': 0.0}, None]),
    (field('key', 'list', children=[field('item', 'float64')]),
     [['NaN', None, -0.0], ['NaN', None, 0.0], [], [None], None]),
    (field('key', 'map', children=[dict(name='entry', type='struct', nullable=False,
         children=[dict(name='key', type='utf8', nullable=False), field('value', 'float64')])]),
     [[{'key': '中文\0', 'value': 'NaN'}], [{'key': '中文\0', 'value': 'NaN'}],
      [], [{'key': '中文\0', 'value': None}], None]),
]

def semantic_key(schema, value):
    if value is None:
        return None
    kind = schema['type']
    if kind == 'decimal128':
        return decimal.Decimal(value)
    if kind == 'timestamp':
        return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(datetime.timezone.utc)
    if kind == 'time64':
        return datetime.time.fromisoformat(value)
    if kind == 'binary':
        return base64.b64decode(value, validate=True)
    if kind == 'struct':
        return tuple(semantic_key(child, value.get(child['name'])) for child in schema['children'])
    if kind in ['list', 'map']:
        return tuple(semantic_key(schema['children'][0], item) for item in value)
    return value

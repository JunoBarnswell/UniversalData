"""Apache Avro -> Native Package -> Apache Avro, including reader resolution."""
import copy
import ctypes
import datetime
import decimal
import json
import pathlib
import subprocess
import sys
import tempfile
import zlib

import avro.datafile
import avro.io
import avro.schema

runner, library = sys.argv[1:3]
native = ctypes.CDLL(library)

class String(ctypes.Structure):
    _fields_ = [('data', ctypes.c_char_p), ('length', ctypes.c_uint32)]

native.ud_context_create.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
native.ud_avro_discover.argtypes = [ctypes.c_void_p, ctypes.c_char_p, String, ctypes.POINTER(ctypes.c_void_p)]
native.ud_schema_to_json.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_uint32)]
native.ud_batch_destroy.argtypes = [ctypes.c_void_p]
native.ud_free.argtypes = [ctypes.c_void_p]
native.ud_context_destroy.argtypes = [ctypes.c_void_p]

def discover(path, reject=False):
    context, schema, text = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
    size = ctypes.c_uint32()
    assert native.ud_context_create(None, ctypes.byref(context)) == 0
    try:
        status = native.ud_avro_discover(context, str(path).encode(), String(b'avro-test', 9), ctypes.byref(schema))
        if reject:
            assert status != 0, 'Non-finite or unresolved schema was silently accepted'
            return
        assert status == 0, status
        assert native.ud_schema_to_json(schema, ctypes.byref(text), ctypes.byref(size)) == 0
        return json.loads(ctypes.string_at(text, size.value))
    finally:
        native.ud_free(text)
        native.ud_batch_destroy(schema)
        assert native.ud_context_destroy(context) == 0

def write(path, schema, rows, codec='null'):
    with avro.datafile.DataFileWriter(path.open('wb'), avro.io.DatumWriter(), avro.schema.parse(json.dumps(schema)), codec=codec) as writer:
        for row in rows:
            writer.append(row)

def read(path, reader_schema=None):
    reader = avro.io.DatumReader(readers_schema=avro.schema.parse(json.dumps(reader_schema)) if reader_schema else None)
    with avro.datafile.DataFileReader(path.open('rb'), reader) as stream:
        return list(stream)

with tempfile.TemporaryDirectory(prefix='ud-apache-avro-') as directory:
    root = pathlib.Path(directory)
    writer_schema = {'type': 'record', 'name': 'Events', 'namespace': 'enterprise', 'fields': [
        {'name': 'id', 'type': 'long'},
        {'name': 'nested', 'type': {'type': 'record', 'name': 'Detail', 'fields': [{'name': 'value', 'type': 'string'}]}},
        {'name': 'reference', 'type': 'Detail'},
        {'name': 'choice', 'type': ['null', 'long', 'string', 'Detail']},
        {'name': 'colour', 'type': {'type': 'enum', 'name': 'Colour', 'symbols': ['RED', 'GREEN', 'BLUE']}},
        {'name': 'fingerprint', 'type': {'type': 'fixed', 'name': 'Fingerprint', 'size': 4}},
        {'name': 'otherFingerprint', 'type': 'Fingerprint'},
        {'name': 'values', 'type': {'type': 'array', 'items': ['null', 'string']}},
        {'name': 'properties', 'type': {'type': 'map', 'values': 'long'}},
        {'name': 'amount', 'type': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 18, 'scale': 2}},
        {'name': 'time', 'type': {'type': 'long', 'logicalType': 'timestamp-micros'}}]}
    rows = [dict(id=-(2**63) if index == 0 else 2**63 - 1, nested={'value': '中文'}, reference={'value': 'reused'},
        choice=choice, colour=['RED', 'GREEN', 'BLUE', 'RED'][index], fingerprint=b'\x00\x01\xfe\xff', otherFingerprint=b'abcd',
        values=[None, '', '文字'], properties={'negative': -17, 'zero': 0}, amount=decimal.Decimal('-1234567890.25'),
        time=datetime.datetime(2025, 2, 3, 4, 5, 6, 123456, tzinfo=datetime.timezone.utc))
        for index, choice in enumerate([None, 4394967297, 'a union string', {'value': 'a union record'}])]

    def pipeline(name, source, native_schema, output_schema, reader_schema=None, status=0):
        target = root / (name + '.avro')
        config = dict(connection='input', schema=native_schema)
        if reader_schema is not None:
            config['readerSchema'] = reader_schema
        package = dict(version=12, id=name, connections=[dict(id='input', driver='file', resource=str(source)), dict(id='output', driver='file', resource=str(target))],
            controlFlow=dict(tasks=[dict(id='copy', kind='dataFlow', config=dict(pipeline='flow'))], constraints=[]),
            dataFlows=[dict(id='flow', components=[dict(id='source', component='file.avro.source', config=config),
                dict(id='sink', component='file.avro.sink', config=dict(connection='output', writerSchema=output_schema, codec='deflate', fileMode='overwrite'))],
                paths=[dict(_from='source')])])
        package['dataFlows'][0]['paths'] = [{'from': 'source', 'output': 0, 'to': 'sink', 'input': 0}]
        definition = root / (name + '.json')
        definition.write_text(json.dumps(package))
        result = subprocess.run([runner, str(definition), str(status)], text=True, capture_output=True, timeout=30)
        assert result.returncode == 0, result.stderr
        return target

    for codec in ['null']:
        source = root / ('source-' + codec + '.avro')
        write(source, writer_schema, rows, codec)
        expected = read(source)
        output = pipeline('roundtrip-' + codec, source, discover(source), writer_schema)
        assert read(output) == expected
        # Native emits standard raw RFC 1951 blocks; Apache independently reads
        # them, and the same file is then consumed by the Native Source.
        deflated = pipeline('native-deflate', output, discover(output), writer_schema)
        assert read(deflated) == expected

    # Apache Python 1.12.0 retains three Adler32 bytes after the raw deflate
    # stream (codecs.py:116). Keep the nonstandard producer as a rejection case;
    # accepting an arbitrary trailer would weaken framing validation.
    malformed = root / 'apache-python-deflate-trailer.avro'
    write(malformed, writer_schema, rows, 'deflate')
    compressed = avro.codecs.DeflateCodec.compress(b'independent-trailer-proof')[0]
    decoder = zlib.decompressobj(-15)
    assert decoder.decompress(compressed) == b'independent-trailer-proof' and len(decoder.unused_data) == 3
    pipeline('reject-deflate-trailer', malformed, discover(malformed), writer_schema, status=9)

    # The same immutable file is resolved by Apache and Native independently.
    reader_schema = copy.deepcopy(writer_schema)
    reader_schema['fields'] = [reader_schema['fields'][3], reader_schema['fields'][0], reader_schema['fields'][4], reader_schema['fields'][1]]
    reader_schema['fields'][1].update(name='eventId', aliases=['id'])
    reader_schema['fields'][2]['type'] = {'type': 'enum', 'name': 'RenamedColour', 'aliases': ['Colour'], 'symbols': ['RED', 'UNKNOWN'], 'default': 'UNKNOWN'}
    # Define Detail before its use in the union in the reader's declaration order.
    reader_schema['fields'] = [reader_schema['fields'][3], *reader_schema['fields'][:3]]
    reader_schema['fields'].extend([
        {'name': 'enabled', 'type': 'boolean', 'default': True},
        {'name': 'label', 'type': ['string', 'null'], 'default': 'a default'},
        {'name': 'missing', 'type': ['null', 'long'], 'default': None},
        {'name': 'tags', 'type': {'type': 'array', 'items': 'string'}, 'default': ['new']},
        {'name': 'settings', 'type': {'type': 'map', 'values': 'long'}, 'default': {'limit': 9}}])
    empty = root / 'reader-shape.avro'
    write(empty, reader_schema, [])
    output = pipeline('resolved', source, discover(empty), reader_schema, reader_schema)
    expected_resolved = [dict(nested=row['nested'], choice=row['choice'], eventId=row['id'],
        colour='RED' if row['colour'] == 'RED' else 'UNKNOWN', enabled=True, label='a default',
        missing=None, tags=['new'], settings={'limit': 9}) for row in read(source)]
    assert read(output) == expected_resolved

    # Unbounded recursive values have no finite Native field shape and reject
    # before any records are consumed or any destination is published.
    recursive = {'type': 'record', 'name': 'Node', 'fields': [{'name': 'next', 'type': ['null', 'Node'], 'default': None}]}
    invalid = root / 'recursive.avro'
    write(invalid, recursive, [])
    discover(invalid, reject=True)
print('Apache Avro 1.12: named references, tagged unions, records, enum/fixed, arrays/maps, decimals/timestamps, aliases/defaults and reader resolution passed')

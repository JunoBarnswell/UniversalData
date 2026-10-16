"""Independent Avro OCF fixtures; Native Source/Destination interoperability."""
import json
import pathlib
import struct
import subprocess
import sys
import tempfile
import zlib

runner = pathlib.Path(sys.argv[1]).resolve()

def integer(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    return bytes(out + bytes([value]))

def binary(value):
    return integer(len(value)) + value

def read_integer(stream):
    value = 0
    for at in range(10):
        byte = stream.read(1)
        if not byte:
            raise ValueError("truncated Avro integer")
        part = byte[0]
        value |= (part & 127) << (at * 7)
        if not part & 128:
            return (value >> 1) ^ -(value & 1)
    raise ValueError("invalid Avro integer")

def read_binary(stream):
    count = read_integer(stream)
    if count < 0:
        raise ValueError("negative length")
    value = stream.read(count)
    if len(value) != count:
        raise ValueError("truncated Avro bytes")
    return value

def object_file(path, schema, values):
    sync = b"0123456789abcdef"
    metadata = binary(b"avro.schema") + binary(json.dumps(schema).encode()) + binary(b"avro.codec") + binary(b"null")
    path.write_bytes(b"Obj\1" + integer(2) + metadata + integer(0) + sync + integer(len(values)) + binary(b"".join(values)) + sync)

def read_object(path):
    import io
    stream = io.BytesIO(path.read_bytes())
    assert stream.read(4) == b"Obj\1"
    metadata = {}
    while count := read_integer(stream):
        assert count > 0
        for _ in range(count):
            key = read_binary(stream)
            metadata[key] = read_binary(stream)
    sync = stream.read(16)
    values = []
    while stream.tell() < len(stream.getvalue()):
        count = read_integer(stream)
        data = read_binary(stream)
        assert stream.read(16) == sync
        if metadata.get(b"avro.codec") == b"deflate":
            data = zlib.decompress(data, -15)
        block = io.BytesIO(data)
        for _ in range(count):
            values.append((read_integer(block), read_binary(block).decode()))
        assert block.read() == b""
    return metadata, values

with tempfile.TemporaryDirectory(prefix="ud-avro-") as root:
    root = pathlib.Path(root)
    source, target = root / "source.avro", root / "target.avro"
    standard = {"type": "record", "name": "rows", "fields": [{"name": "id", "type": "long"}, {"name": "text", "type": "string"}]}
    values = [(-9223372036854775808, "中文"), (9223372036854775807, "line\nquoted"), (0, "")]
    object_file(source, standard, [integer(key) + binary(text.encode()) for key, text in values])
    schema = {"version": 12, "fields": [{"name": "id", "type": "int64", "nullable": False}, {"name": "text", "type": "utf8", "nullable": False}]}
    for codec in ("null", "deflate"):
        package = {"version": 12, "id": "avro-interop", "connections": [{"id": "input", "driver": "file", "resource": str(source)}, {"id": "output", "driver": "file", "resource": str(target)}],
                   "controlFlow": {"tasks": [{"id": "copy", "kind": "dataFlow", "config": {"pipeline": "flow"}}], "constraints": []},
                   "dataFlows": [{"id": "flow", "components": [{"id": "source", "component": "file.avro.source", "config": {"connection": "input", "schema": schema}}, {"id": "sink", "component": "file.avro.sink", "config": {"connection": "output", "codec": codec, "fileMode": "overwrite"}}], "paths": [{"from": "source", "output": 0, "to": "sink", "input": 0}]}]}
        definition = root / "package.json"
        definition.write_text(json.dumps(package))
        subprocess.run([str(runner), str(definition), "0"], check=True)
        metadata, actual = read_object(target)
        assert actual == values and json.loads(metadata[b"ud.schema"])["version"] == 12
    # Invalid framing cannot become an isolated row conversion failure.
    source.write_bytes(source.read_bytes()[:-1])
    subprocess.run([str(runner), str(definition), "9"], check=True)

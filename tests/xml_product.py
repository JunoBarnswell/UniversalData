"""Real Native XML behavior; no managed parser or substitute component."""
import base64
import json
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-xml-product-") as folder:
    root = pathlib.Path(folder)
    def run(doc, status=0):
        package = root / "package.json"
        package.write_text(json.dumps(doc), encoding="utf-8")
        result = subprocess.run([sys.argv[1], str(package), str(status)], capture_output=True, text=True, timeout=40)
        assert result.returncode == 0, (result.stdout, result.stderr)
    def document(connections, tasks, flows=None):
        return dict(version=10, id="xml-product", connections=[dict(id=k, driver="file", resource=str(v)) for k,v in connections.items()],
                    controlFlow=dict(tasks=tasks, constraints=[]), dataFlows=flows or [])
    source = root / "source.xml"
    raw = b'<r xmlns="urn:records"><row><id>18446744073709551615</id><text>first</text></row><row><id>bad</id><text>error</text></row><row><id>2</id><text>last</text></row></r>'
    source.write_bytes(raw)
    good = root / "good.ndjson"; errors = root / "errors.ndjson"
    fields = [dict(name="id", type="uint64", nullable=False), dict(name="text", type="utf8", nullable=False)]
    flow = dict(id="xml", components=[
        dict(id="source", component="file.xml.source", config=dict(connection="source", schema=dict(version=10, fields=fields),
            recordPath="/r/row", fields=dict(id="n:id", text="n:text"), namespaces=dict(n="urn:records"), errorPolicy="redirect", includeInput=True)),
        dict(id="good", component="file.ndjson.destination", config=dict(connection="good")),
        dict(id="error", component="file.ndjson.destination", config=dict(connection="error"))],
        paths=[dict(**{"from":"source"}, output=i, to=k, input=0) for i,k in enumerate(("good","error"))])
    doc = document(dict(source=source, good=good, error=errors), [dict(id="flow", kind="dataFlow", config=dict(pipeline="xml"))], [flow])
    run(doc)
    rows = [json.loads(line) for line in good.read_text().splitlines()]
    assert [int(row["id"]) for row in rows] == [18446744073709551615, 2]
    rejected = [json.loads(line) for line in errors.read_text().splitlines()]
    assert len(rejected) == 1
    retained = next(value for value in rejected[0].values() if isinstance(value, str) and base64.b64encode(b'<row><id>bad</id><text>error</text></row>').decode() == value)
    assert base64.b64decode(retained) == b'<row><id>bad</id><text>error</text></row>'
    # Opening tags crossing Expat input blocks must retain exact original bytes.
    source.write_bytes(b'<r xmlns="urn:records">' + b' ' * 16340 + b'<row a="' + b'x' * 40 + b'"><id>7</id><text>boundary</text></row></r>')
    for path in (good,errors): path.unlink()
    run(doc)
    assert int(json.loads(good.read_text())["id"]) == 7
    source.write_text('<r><row><id>1</id><text>unclosed</text>', encoding="utf-8")
    for path in (good,errors): path.unlink()
    run(doc, 6)
    source.write_text('<!DOCTYPE r [<!ENTITY unsafe SYSTEM "file:///never-read">]><r>&unsafe;</r>', encoding="utf-8")
    run(doc, 13)
    source.write_text('<r><row><id>7</id></row></r>', encoding="utf-8")
    output = root / "xpath.json"
    run(document(dict(source=source, target=output), [dict(id="xpath", kind="xml.xpath", config=dict(source="source", expression="count(/r/row)", target="target"))]))
    assert json.loads(output.read_text()) == 1
    style = root / "style.xsl"
    style.write_text('<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"><xsl:output method="text" encoding="UTF-8"/><xsl:template match="/">value=<xsl:value-of select="/r/row/id"/></xsl:template></xsl:stylesheet>', encoding="utf-8")
    target = root / "transformed.txt"
    run(document(dict(source=source, auxiliary=style, target=target), [dict(id="xslt", kind="xml.xslt", config=dict(source="source", auxiliary="auxiliary", target="target"))]))
    assert target.read_text() == "value=7"
    schema = root / "schema.xsd"
    schema.write_text('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="r"><xs:complexType><xs:sequence><xs:element name="row"><xs:complexType><xs:sequence><xs:element name="id" type="xs:unsignedLong"/></xs:sequence></xs:complexType></xs:element></xs:sequence></xs:complexType></xs:element></xs:schema>', encoding="utf-8")
    xsd = document(dict(source=source, auxiliary=schema), [dict(id="xsd", kind="xml.xsd", config=dict(source="source", auxiliary="auxiliary"))])
    run(xsd)
    source.write_text('<r><row><id>-1</id></row></r>', encoding="utf-8"); run(xsd, 14)
    schema.write_text('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:include schemaLocation="file:///never-read"/></xs:schema>', encoding="utf-8")
    run(xsd, 13)

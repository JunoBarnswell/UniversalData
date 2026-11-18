"""Append preserves OOXML parts, original formulas and unrelated worksheets."""
import json
import pathlib
import subprocess
import sys
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from product_version import PROTOCOL_VERSION

with tempfile.TemporaryDirectory(prefix="ud-xlsx-append-") as folder:
    root = pathlib.Path(folder)
    source, target = root / "rows.ndjson", root / "book.xlsx"
    source.write_text('{"id":18446744073709551615,"label":"新增"}\n')
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    parts = {
        "[Content_Types].xml": '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
        "_rels/.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        "xl/workbook.xml": f'<workbook xmlns="{ns}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="r1"/><sheet name="Keep" sheetId="2" r:id="r2"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="r2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": f'<worksheet xmlns="{ns}"><dimension ref="A1:B2"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>label</t></is></c></row><row r="2"><c r="A2"><f>1+1</f><v>2</v></c><c r="B2" t="inlineStr"><is><t>保留</t></is></c></row></sheetData></worksheet>',
        "xl/worksheets/sheet2.xml": f'<worksheet xmlns="{ns}"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>untouched</t></is></c></row></sheetData></worksheet>',
        "docProps/custom.xml": '<custom>preserved metadata</custom>',
    }
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as workbook:
        for name, content in parts.items():
            workbook.writestr(name, content.encode())
    schema = {"version": PROTOCOL_VERSION, "fields": [{"name": "id", "type": "uint64", "nullable": False}, {"name": "label", "type": "utf8", "nullable": False}]}
    doc = {"version": PROTOCOL_VERSION, "id": "xlsx-preserving-append", "connections": [{"id": "input", "driver": "file", "resource": str(source)}, {"id": "output", "driver": "file", "resource": str(target)}],
        "controlFlow": {"tasks": [{"id": "copy", "kind": "dataFlow", "config": {"pipeline": "rows"}}], "constraints": []}, "dataFlows": [{"id": "rows", "components": [
            {"id": "read", "component": "file.ndjson.source", "config": {"connection": "input", "schema": schema}},
            {"id": "write", "component": "file.xlsx.destination", "config": {"connection": "output", "fileMode": "append", "sheet": "Data", "header": True}}],
            "paths": [{"from": "read", "output": 0, "to": "write", "input": 0}]}]}
    definition = root / "package.json"
    definition.write_text(json.dumps(doc))
    subprocess.run([sys.argv[1], str(definition), "0"], check=True)
    with zipfile.ZipFile(target) as workbook:
        for name, content in parts.items():
            if name != "xl/worksheets/sheet1.xml":
                assert workbook.read(name) == content.encode(), name
        actual = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        rows = actual.findall(f"{{{ns}}}sheetData/{{{ns}}}row")
        assert len(rows) == 3 and rows[1].find(f"{{{ns}}}c/{{{ns}}}f").text == "1+1"
        assert rows[2].find(f"{{{ns}}}c/{{{ns}}}is/{{{ns}}}t").text == "18446744073709551615"
    # Positional compatibility alone cannot prove that an existing header still
    # identifies the configured columns. Refuse mismatches before publication.
    parts["xl/worksheets/sheet1.xml"] = parts["xl/worksheets/sheet1.xml"].replace(">label<", ">renamed<")
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as workbook:
        for name, content in parts.items(): workbook.writestr(name, content.encode())
    before = target.read_bytes()
    subprocess.run([sys.argv[1], str(definition), "10"], check=True)
    assert target.read_bytes() == before

"""Exercises the authenticated analysis product over its real Management and Native Worker."""
import argparse
import base64
import csv
from decimal import Decimal
import hashlib
import http.cookiejar
import io
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPCookieProcessor, ProxyHandler, Request, build_opener
import uuid
import zipfile


class Session:
    def __init__(self, address, subject, password):
        self.address = address
        self.client = build_opener(ProxyHandler({}), HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.csrf = ""
        self.csrf = self.call("/api/auth/login", {"subject": subject, "password": password})["csrf"]

    def call(self, path, data=None, method=None, headers=None, binary=False):
        payload = data if isinstance(data, bytes) else json.dumps(data).encode() if data is not None else None
        request = Request(self.address + path, payload, {
            "X-UD-CSRF": self.csrf, "Content-Type": "application/json", **(headers or {})
        }, method=method)
        try:
            with self.client.open(request, timeout=75) as response:
                body = response.read()
                return body if binary else json.loads(body)
        except HTTPError as error:
            raise AssertionError(f"{request.get_method()} {path}: {error.code} {error.read().decode()}") from error


def model_for(preview):
    model = preview["suggestion"]
    return {**model, "confirmLineAmount": True, "confirmSingleCurrency": True, "currency": "CNY"}


def draft(session, dataset, preview):
    path = "/api/analysis/datasets/" + dataset["id"]
    return session.call(path + "/draft", {
        "expectedRevision": dataset["revision"], "name": dataset["name"],
        "model": model_for(preview), "sheet": preview["sheet"]
    }, "PUT")


def publish(session, dataset):
    path = "/api/analysis/datasets/" + dataset["id"]
    operation = {
        "operationId": uuid.uuid4().hex, "expectedRevision": dataset["revision"],
        "expectedReleaseId": dataset["publishedReleaseId"]
    }
    receipt = session.call(path + "/publish", operation)
    assert session.call(path + "/operations/" + operation["operationId"]) == receipt
    assert session.call(path + "/publish", operation) == receipt
    return receipt


def workbook(formula=False):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", """<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
          <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
          <Default Extension="xml" ContentType="application/xml"/>
          <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
          <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        </Types>""")
        archive.writestr("_rels/.rels", """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
            <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
        </Relationships>""")
        archive.writestr("xl/workbook.xml", """<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheets><sheet name="订单" sheetId="1" r:id="rId1"/></sheets></workbook>""")
        archive.writestr("xl/_rels/workbook.xml.rels", """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
        </Relationships>""")
        value = "<f>50+50</f><v>100</v>" if formula else "<v>100</v>"
        archive.writestr("xl/worksheets/sheet1.xml", f"""<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
          <sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>订单号</t></is></c>
          <c r="B1" t="inlineStr"><is><t>实付金额</t></is></c></row>
          <row r="2"><c r="A2" t="inlineStr"><is><t>0008</t></is></c><c r="B2">{value}</c></row>
          </sheetData></worksheet>""")
    return output.getvalue()


def export_rows(session, path, context, details=False, cell=None):
    data = session.call(path + "/export", {"context": context, "details": details, "cell": cell}, binary=True)
    return list(csv.reader(io.StringIO(data.decode("utf-8-sig"))))


def exercise(session, other):
    root = "/api/analysis/datasets"
    dataset = session.call(root, {"sourceId": "sample"})
    path = root + "/" + dataset["id"]
    preview = session.call(path + "/preview")
    fields = {field["label"]: field["id"] for field in preview["fields"]}
    assert preview["rows"][0][fields["订单号"]] == "0001"
    published = publish(session, draft(session, dataset, preview))
    assert published["state"] == "published", published
    release = published["release"]["id"]

    query = {"releaseId": release, "dimensions": [
        {"fieldId": fields["地区"]}, {"fieldId": fields["商品类别"]}
    ]}
    result = session.call(path + "/query", query)
    total = result["total"]
    assert (Decimal(total["amount"]), int(total["orders"]), int(total["records"]), Decimal(total["average"])) == (450, 2, 3, 225), total
    by_category = {cell["keys"][0]: int(cell["values"]["orders"]) for cell in result["groups"][1]["cells"]}
    assert by_category == {"家具": 2, "用品": 1}, by_category
    details = session.call(path + "/drill", {"context": result["context"], "cell": ["华东", "家具"]})
    assert [item[fields["订单号"]] for item in details["rows"]] == ["0001"], details
    assert export_rows(session, path, result["context"])[-1][2] == "总计"
    assert export_rows(session, path, result["context"], True, ["华东", "家具"])[-1][0] == "0001"
    filtered = session.call(path + "/query", {**query, "filters": [
        {"fieldId": fields["下单日期"], "operator": "from", "value": "2026-11-02"},
        {"fieldId": fields["下单日期"], "operator": "before", "value": "2026-11-06"}
    ]})
    assert Decimal(filtered["total"]["amount"]) == 150
    empty = session.call(path + "/query", {**query, "filters": [
        {"fieldId": fields["地区"], "operator": "eq", "value": "不存在"}
    ]})
    assert empty["empty"] and empty["total"]["amount"] is None
    assert dataset["id"] in {item["id"] for item in session.call(root)}
    assert other.call(root) == []
    try:
        other.call(path + "/drill", {"context": result["context"]})
    except AssertionError as denied:
        assert ": 403 " in str(denied)
    else:
        raise AssertionError("Another subject could read the analysis context")

    # The invalid currency is beyond the preview. A rejected refresh keeps the old release.
    current = session.call(path)
    bad = ("订单号,下单日期,地区,商品类别,实付金额,币种\n" +
           "".join(f"{row:04},2026-11-01,华东,家具,10,CNY\n" for row in range(40)) +
           "0099,2026-11-01,华东,家具,10,USD\n").encode()
    refresh = session.call(root + f"/upload?datasetId={dataset['id']}&expectedRevision={current['revision']}",
                           bad, headers={"X-File-Name": "refresh.csv", "Content-Type": "application/octet-stream"})
    assert refresh["id"] == dataset["id"]
    refreshed_preview = session.call(path + "/preview")
    assert len(refreshed_preview["rows"]) == 32
    rejected = publish(session, draft(session, refresh, refreshed_preview))
    assert rejected["state"] == "rejected" and rejected["affectedRows"] == 1 and rejected["scannedRows"] == 41, rejected
    assert session.call(path)["publishedReleaseId"] == release
    assert session.call(path + "/query", query)["total"] == total

    current = session.call(path)
    corrected = session.call(root + f"/upload?datasetId={dataset['id']}&expectedRevision={current['revision']}",
                             bad.replace(b"10,USD", b"10,CNY"),
                             headers={"X-File-Name": "corrected.csv", "Content-Type": "application/octet-stream"})
    second = publish(session, draft(session, corrected, session.call(path + "/preview")))
    assert second["state"] == "published" and second["release"]["id"] != release, second
    assert Decimal(session.call(path + "/query", {**query, "releaseId": second["release"]["id"]})["total"]["amount"]) == 410
    assert session.call(path + "/query", query)["total"] == total
    assert session.call(path)["report"]["query"]["releaseId"] == release

    for formula in (False, True):
        book = session.call(root + "/upload", workbook(formula), headers={
            "X-File-Name": quote("工作表.xlsx"), "Content-Type": "application/octet-stream"})
        book_path = root + "/" + book["id"]
        if formula:
            try:
                session.call(book_path + "/preview")
            except AssertionError as rejected:
                assert ": 422 " in str(rejected)
            else:
                raise AssertionError("Excel cached formula could be read as published input")
        else:
            excel_preview = session.call(book_path + "/preview")
            excel = publish(session, draft(session, book, excel_preview))
            assert excel["state"] == "published", excel
            result = session.call(book_path + "/query", excel["release"]["startingReport"]["query"])
            assert Decimal(result["total"]["amount"]) == 100

    print("Analysis workspace passed CSV, Native Decimal, distinct totals, release isolation, Excel and permissions.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dotnet", required=True)
    parser.add_argument("--management", required=True, type=Path)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="analysis-", dir=args.work) as directory:
        private = Path(directory)
        identity = private / "identity.json"
        password = secrets.token_urlsafe(32)
        salt = os.urandom(32)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600000)
        encoded = "pbkdf2-sha256$600000$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()
        identity.write_text(json.dumps({
            "version": 1, "roles": {"operator": [{"permission": "*", "resource": "*"}]},
            "users": [{"id": user, "tenant": "bi", "passwordHash": encoded, "stamp": secrets.token_hex(32),
                       "roles": ["operator"]} for user in ("analyst", "other")],
        }))
        identity.chmod(0o600)
        policy = private / "policy.json"
        policy.write_text(json.dumps({
            "dataDirectory": str(private / "state"), "identityFile": str(identity),
            "fileRoots": [], "networkOrigins": [], "pluginRoots": [], "credentialFiles": {},
            "tenants": {"bi": {"fileRoots": [], "networkOrigins": [], "pluginRoots": [], "credentialReferences": []}},
        }))
        with socket.socket() as free:
            free.bind(("127.0.0.1", 0))
            address = "http://127.0.0.1:" + str(free.getsockname()[1])
        log = args.work / "analysis-host.log"
        with log.open("w") as output:
            host = subprocess.Popen([args.dotnet, str(args.management), "--worker", str(args.worker), "--policy",
                                     str(policy), "--urls", address], stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if host.poll() is not None:
                        raise RuntimeError(f"Management stopped. See {log}")
                    try:
                        build_opener(ProxyHandler({})).open(address + "/health/live", timeout=1).close()
                        break
                    except (HTTPError, URLError, TimeoutError):
                        time.sleep(0.1)
                else:
                    raise TimeoutError(f"Management unavailable. See {log}")
                exercise(Session(address, "analyst", password), Session(address, "other", password))
            finally:
                host.terminate()
                try:
                    host.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    host.kill()
                    host.wait()


if __name__ == "__main__":
    main()

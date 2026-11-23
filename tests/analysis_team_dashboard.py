"""Authenticated dashboard, exact totals, SQL refresh, restart and member revocation acceptance."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from contextlib import closing
from urllib.request import ProxyHandler, build_opener
from urllib.error import URLError
from uuid import uuid4

from analysis_workspace import Session, model_for, publish


def check_denied(action, code=403):
    try:
        action()
    except AssertionError as error:
        assert f": {code} " in str(error), error
    else:
        raise AssertionError(f"Expected HTTP {code}")


def verify(server, db, restart):
    author, reader, stranger = server()
    root = "/api/analysis/dashboards"
    dataset = author.call("/api/analysis/datasets", {"sourceId": "sales"})
    ds = "/api/analysis/datasets/" + dataset["id"]
    preview = author.call(ds + "/preview")
    ids = {field["label"]: field["id"] for field in preview["fields"]}
    metrics = [
        {"id": "sales_total", "label": "实收金额", "operation": "sum", "fieldId": ids["amount"], "unit": "CNY"},
        {"id": "unique_orders", "label": "独立订单", "operation": "distinct", "fieldId": ids["orderid"]},
        {"id": "lines", "label": "非空订单行", "operation": "count", "fieldId": ids["orderid"]},
        {"id": "twice", "label": "双倍金额", "operation": "add", "left": "sales_total", "right": "sales_total"},
        {"id": "difference", "label": "还原金额", "operation": "subtract", "left": "twice", "right": "sales_total"},
        {"id": "per_order", "label": "实收客单价", "operation": "divide", "left": "difference", "right": "unique_orders"},
        {"id": "zero", "label": "差额", "operation": "subtract", "left": "lines", "right": "lines"},
        {"id": "undefined_ratio", "label": "空比值", "operation": "divide", "left": "lines", "right": "zero"},
    ]
    model_request = {"expectedRevision": dataset["revision"], "name": dataset["name"],
                     "model": {**model_for(preview), "metrics": metrics}, "sheet": preview["sheet"]}
    invalid = [*metrics, {"id": "cycle", "label": "循环引用", "operation": "divide", "left": "cycle", "right": "lines"}]
    check_denied(lambda: author.call(ds + "/draft", {**model_request,
        "model": {**model_request["model"], "metrics": invalid}}, "PUT"), 422)
    model = author.call(ds + "/draft", model_request, "PUT")
    release = publish(author, model)["release"]
    cards = [
        {"id": "sales", "title": "销售额", "chart": "kpi", "measureId": "amount", "dimensions": [], "comparison": "mom"},
        {"id": "orders", "title": "订单数", "chart": "kpi", "measureId": "orders", "dimensions": [], "comparison": "none"},
        {"id": "regions", "title": "地区", "chart": "bar", "measureId": "amount",
         "dimensions": [{"fieldId": ids["region"], "level": "value"}], "comparison": "none"},
        {"id": "mean", "title": "实收客单价", "chart": "kpi", "measureId": "per_order", "dimensions": [], "comparison": "yoy"},
        {"id": "line-count", "title": "非空订单行", "chart": "kpi", "measureId": "lines", "dimensions": [], "comparison": "none"},
        {"id": "zero-rate", "title": "空比值", "chart": "kpi", "measureId": "undefined_ratio", "dimensions": [], "comparison": "none"},
    ]
    board = author.call(root, {"name": "经营", "datasetId": dataset["id"], "cards": cards})
    path = root + "/" + board["id"]
    check_denied(lambda: reader.call(path))
    published = author.call(path + "/publish", {"expectedRevision": board["revision"]})
    check_denied(lambda: author.call(path + "/publish", {"expectedRevision": board["revision"]}), 409)
    grant = {"subject": "other", "details": False, "export": False}
    shared = author.call(path + "/members", {"expectedRevision": published["revision"], "members": [grant]}, "PUT")
    assert any(item["id"] == board["id"] for item in reader.call(root)["items"])
    check_denied(lambda: stranger.call(path))
    filters = [{"fieldId": ids["orderdate"], "operator": op, "value": value}
               for op, value in (("from", "2026-11-01"), ("before", "2026-12-01"))]
    query = {"revision": published["publishedRevision"], "filters": filters}
    result = reader.call(path + "/query", query)
    card = result["cards"][0]
    assert Decimal(card["current"]["total"]["amount"]) == 150
    assert Decimal(card["previous"]["total"]["amount"]) == 100
    assert Decimal(card["delta"]) == 50 and Decimal(card["changeRate"]) == Decimal(".5")
    assert result["cards"][1]["current"]["total"]["orders"] == "1"
    assert Decimal(result["cards"][3]["current"]["total"]["per_order"]) == 150
    assert result["cards"][3]["previous"]["empty"] and result["cards"][3]["delta"] is None
    assert result["cards"][4]["current"]["total"]["lines"] == "2"
    assert result["cards"][5]["current"]["total"]["undefined_ratio"] is None
    check_denied(lambda: reader.call(path + "/query", {**query, "filters": filters[:1]}), 422)
    drill = {"context": card["current"]["context"]}
    check_denied(lambda: reader.call(path + "/drill", drill))
    check_denied(lambda: reader.call(path + "/export", drill, binary=True))
    check_denied(lambda: author.call(path + "/drill", drill))
    shared = author.call(path + "/members", {"expectedRevision": shared["revision"],
        "members": [{**grant, "details": True, "export": True}]}, "PUT")
    assert len(reader.call(path + "/drill", drill)["rows"]) == 2
    assert b"9.99" not in reader.call(path + "/export", {**drill, "details": True}, binary=True)

    scheduled = author.call(ds + "/refresh-schedule", {"expectedRevision": "0",
        "enabled": True, "intervalMinutes": 60}, "PUT")
    assert scheduled["nextDueAt"] and scheduled["enabled"]
    restart()
    author, reader, stranger = server()
    assert author.call(ds + "/refresh-schedule") == scheduled
    assert reader.call(path)["publishedRevision"] == published["publishedRevision"]
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("INSERT INTO sales VALUES ('new', '2026-11-05','华东','20.00','CNY')")
    token = uuid4().hex
    ticket = author.call(ds + "/refresh", {"operationId": token})
    assert ticket["operationId"] == token
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        items = author.call(ds + "/refreshes")["items"]
        terminal = next((item for item in items if item["operationId"] == token and item["finishedAt"]), None)
        if terminal:
            assert terminal["state"] == "published", terminal
            break
        time.sleep(.3)
    else:
        raise AssertionError("Refresh did not finish")
    assert author.call(ds + "/refresh", {"operationId": token}) == terminal
    assert len(reader.call(path + "/drill", drill)["rows"]) == 2
    same = reader.call(path + "/query", {**query, "releaseId": release["id"]})
    latest = reader.call(path + "/query", query)
    assert same["releaseId"] == release["id"] and latest["releaseId"] != same["releaseId"]
    assert Decimal(latest["cards"][0]["current"]["total"]["amount"]) == 170
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("INSERT INTO sales VALUES ('bad', 'not-a-date', '华东', '5.00', 'CNY')")
    rejected_id = uuid4().hex
    author.call(ds + "/refresh", {"operationId": rejected_id})
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        history = author.call(ds + "/refreshes")["items"]
        failed = next((item for item in history if item["operationId"] == rejected_id and item["finishedAt"]), None)
        if failed:
            assert failed["state"] == "rejected" and failed["error"], failed
            break
        time.sleep(.3)
    else:
        raise AssertionError("Rejected refresh did not finish")
    assert reader.call(path + "/query", query)["releaseId"] == latest["releaseId"]
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("DELETE FROM sales WHERE orderid='bad'")
    author.call(path + "/members", {"expectedRevision": shared["revision"], "members": []}, "PUT")
    check_denied(lambda: reader.call(path + "/drill", drill))
    assert all(item["id"] != board["id"] for item in reader.call(root)["items"])
    schedule = author.call(ds + "/refresh-schedule")
    existing = {item["operationId"] for item in author.call(ds + "/refreshes")["items"]}
    author.call(ds + "/refresh-schedule", {"expectedRevision": schedule["revision"],
        "enabled": True, "intervalMinutes": 1}, "PUT")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        scheduled_receipt = next((item for item in author.call(ds + "/refreshes")["items"]
            if item["operationId"] not in existing and item["finishedAt"]), None)
        if scheduled_receipt:
            assert scheduled_receipt["state"] == "published", scheduled_receipt
            assert scheduled_receipt["releaseId"] == author.call(ds)["publishedReleaseId"]
            break
        time.sleep(2)
    else:
        raise AssertionError("Scheduled refresh did not publish a release")
    return {"database": "SQLite", "sales": "150.00", "orders": 1, "afterRefresh": "170.00",
            "scheduledRefresh": "published"}


def main():
    parser = argparse.ArgumentParser()
    for name in ("management", "worker", "plugin", "work"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--dotnet", required=True)
    parser.add_argument("--browser-executable", type=Path)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="team-", dir=args.work) as tmp:
        private = Path(tmp).resolve()
        uploads = private / "sources"
        uploads.mkdir()
        db = uploads / "sales.sqlite"
        with closing(sqlite3.connect(db)) as conn, conn:
            conn.execute("CREATE TABLE sales(orderid TEXT, orderdate TEXT, region TEXT, amount TEXT, currency TEXT)")
            conn.executemany("INSERT INTO sales VALUES(?,?,?,?,?)", [
                ("001", "2026-11-02", "华东", "100.00", "CNY"), ("001", "2026-11-03", "华南", "50.00", "CNY"),
                ("002", "2026-10-02", "华东", "100.00", "CNY")])
        password = secrets.token_urlsafe(24)
        salt = os.urandom(32)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600000)
        encoded = "pbkdf2-sha256$600000$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()
        identity = private / "identity.json"
        identity.write_text(json.dumps({"version": 1, "roles": {"author": [{"permission": "*", "resource": "*"}]},
            "users": [{"id": name, "tenant": tenant, "passwordHash": encoded, "stamp": secrets.token_hex(32),
                       "roles": ["author"] if name == "analyst" else []}
                      for name, tenant in (("analyst", "bi"), ("other", "bi"), ("stranger", "outside"))]}))
        identity.chmod(0o600)
        resources = {"fileRoots": [], "networkOrigins": [], "credentialReferences": [], "pluginRoots": [str(args.plugin.resolve().parent)]}
        policy = private / "policy.json"
        policy.write_text(json.dumps({"identityFile": str(identity), "dataDirectory": str(private / "data"),
            "fileRoots": [str(uploads)], "networkOrigins": [], "pluginRoots": resources["pluginRoots"], "credentialFiles": {},
            "tenants": {"bi": {**resources, "readOnlyRoots": [str(uploads)], "analysisSqlSources": [
                {"id": "sales", "name": "门店销售", "table": "sales", "allowSharing": True,
                 "connection": {"driver": "ud:sqlite", "resource": str(db)}}]}, "outside": resources}}))
        with socket.socket() as free:
            free.bind(("127.0.0.1", 0))
            addr = "http://127.0.0.1:" + str(free.getsockname()[1])
        log = (args.work / "team-host.log").open("w", encoding="utf-8")
        def start():
            host = subprocess.Popen([args.dotnet, str(args.management.resolve()), "--worker", str(args.worker.resolve()),
                "--policy", str(policy), "--urls", addr], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline and host.poll() is None:
                    try:
                        build_opener(ProxyHandler({})).open(addr + "/health/live", timeout=1).close()
                        return host
                    except (URLError, TimeoutError):
                        time.sleep(.2)
                raise RuntimeError("Management failed to start, see team-host.log")
            except BaseException:
                host.terminate()
                host.wait(timeout=10)
                raise
        host = start()
        def restart():
            nonlocal host
            host.terminate()
            host.wait(timeout=10)
            host = start()
        try:
            result = verify(lambda: [Session(addr, user, password) for user in ("analyst", "other", "stranger")], db, restart)
            if args.browser_executable:
                env = {**os.environ, "UD_BROWSER_PASSWORD": password}
                subprocess.run([sys.executable, str(Path(__file__).with_name("analysis_team_browser.py")),
                    "--url", addr, "--source-id", "sales", "--evidence", str(args.work), "--browser-executable",
                    str(args.browser_executable)], env=env, check=True)
            print(json.dumps(result, ensure_ascii=True))
        finally:
            host.terminate()
            host.wait(timeout=10)
            log.close()


if __name__ == "__main__":
    main()

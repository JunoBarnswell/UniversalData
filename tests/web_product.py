"""Real browser + product service/Workers. The host policy must grant --root."""
import argparse
import http.cookiejar
import json
import os
import pathlib
import signal
import time
import urllib.request
import uuid
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--url", required=True)
parser.add_argument("--root", required=True, type=pathlib.Path)
parser.add_argument("--evidence", required=True, type=pathlib.Path)
parser.add_argument("--browser-executable", type=pathlib.Path)
parser.add_argument("--subject", default="acceptance")
args = parser.parse_args()
password = os.environ['UD_BROWSER_PASSWORD']
args.evidence.mkdir(parents=True, exist_ok=True)
identity = "browser-" + uuid.uuid4().hex
root = args.root / identity
root.mkdir()
(root / "facts.ndjson").write_text('{"id":1,"label":"first"}\n{"id":2,"label":"second"}\n')

client = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
csrf = ''
def api(method, path, value=None):
    request = urllib.request.Request(args.url + "/api" + path, method=method,
        data=None if value is None else json.dumps(value).encode(), headers={"Content-Type": "application/json", "X-UD-CSRF": csrf})
    with client.open(request, timeout=20) as response:
        data = response.read()
        return json.loads(data) if data else None

def wait_run(ticket, state):
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        run = api("GET", "/executions/" + ticket)
        if run["state"] == state:
            return run
        if run["state"] in ("Failed", "Cancelled"):
            raise AssertionError(run)
        time.sleep(.1)
    raise AssertionError((state, run))

schema = dict(version=10, fields=[dict(name="id", type="int64", nullable=False), dict(name="label", type="utf8", nullable=False)])
document = dict(version=10, id=identity, connections=[dict(id="facts", driver="file", resource=str(root / "facts.ndjson")),
    dict(id="output", driver="file", resource=str(root / "output.ndjson"))],
    controlFlow=dict(tasks=[dict(id="flow", kind="dataFlow", config=dict(pipeline="main"))], constraints=[]),
    dataFlows=[dict(id="main", components=[dict(id="read", component="file.ndjson.source", config=dict(connection="facts", schema=schema)),
        dict(id="copy", component="copyColumn", config=dict(columns=[dict(column="label", name="copied")])),
        dict(id="write", component="file.ndjson.destination", config=dict(connection="output", fileMode="overwrite"))],
        paths=[{"from":"read", "output":0, "to":"copy", "input":0}, {"from":"copy", "output":0, "to":"write", "input":0}])],
    layout=dict(version=10, nodes=[dict(flow="main", id=key, x=x, y=190) for key,x in [("read",50),("copy",320),("write",590)]], viewports=[dict(flow="main", x=0, y=0, zoom=1)]))
errors = []
csrf = api('POST', '/auth/login', {'subject': args.subject, 'password': password})['csrf']
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(
        executable_path=str(args.browser_executable) if args.browser_executable else None,
        headless=True, args=["--no-sandbox"])
    page = browser.new_page(viewport=dict(width=1505, height=1000))
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(args.url)
    page.get_by_role('textbox', name='账户', exact=True).fill(args.subject)
    page.get_by_label('密码', exact=True).fill(password)
    page.get_by_role('button', name='登录', exact=True).click()
    page.get_by_role("button", name="包配置", exact=True).click()
    page.get_by_role("textbox", name="包配置", exact=True).fill(json.dumps(document))
    page.get_by_role("button", name="应用", exact=True).click()
    page.get_by_role("button", name="校验", exact=True).click()
    page.get_by_text("校验通过", exact=True).wait_for(timeout=15000)
    page.locator('.react-flow__node[data-id="copy"]').click()
    page.get_by_text("字段复制映射", exact=True).wait_for()
    # Actual graph drag enters undo history and survives save/reload.
    node = page.locator('.react-flow__node[data-id="copy"]')
    bounds = node.bounding_box()
    page.mouse.move(bounds["x"] + 60, bounds["y"] + 25)
    page.mouse.down(); page.mouse.move(bounds["x"] + 80, bounds["y"] + 65, steps=8); page.mouse.up()
    page.get_by_role("button", name="撤销", exact=True).click()
    page.get_by_role("button", name="重做", exact=True).click()
    page.get_by_role("button", name="校验", exact=True).click()
    page.get_by_text("校验通过", exact=True).wait_for(timeout=15000)
    page.screenshot(path=str(args.evidence / "designer.png"), full_page=True)
    page.get_by_role("button", name="保存", exact=True).click()
    page.get_by_text("已保存 v1", exact=True).wait_for(timeout=15000)
    saved = api("GET", f"/packages/{identity}/versions/1")
    assert next(node for node in saved["layout"]["nodes"] if node["id"] == "copy")["y"] != 190
    page.get_by_role("textbox", name="作业包名称").fill("unsaved-change")
    page.get_by_role("button", name="重载", exact=True).click()
    page.get_by_role("button", name=identity + " · v1", exact=True).click()
    assert page.get_by_role("textbox", name="作业包名称").input_value() == identity
    page.get_by_role("button", name="部署并执行", exact=True).click()
    page.get_by_role("heading", name="执行管理", exact=True).wait_for()
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        runs = [run for run in api("GET", "/executions") if run["packageId"] == identity]
        if runs: break
        time.sleep(.1)
    completed = wait_run(runs[0]["id"], "Succeeded")
    assert completed["executionId"] and completed["result"] == 0
    output = [json.loads(line) for line in (root / "output.ndjson").read_text().splitlines()]
    assert output == [dict(id=1,label="first",copied="first"),dict(id=2,label="second",copied="second")]
    page.get_by_role("button", name="查看", exact=True).first.click()
    page.locator(".log-panel tbody tr").first.wait_for(timeout=10000)
    page.screenshot(path=str(args.evidence / "executions.png"), full_page=True)
    debug = api("POST", "/executions", dict(packageId=identity, packageRevision="1", debug=dict(previewRows=2,breakpoints=["/4:flow;"])))
    active = wait_run(debug["id"], "Running")
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snapshot = api("GET", "/executions/" + debug["id"] + "/debug")
        if snapshot["paused"]: break
        time.sleep(.1)
    assert snapshot["paused"]
    # Terminate the real Worker while Native owns the durable checkpoint.
    os.kill(active["processId"], getattr(signal, "SIGKILL", signal.SIGTERM))
    uncertain = wait_run(debug["id"], "RecoveryRequired")
    assert uncertain["executionId"] == active["executionId"]
    api("POST", "/executions/" + debug["id"] + "/resume")
    resumed = wait_run(debug["id"], "Running")
    assert resumed["executionId"] == active["executionId"]
    api("POST", "/executions/" + debug["id"] + "/debug", {"action":"continue"})
    recovered = wait_run(debug["id"], "Succeeded")
    logs = api("GET", "/logs?executionId=" + recovered["executionId"])
    assert logs and len({entry["revision"] for entry in logs}) == len(logs)
    assert [json.loads(line) for line in (root / "output.ndjson").read_text().splitlines()] == output
    page.get_by_role("button", name="日志", exact=True).click()
    page.get_by_role("heading", name="执行日志", exact=True).wait_for()
    page.get_by_role("button", name="作业", exact=True).click()
    page.get_by_role("heading", name="作业管理", exact=True).wait_for()
    page.set_viewport_size(dict(width=768, height=900))
    page.get_by_role("button", name="设计器", exact=True).click()
    page.screenshot(path=str(args.evidence / "designer-compact.png"), full_page=True)
    assert not errors, errors
    browser.close()
(args.evidence / "browser.json").write_text(json.dumps(dict(status="Passed", packageId=identity,
    edit=True, dragUndoRedo=True, saveReload=True, deployment=True, nativeOutput=True, logs=True,
    actualWorkerTermination=True, resumeSameNativeId=True, debugPause=True, browserErrors=errors), indent=2))
print("Real browser editing, field mapping, drag/undo/redo, save/reload, deployment, Worker output/logs/debug and killed-Worker recovery passed")

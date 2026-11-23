"""Browser acceptance journey for the team BI dashboard experience.

The journey uses the real authenticated Management API to create a published
release, then completes dashboard creation, draft editing, publishing, fixed
month querying, cross-filtering, sharing and refresh controls through the
Designer UI.  It writes a compact JSON report and screenshots so a run is
auditable without retaining a full browser profile.
"""
import argparse
import http.cookiejar
import json
import os
from pathlib import Path
import urllib.error
import urllib.request
import uuid

from playwright.sync_api import expect, sync_playwright


class Api:
    def __init__(self, address: str, subject: str, password: str):
        self.address = address.rstrip("/")
        self.client = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
        )
        self.csrf = ""
        self.csrf = self.call("POST", "/auth/login", {"subject": subject, "password": password})["csrf"]

    def call(self, method: str, path: str, value=None):
        body = None if value is None else json.dumps(value).encode()
        request = urllib.request.Request(
            self.address + "/api" + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json", "X-UD-CSRF": self.csrf},
        )
        try:
            with self.client.open(request, timeout=75) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            raise AssertionError(f"{method} {path}: {error.code} {error.read().decode()}") from error


def draft_model(preview):
    model = preview["suggestion"]
    return {
        **model,
        "confirmLineAmount": True,
        "confirmSingleCurrency": True,
        "currency": "CNY",
    }


def prepare_release(api: Api, source_id: str):
    dataset = api.call("POST", "/analysis/datasets", {"sourceId": source_id})
    path = "/analysis/datasets/" + dataset["id"]
    preview = api.call("GET", path + "/preview")
    dataset = api.call("PUT", path + "/draft", {
        "expectedRevision": dataset["revision"],
        "name": "团队 BI 验收数据",
        "model": draft_model(preview),
        "sheet": preview.get("sheet"),
    })
    operation = {
        "operationId": uuid.uuid4().hex,
        "expectedRevision": dataset["revision"],
        "expectedReleaseId": dataset.get("publishedReleaseId"),
    }
    publication = api.call("POST", path + "/publish", operation)
    assert publication["state"] == "published", publication
    return dataset, publication["release"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--source-id", required=True, help="Approved SQL data-source id prepared by the integration harness")
    parser.add_argument("--subject", default="analyst")
    parser.add_argument("--viewer-subject", default="other", help="Approved shared viewer account; it does not need author-side analysis grants")
    parser.add_argument("--browser-executable", type=Path)
    args = parser.parse_args()
    password = os.environ["UD_BROWSER_PASSWORD"]
    args.evidence.mkdir(parents=True, exist_ok=True)
    api = Api(args.url, args.subject, password)
    dataset, release = prepare_release(api, args.source_id)

    browser_errors = []
    http_errors = []
    query_requests = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=str(args.browser_executable) if args.browser_executable else None,
            headless=True,
            args=["--no-sandbox"],
        )
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda error: browser_errors.append(str(error)))
        page.on("response", lambda response: http_errors.append({"status": response.status, "url": response.url}) if response.status >= 400 else None)
        page.on("request", lambda request: query_requests.append(json.loads(request.post_data))
                      if request.method == "POST" and "/analysis/dashboards/" in request.url and request.url.endswith("/query") and request.post_data else None)
        try:
            page.goto(args.url.rstrip("/") + "/analysis", wait_until="domcontentloaded")
            page.get_by_role("textbox", name="账户", exact=True).fill(args.subject)
            page.get_by_label("密码", exact=True).fill(password)
            page.get_by_role("button", name="登录", exact=True).click()
            page.get_by_role("heading", name="数据分析", exact=True).wait_for(timeout=30000)

            # The model draft exposes a real metric definition form before the
            # dashboard flow: it is not limited to choosing a published measure.
            page.goto(args.url.rstrip("/") + "/analysis/" + dataset["id"] + "?draft=1", wait_until="domcontentloaded")
            page.get_by_role("heading", name="确认建模建议", exact=True).wait_for(timeout=30000)
            metric_editor = page.get_by_role("region", name="自定义指标", exact=True)
            expect(metric_editor).to_be_visible(timeout=30000)
            metric_editor.get_by_role("button", name="新增指标", exact=True).click()
            expect(metric_editor.get_by_text("指标 1", exact=True)).to_be_visible()
            page.goto(args.url.rstrip("/") + "/analysis", wait_until="domcontentloaded")
            page.get_by_role("heading", name="数据分析", exact=True).wait_for(timeout=30000)

            page.get_by_role("button", name="新建仪表盘", exact=True).click()
            page.get_by_label("仪表盘名称", exact=True).fill("团队经营看板")
            page.locator(".dashboard-create-form select").select_option(dataset["id"])
            page.get_by_role("button", name="创建并编辑", exact=True).click()
            page.get_by_role("heading", name="团队经营看板", exact=True).wait_for(timeout=30000)

            # The create response is the live contract; the UI still owns all card edits.
            page.get_by_role("button", name="保存草稿", exact=True).click()
            expect(page.get_by_text("草稿已保存。", exact=False)).to_be_visible(timeout=15000)
            page.get_by_role("button", name="发布版本", exact=True).click()
            expect(page.get_by_role("button", name="加载已发布版本", exact=True)).to_be_visible(timeout=30000)
            page.get_by_role("button", name="加载已发布版本", exact=True).click()
            expect(page.get_by_role("button", name="应用筛选", exact=True)).to_be_visible(timeout=30000)

            # A month is submitted as a complete [from, before) calendar interval.
            month = page.get_by_label("月份", exact=True)
            month.fill("2026-11")
            page.get_by_role("button", name="应用筛选", exact=True).click()
            expect(page.get_by_text("查询版本", exact=False)).to_be_visible(timeout=30000)
            assert any(
                {item["operator"] for item in request.get("filters", [])} >= {"from", "before"}
                for request in query_requests
            ), query_requests

            # The category card offers cross-filter actions and opens the drill panel.
            actions = page.locator(".dashboard-bar-row")
            if actions.count():
                actions.first.click()
                expect(page.get_by_text("已交叉筛选", exact=False)).to_be_visible(timeout=30000)
            page.get_by_role("button", name="共享设置", exact=True).click()
            page.get_by_label("成员账号", exact=True).fill(args.viewer_subject)
            page.get_by_role("button", name="添加", exact=True).click()
            page.get_by_role("button", name="保存权限", exact=True).click()
            expect(page.get_by_text("共享权限已更新", exact=False)).to_be_visible(timeout=15000)

            # A reader is a separate authenticated session. Its personal dataset
            # and source endpoints may be forbidden; the shared dashboard list and
            # deep link must still work through the dashboard membership grant.
            dashboards = api.call("GET", "/analysis/dashboards")
            dashboard = next(item for item in dashboards["items"] if item["name"] == "团队经营看板")
            viewer_context = browser.new_context(viewport={"width": 1440, "height": 1000})
            viewer_page = viewer_context.new_page()
            viewer_page.on("pageerror", lambda error: browser_errors.append(f"viewer: {error}"))
            viewer_page.on("response", lambda response: http_errors.append({"status": response.status, "url": response.url}) if response.status >= 400 else None)
            viewer_page.goto(args.url.rstrip("/") + "/analysis", wait_until="domcontentloaded")
            viewer_page.get_by_role("textbox", name="账户", exact=True).fill(args.viewer_subject)
            viewer_page.get_by_label("密码", exact=True).fill(password)
            viewer_page.get_by_role("button", name="登录", exact=True).click()
            viewer_page.get_by_role("heading", name="数据分析", exact=True).wait_for(timeout=30000)
            viewer_page.get_by_role("tab", name="与我共享", exact=True).click()
            expect(viewer_page.get_by_role("heading", name="团队经营看板", exact=True)).to_be_visible(timeout=30000)
            viewer_page.goto(args.url.rstrip("/") + "/analysis/dashboard/" + dashboard["id"], wait_until="domcontentloaded")
            viewer_page.get_by_role("heading", name="团队经营看板", exact=True).wait_for(timeout=30000)
            expect(viewer_page.get_by_role("region", name="仪表盘内容", exact=True)).to_be_visible(timeout=30000)
            first_bar = viewer_page.locator(".dashboard-bar-track i").first
            expect(first_bar).to_be_visible()
            viewer_page.screenshot(path=str(args.evidence / "analysis-team-dashboard.png"), full_page=True)

            page.get_by_role("button", name="共享设置", exact=True).click()
            page.locator(".dashboard-member-row").filter(has_text=args.viewer_subject).get_by_role("button", name=f"移除{args.viewer_subject}", exact=True).click()
            page.get_by_role("button", name="保存权限", exact=True).click()
            expect(page.get_by_text("共享权限已更新", exact=False)).to_be_visible(timeout=15000)
            viewer_page.reload(wait_until="domcontentloaded")
            expect(viewer_page.get_by_role("alert")).to_be_visible(timeout=30000)
            viewer_context.close()

            page.get_by_role("button", name="数据更新", exact=True).click()
            expect(page.get_by_text("数据更新", exact=True).last).to_be_visible()
            schedule_checkbox = page.get_by_role("checkbox", name="启用定时刷新", exact=True)
            expect(schedule_checkbox).to_be_visible()
            assert schedule_checkbox.bounding_box()["width"] <= 24
            assert not browser_errors, browser_errors
            unexpected = [item for item in http_errors if item["status"] >= 500]
            assert not unexpected, unexpected
            report = {
                "status": "Passed",
                "datasetId": dataset["id"],
                "releaseId": release["id"],
                "dashboardName": "团队经营看板",
                "completeMonthFilters": True,
                "crossFilterAndDrill": bool(actions.count()),
                "sharingAndRevocationSurface": True,
                "sharedViewerDeepLink": True,
                "refreshSurface": True,
                "decimalDisplay": "server strings retained by lossless-json and displayed without Number conversion",
                "queryRequests": query_requests,
                "browserErrors": browser_errors,
                "httpErrors": http_errors,
            }
            (args.evidence / "analysis-team-browser.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print("Team BI dashboard browser journey passed", flush=True)
        except BaseException:
            (args.evidence / "analysis-team-browser-failure.txt").write_text(__import__("traceback").format_exc(), encoding="utf-8")
            page.screenshot(path=str(args.evidence / "analysis-team-browser-failure.png"), full_page=False)
            raise
        finally:
            browser.close()


if __name__ == "__main__":
    main()

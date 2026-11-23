"""Measure real six-card renders during a real refresh using authenticated browser sessions."""
import argparse
import asyncio
import json
import math
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from playwright.async_api import async_playwright


async def checked(response):
    if not response.ok:
        raise RuntimeError(f"Management returned HTTP {response.status}")
    return await response.json()


async def run(args, evidence):
    cases = json.loads(args.cases.read_text(encoding="utf-8"))
    sessions = sorted(args.sessions.glob("*.json"))
    if len(sessions) != args.users or len(cases) < 2:
        raise ValueError("Provide one storage state per user and at least two independently reconciled filter cases")
    timeline = []
    query_timeout_ms = args.query_timeout * 1000
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(executable_path=args.browser_executable)
        try:
            author = await browser.new_context(base_url=args.url, storage_state=args.author)
            identity = await checked(await author.request.get("/api/auth/session"))
            csrf = {"X-UD-CSRF": identity["csrf"]}
            ds = "/api/analysis/datasets/" + args.dataset
            current = await checked(await author.request.get(ds))
            release = await checked(await author.request.get(ds + "/releases/" + current["publishedReleaseId"]))
            if release["rows"] != args.rows:
                raise AssertionError(f"Published row count {release['rows']} differs from requested {args.rows}")
            evidence["release"] = release["id"]
            contexts = [await browser.new_context(base_url=args.url, storage_state=state) for state in sessions]
            sessions_info = await asyncio.gather(*(context.request.get("/api/auth/session") for context in contexts))
            subjects = await asyncio.gather(*(checked(response) for response in sessions_info))
            if len({entry["subject"] for entry in subjects}) != len(contexts):
                raise ValueError("Each concurrent session must belong to a distinct user")
            pages = await asyncio.gather(*(context.new_page() for context in contexts))
            started = time.perf_counter()
            evidence["phase"] = "open dashboards"
            async def open_board(page):
                start = time.perf_counter()
                async with page.expect_response(
                    lambda r: r.url.endswith("/" + args.board + "/query") and r.request.method == "POST",
                    timeout=query_timeout_ms
                ) as pending:
                    await page.goto("/analysis/dashboard/" + args.board, wait_until="domcontentloaded", timeout=query_timeout_ms)
                payload = await checked(await pending.value)
                if len(payload["cards"]) != 6 or payload["releaseId"] != release["id"]:
                    raise AssertionError("Initial render did not return all six cards from the pinned release")
                await page.wait_for_function(
                    "() => document.querySelector('[data-query-context]')?.children.length === 6", timeout=query_timeout_ms)
                return time.perf_counter() - start
            opened = await asyncio.gather(*(open_board(page) for page in pages), return_exceptions=True)
            evidence["firstOpenSeconds"] = [value for value in opened if isinstance(value, float)]
            errors = [value for value in opened if isinstance(value, Exception)]
            if errors:
                evidence["firstOpenFailures"] = len(errors)
                raise errors[0]
            evidence["firstOpenP95Seconds"] = sorted(opened)[math.ceil(len(opened) * .95) - 1]
            operation = uuid4().hex
            refresh_start = time.perf_counter()
            evidence["phase"] = "refresh and filter"
            await checked(await author.request.post(ds + "/refresh", headers=csrf, data={"operationId": operation}))

            async def exercise(page, case, round_index):
                await page.get_by_label("月份", exact=True).fill(case["month"])
                category = page.get_by_role("combobox", name="分类字段", exact=True)
                if case.get("categoryField"):
                    await category.select_option(case["categoryField"])
                    await page.get_by_label("分类值", exact=True).fill(case["categoryValue"])
                elif await category.count():
                    await category.select_option("")
                before = await page.locator("[data-query-context]").get_attribute("data-query-context")
                started_click = time.perf_counter()
                async with page.expect_response(lambda r: r.url.endswith("/" + args.board + "/query") and r.request.method == "POST",
                                                timeout=query_timeout_ms) as pending:
                    await page.get_by_role("button", name="应用筛选", exact=True).click()
                response = await pending.value
                if case.get("categoryField") and not any(
                    f["fieldId"] == case["categoryField"] and f["operator"] == "eq" and f["value"] == case["categoryValue"]
                    for f in response.request.post_data_json["filters"]
                ):
                    raise AssertionError("The rendered form did not submit the selected category")
                payload = await checked(response)
                if len(payload["cards"]) != 6 or payload["releaseId"] != release["id"]:
                    raise AssertionError("Rendered board must contain six cards from the pinned release")
                await page.wait_for_function(
                    "(old) => { const n=document.querySelector('[data-query-context]'); return n && n.dataset.queryContext !== old && n.getAttribute('aria-busy') !== 'true' && n.children.length === 6 }",
                    arg=before)
                elapsed = time.perf_counter() - started_click
                for card in payload["cards"]:
                    expected = case["totals"][card["id"]]
                    actual = card["current"]["total"][expected["measureId"]]
                    if (None if actual is None else Decimal(actual)) != (None if expected["value"] is None else Decimal(expected["value"])):
                        raise AssertionError(f"Incorrect total for {card['id']}")
                timeline.append({"case": case["id"], "round": round_index, "seconds": elapsed})

            finished = None
            index = 0
            while not finished or index < args.rounds:
                if time.perf_counter() - refresh_start > args.refresh_limit + 60:
                    raise TimeoutError("Refresh exceeded acceptance window")
                outcomes = await asyncio.gather(*(exercise(page, cases[(index + i) % len(cases)], index)
                                                for i, page in enumerate(pages)), return_exceptions=True)
                errors = [value for value in outcomes if isinstance(value, Exception)]
                if errors:
                    evidence["filterFailures"] = len(errors)
                    raise errors[0]
                if finished is None:
                    receipts = await checked(await author.request.get(ds + "/refreshes"))
                    finished = next((r for r in receipts["items"] if r["operationId"] == operation and r["finishedAt"]), None)
                    if finished:
                        evidence["refreshReceipt"] = finished
                        evidence["refreshSecondsUpperBound"] = time.perf_counter() - refresh_start
                        if finished["state"] != "published":
                            raise AssertionError("Refresh did not publish a release")
                index += 1
            evidence["wallSeconds"] = time.perf_counter() - started
        finally:
            await browser.close()
            evidence["samples"] = timeline

    durations = sorted(x["seconds"] for x in timeline)
    for percentile in (50, 95, 99):
        evidence[f"p{percentile}Seconds"] = durations[math.ceil(len(durations) * percentile / 100) - 1]
    duration = datetime.fromisoformat(finished["finishedAt"]) - datetime.fromisoformat(finished["startedAt"])
    evidence["refreshSeconds"] = duration.total_seconds()
    if max(evidence["firstOpenP95Seconds"], evidence["p95Seconds"]) > args.p95_limit or evidence["refreshSeconds"] > args.refresh_limit:
        raise AssertionError("Performance thresholds exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("url", "board", "dataset", "driver"):
        parser.add_argument("--" + name, required=True)
    for name in ("sessions", "author", "cases", "evidence"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--browser-executable")
    parser.add_argument("--users", default=50, type=int)
    parser.add_argument("--rows", default=30_000_000, type=int)
    parser.add_argument("--rounds", default=5, type=int)
    parser.add_argument("--p95-limit", default=3, type=float)
    parser.add_argument("--refresh-limit", default=1800, type=float)
    parser.add_argument("--query-timeout", default=180, type=float,
                        help="Seconds to observe a page or filter operation, independently of its P95 pass threshold")
    args = parser.parse_args()
    if min(args.rows, args.users, args.rounds, args.p95_limit, args.refresh_limit, args.query_timeout) <= 0:
        parser.error("Rows, users, rounds and timing limits must be positive")
    evidence = {"driver": args.driver, "rows": args.rows, "users": args.users, "status": "Fail",
                "phase": "validate sessions and release"}
    try:
        asyncio.run(run(args, evidence))
        evidence["status"] = "Pass"
        evidence["phase"] = "complete"
    except Exception as error:
        evidence["failureType"] = type(error).__name__
        raise
    finally:
        args.evidence.write_text(json.dumps(evidence, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Deterministic independent input and accounting fixture, NOT an SSIS golden.

Generates identical logical records in valid/dirty CSV files and independently
calculates branch counts and Decimal totals. Actual SSIS and UniversalData must
run from these inputs and must not reuse this Python classifier as production ETL.
"""
import argparse
import csv
import hashlib
import json
import pathlib
import struct
from collections import Counter, defaultdict
from decimal import Decimal

HEADER = ["RowId", "OrderNo", "CustomerName", "Status", "Region", "ProductCode", "Amount", "CategoryPath", "CreatedAt", "Remark"]
STATUS = ["PAID", "NEW", "REFUND", "PAID", "PENDING"]
REGIONS = ["EAST", "SOUTH", "NORTH", "WEST"]
PRODUCTS = [f"P{i:02}" for i in range(1, 8)]
N = 50000


def raw_row(i, dirty):
    status = None if i % 887 == 0 else STATUS[i % 5]
    region = REGIONS[i % 4]
    product = PRODUCTS[i % 7]
    cents = None if i % 101 == 0 else ((i * 37) % 200000 - 25000)
    amount = None if cents is None else Decimal(cents) / Decimal(100)
    invalid = dirty and i % 997 == 0
    return {
        "RowId": i,
        "OrderNo": f"OD{i:08d}",
        "CustomerName": f"\t客户{i % 701:03d}\t" if i % 11 == 0 else f"  客户{i % 701:03d}  ",
        "Status": status,
        "Region": region,
        "ProductCode": product,
        "Amount": "INVALID_DECIMAL" if invalid else ("" if amount is None else f"{amount:.2f}"),
        "CategoryPath": f"设备|{product}|{region}",
        "CreatedAt": f"2026-10-{(i % 28) + 1:02d} {i % 24:02d}:15:00",
        "Remark": '商品,"合格"\r\n检查' if i % 113 == 0 else ("🚀完成" if i % 17 == 0 else "正常"),
    }, amount, invalid


def digest_rows(indices):
    h = hashlib.sha256()
    for i in sorted(indices):
        h.update(struct.pack(">Q", i))
    return h.hexdigest()


def generate(root, dirty):
    path = root / ("ssis_50000_dirty.csv" if dirty else "ssis_50000_clean.csv")
    branches = defaultdict(list)
    region_counts, product_counts = Counter(), Counter()
    region_sum = defaultdict(lambda: Decimal("0.00"))
    product_sum = defaultdict(lambda: Decimal("0.00"))
    error_rows = []
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=HEADER, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for i in range(1, N + 1):
            row, amount, invalid = raw_row(i, dirty)
            writer.writerow({k: "" if v is None else v for k, v in row.items()})
            if invalid:
                error_rows.append(i)
                continue
            status, region, product = row["Status"], row["Region"], row["ProductCode"]
            # Independent branch classifier for accounting expectations only.
            if status == "PAID" and region == "EAST" and amount is not None and amount >= Decimal(500):
                destination = "VIP"
            elif status == "REFUND" or (amount is not None and amount < 0):
                destination = "REFUND"
            elif status == "PAID":
                destination = "PAID_OTHER"
            else:
                destination = "OTHER"
            branches[destination].append(i)
            region_counts[region] += 1
            product_counts[product] += 1
            if amount is not None:
                region_sum[region] += amount
                product_sum[product] += amount

    with path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        count = 0
        for count, record in enumerate(reader, 1):
            assert int(record["RowId"]) == count
    assert count == N, (path, count)
    assert sum(map(len, branches.values())) + len(error_rows) == N
    assert len(set().union(*(set(ids) for ids in branches.values()))) == N - len(error_rows)
    return {
        "source": path.name,
        "inputRows": N,
        "sourceFileSha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "sourceFileBytes": path.stat().st_size,
        "businessRows": N - len(error_rows),
        "errorRows": len(error_rows),
        "errorRowIds": error_rows,
        "errorRowIdsSha256": digest_rows(error_rows),
        "branches": {
            k: {"count": len(branches[k]), "rowIdsSha256": digest_rows(branches[k])}
            for k in ("VIP", "REFUND", "PAID_OTHER", "OTHER")
        },
        "region": {
            k: {"count": region_counts[k], "amountSum": f"{region_sum[k]:.2f}"}
            for k in REGIONS
        },
        "product": {
            k: {"count": product_counts[k], "amountSum": f"{product_sum[k]:.2f}"}
            for k in PRODUCTS
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parent)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    facts = {
        "description": "Independent deterministic 50000-row business expectations; NOT real SSIS Golden output",
        "formatVersion": 1,
        "header": HEADER,
        "clean": generate(args.output, False),
        "dirty": generate(args.output, True),
    }
    expected = args.output / "ssis_50000_expected.json"
    expected.write_text(json.dumps(facts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    assert [facts["clean"]["branches"][v]["count"] for v in ("VIP", "REFUND", "PAID_OTHER", "OTHER")] == [3008, 15342, 14298, 17352]
    assert [facts["dirty"]["branches"][v]["count"] for v in ("VIP", "REFUND", "PAID_OTHER", "OTHER")] == [3004, 15327, 14284, 17335]
    assert facts["dirty"]["errorRows"] == 50
    print(json.dumps({"clean": {"branches": facts["clean"]["branches"], "region": facts["clean"]["region"]},
                      "dirty": {"branches": facts["dirty"]["branches"], "errors": facts["dirty"]["errorRows"]}}, ensure_ascii=False))

if __name__ == "__main__":
    main()

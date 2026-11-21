"""Check a spilled mixed-order page and its exact output after a Native process dies."""
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import tempfile

from next_phase_pipeline import PLUGIN, VERSION, component, edge, field, run, schema


def prepare(root):
    (root / "state").mkdir()
    (root / "spill").mkdir()
    shapes = schema([field("entry_id", keyOrder=1), field("shop_id"), field("score", nullable=True)])
    with closing(sqlite3.connect(root / "orders.db")) as source, source:
        source.execute("CREATE TABLE orders(entry_id BIGINT NOT NULL PRIMARY KEY, shop_id INTEGER NOT NULL, score INTEGER)")
        source.executemany("INSERT INTO orders VALUES(?,?,?)",
                           ((key, key % 17, key % 11 if key % 5 else None) for key in range(4095, 0, -1)))
        expected = source.execute(
            "SELECT * FROM orders ORDER BY score DESC NULLS LAST, shop_id, entry_id LIMIT 325 OFFSET 37"
        ).fetchall()
    with closing(sqlite3.connect(root / "output.db")) as target, target:
        target.execute("CREATE TABLE page(entry_id BIGINT NOT NULL PRIMARY KEY, shop_id INTEGER NOT NULL, score INTEGER)")
    config = dict(version=VERSION, id="linq-range",
                  connections=[
                      dict(id="orders", driver=PLUGIN, resource=str(root / "orders.db"), credentialReference="fixture"),
                      dict(id="output", driver=PLUGIN, resource=str(root / "output.db"), credentialReference="fixture"),
                      dict(id="spill", driver="file", resource=str(root / "spill")),
                  ],
                  policy=dict(buffers=dict(connection="spill", residentBytes=4096, workingBytes=4194304,
                                           spillBytes=100000000, maxBlocks=4096)),
                  controlFlow=dict(tasks=[dict(id="page", kind="dataFlow", config=dict(pipeline="rows"))], constraints=[]),
                  dataFlows=[dict(id="rows", components=[
                      component("orders", "sql.source", connection="orders", table="orders", schema=shapes,
                                sql="SELECT entry_id, shop_id, score FROM orders ORDER BY entry_id", batchSize=31),
                      component("sort", "sort", keys=["score", "shop_id"], keyDirections=[True, False]),
                      component("page", "rowRange", ranges=[
                          dict(op="skip", count=35), dict(op="take", count=601),
                          dict(op="skip", count=2), dict(op="take", count=325),
                      ]),
                      component("target", "sql.destination", connection="output", table="page", mode="append",
                                provisionReceipts=True),
                  ], paths=[edge("orders", "sort"), edge("sort", "page"), edge("page", "target")])])
    (root / "package.json").write_text(json.dumps(config), encoding="utf-8")
    return expected


def output(root):
    with closing(sqlite3.connect(root / "output.db")) as target:
        return target.execute("SELECT entry_id, shop_id, score FROM page ORDER BY rowid").fetchall()


def main():
    with tempfile.TemporaryDirectory(prefix="ud-linq-range-") as scratch:
        root = Path(scratch)
        complete = root / "complete"
        complete.mkdir()
        expected = prepare(complete)
        clean = run(complete)
        assert clean["spillBytes"] > 0 and output(complete) == expected, clean

        interrupted = root / "interrupted"
        interrupted.mkdir()
        assert prepare(interrupted) == expected
        run(interrupted, kill=4, after=1, component="target")
        committed = output(interrupted)
        assert 0 < len(committed) < len(expected) and committed == expected[:len(committed)]
        recovered = run(interrupted, resume=True)
        assert recovered["executionId"] == clean["executionId"] == "next-phase"
        assert output(interrupted) == expected, recovered
        repeated = run(interrupted, resume=True)
        assert repeated["executionId"] == recovered["executionId"] and output(interrupted) == expected
        digest = sha256(json.dumps(expected, separators=(",", ":")).encode()).hexdigest()
        print(json.dumps(dict(executionId=recovered["executionId"], rows=len(expected),
                              committedBeforeInterruption=len(committed), spillBytes=clean["spillBytes"],
                              outputSha256=digest)))


if __name__ == "__main__":
    main()

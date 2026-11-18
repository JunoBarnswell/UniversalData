import copy
import json
import pathlib
import subprocess
import sys
import tempfile
from product_version import PROTOCOL_VERSION

with tempfile.TemporaryDirectory(prefix="ud-child-") as folder:
    root = pathlib.Path(folder)
    source = root / "source.ndjson"
    source.write_text("".join(json.dumps({"x": i}) + "\n" for i in range(5)))
    scalar = lambda n: {"type": "int64", "default": n}
    literal = lambda n: {"literal": n, "type": "int64"}
    assignment = lambda id, name, value: {"id": id, "kind": "set", "config": {"name": name, "value": literal(value)}}
    child = {"version": PROTOCOL_VERSION, "id": "child", "parameters": {"value": {"type": "int64"}, "input": {"type": "utf8"}, "output": {"type": "utf8"}}, "variables": {"same": scalar(777)}, "connections": [{"id": name, "driver": "file", "resource": {"$parameter": name}} for name in ("input", "output")], "controlFlow": {"tasks": [assignment("setChild", "same", 999), {"id": "probe", "kind": "childProbe", "config": {}}, {"id": "flow", "kind": "dataFlow", "config": {"pipeline": "flow"}}], "constraints": [{"from": "setChild", "to": "probe", "on": "success"}, {"from": "probe", "to": "flow", "on": "success"}]}, "dataFlows": [{"id": "flow", "components": [{"id": "source", "component": "file.ndjson.source", "config": {"connection": "input", "schema": {"version": PROTOCOL_VERSION, "fields": [{"name": "x", "type": "int64", "nullable": False}]}}}, {"id": "sink", "component": "file.ndjson.destination", "config": {"connection": "output"}}], "paths": [{"from": "source", "output": 0, "to": "sink", "input": 0}]}]}
    parent = {"version": PROTOCOL_VERSION, "id": "parent", "parameters": {"parentInput": scalar(7)}, "variables": {"same": scalar(10), "parentOnly": scalar(1)}, "controlFlow": {"tasks": [{"id": "childTask", "kind": "executePackage", "config": {"package": child, "parameters": {"value": {"ref": "parentInput"}, "input": {"literal": str(source), "type": "utf8"}, "output": {"literal": "", "type": "utf8"}}}}, {"id": "root", "kind": "rootProbe", "config": {}}], "constraints": [{"from": "childTask", "to": "root", "on": "success"}]}, "dataFlows": [], "eventHandlers": {"failure": {"tasks": [assignment("handler", "parentOnly", 2)], "constraints": []}}}
    for mode in range(10):
        doc = copy.deepcopy(parent)
        target = root / f"out-{mode}.ndjson"
        doc["controlFlow"]["tasks"][0]["config"]["parameters"]["output"]["literal"] = str(target)
        if mode == 7:
            grandchild = {"version": PROTOCOL_VERSION, "id": "grandchild", "parameters": {"value": {"type": "int64"}}, "variables": {"same": scalar(0)}, "controlFlow": {"tasks": [assignment("setGrandchild", "same", 999), {"id": "grandProbe", "kind": "childProbe", "config": {}}], "constraints": [{"from": "setGrandchild", "to": "grandProbe", "on": "success"}]}, "dataFlows": []}
            child_doc = doc["controlFlow"]["tasks"][0]["config"]["package"]
            child_doc["controlFlow"]["tasks"].insert(2, {"id": "grandchildTask", "kind": "executePackage", "config": {"package": grandchild, "parameters": {"value": {"ref": "value"}}}})
            child_doc["controlFlow"]["constraints"].append({"from": "probe", "to": "grandchildTask", "on": "success"})
            child_doc["controlFlow"]["constraints"].append({"from": "grandchildTask", "to": "flow", "on": "success"})
        if mode == 8:
            doc["controlFlow"]["tasks"][0]["config"]["parameters"]["value"] = {"literal": "wrong-type", "type": "utf8"}
        if mode == 9:
            doc["controlFlow"]["tasks"][0]["config"]["parameters"]["value"] = {"ref": "unknownName"}
        if mode == 6:
            # The old container alias is rejected, rather than silently executed.
            task = doc["controlFlow"]["tasks"][0]
            task["config"] = {}
            task["children"] = {"tasks": [], "constraints": []}
        path = root / f"package-{mode}.json"
        path.write_text(json.dumps(doc))
        state = root / f"state-{mode}"
        result = subprocess.run([sys.argv[1], str(path), str(mode), str(state), "0"])
        assert result.returncode == (91 if mode == 4 else 0), (mode, result.returncode)
        if mode in (0, 4, 7):
            subprocess.run([sys.argv[1], str(path), "0", str(state), "1"], check=True)
            subprocess.run([sys.argv[1], str(path), "0", str(state), "1"], check=True)
            assert [json.loads(x) for x in target.read_text().splitlines()] == [{"x": i} for i in range(5)]
        else:
            assert not target.exists()
    print("Execute Package: literal child definition, parent expression mapping, isolated roots/connections, same Execution/context, failure/CommitUnknown/cancel propagation, child events, depth/legacy rejection and real child checkpoint termination/resume passed")

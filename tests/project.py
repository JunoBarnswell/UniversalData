import copy
import json
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-project-") as folder:
    root = pathlib.Path(folder)
    (root / "source.ndjson").write_text("".join(json.dumps({"x": i}) + "\n" for i in range(5)))
    scalar = lambda x: {"type": "int64", "default": x}
    literal = lambda x: {"literal": x, "type": "int64"}
    ref = lambda x: {"ref": x}
    assign = lambda target, value: {"id": target, "kind": "set", "config": {"name": target, "value": value}}
    project = {"version": 4, "id": "project", "parameters": {"outer": scalar(5), "same": scalar(100), "input": {"type": "utf8", "default": str(root / "source.ndjson")}}, "variables": {"counter": scalar(1), "shadow": scalar(99), "mask": scalar(111)}, "packages": [{"id": "p", "revision": 1}], "connections": [{"id": "source", "driver": "file", "resource": {"$parameter": "input"}}]}
    package = {"version": 4, "id": "p", "parameters": {"shadow": scalar(7), "same": scalar(11), "input": {"type": "utf8", "default": str(root / "wrong-source")}, "output": {"type": "utf8", "default": str(root / "direct.ndjson")}}, "variables": {name: scalar(0) for name in ["outerResult", "scopeResult", "containerResult", "maskResult", "countResult", "sameResult"]}, "connections": [{"id": "output", "driver": "file", "resource": {"$parameter": "output"}}]}
    package["variables"]["mask"] = scalar(222)
    tasks = [assign("outerResult", ref("outer")), assign("maskResult", ref("mask")), assign("sameResult", ref("same")), assign("counter", {"op": "add", "args": [ref("counter"), literal(1)]}), assign("countResult", ref("counter")), {"id": "scope", "kind": "sequence", "config": {}, "variables": {"mask": scalar(333)}, "children": {"tasks": [assign("containerResult", ref("mask")), {**assign("scopeResult", ref("mask")), "variables": {"mask": scalar(444)}}], "constraints": []}}, {"id": "flow", "kind": "dataFlow", "config": {"pipeline": "flow"}}]
    package["controlFlow"] = {"tasks": tasks, "constraints": [{"from": "counter", "to": "countResult", "on": "success"}]}
    package["dataFlows"] = [{"id": "flow", "components": [{"id": "source", "component": "file.ndjson.source", "config": {"connection": "source", "schema": {"version": 4, "fields": [{"name": "x", "type": "int64", "nullable": False}]} }}, {"id": "output", "component": "file.ndjson.destination", "config": {"connection": "output"}}], "paths": [{"from": "source", "output": 0, "to": "output", "input": 0}]}]
    environment = {"version": 4, "id": "environment", "projectId": "project", "projectParameters": {"outer": 10}, "packageParameters": {"p": {"output": str(root / "direct.ndjson")}}}
    deployment = {"version": 4, "id": "deployment", "project": {"id": "project", "revision": 1}, "environment": {"id": "environment", "revision": 1}}
    package2 = copy.deepcopy(package)
    package2["controlFlow"]["tasks"][0]["config"]["value"] = {"op": "add", "args": [ref("outer"), literal(100)]}
    project2 = copy.deepcopy(project)
    project2["packages"][0]["revision"] = 2
    env2 = copy.deepcopy(environment)
    env2["packageParameters"]["p"]["output"] = str(root / "v2.ndjson")
    # Catalog version 1 uses another destination, keeping direct-binding output.
    catalog_env1 = copy.deepcopy(environment)
    catalog_env1["packageParameters"]["p"]["output"] = str(root / "v1.ndjson")
    deploy2 = copy.deepcopy(deployment)
    deploy2["project"]["revision"] = deploy2["environment"]["revision"] = 2
    readonly = {"version": 4, "id": "readonly", "parameters": {"shadow": scalar(7)}, "project": {"id": "project", "variables": {"shadow": scalar(99)}}, "controlFlow": {"tasks": [assign("shadow", literal(1))], "constraints": []}, "dataFlows": []}
    documents = {"project.json": project, "project-v2.json": project2, "package.json": package, "package-v2.json": package2, "environment.json": catalog_env1, "environment-v2.json": env2, "deployment.json": deployment, "deployment-v2.json": deploy2, "readonly.json": readonly}
    def normalize_paths(value):
        if isinstance(value,dict):return {k:normalize_paths(v) for k,v in value.items()}
        if isinstance(value,list):return [normalize_paths(v) for v in value]
        if isinstance(value,str) and value.startswith(str(root)):return value.replace("\\","/")
        return value
    for name,document in documents.items():
        (root/name).write_text(json.dumps(normalize_paths(document)))
    subprocess.run([sys.argv[1], root.as_posix()], check=True)
    for name in ["direct.ndjson", "v1.ndjson", "v2.ndjson", "rollback.ndjson"]:
        assert [json.loads(line) for line in (root / name).read_text().splitlines()] == [{"x": i} for i in range(5)]
    print("Project/Environment/Deployment: outer scopes, nearest shadowing, read-only parameters, shared connection owner scope, typed overrides, exact revisions, restart, version switch and rollback passed")

"""The optional transport fails before I/O when libcurl is not selected."""
import json
import pathlib
import socket
import subprocess
import sys
import tempfile

with socket.socket() as server, tempfile.TemporaryDirectory(prefix="ud-http-disabled-") as folder:
    server.bind(("127.0.0.1", 0))
    server.listen()
    port = server.getsockname()[1]
    doc = {"version": 12, "id": "disabled", "connections": [
        {"id": "endpoint", "driver": "http", "resource": f"http://127.0.0.1:{port}/"}],
        "controlFlow": {"tasks": [{"id": "request", "kind": "http.request",
                                  "config": {"connection": "endpoint"}}], "constraints": []},
        "dataFlows": []}
    path = pathlib.Path(folder) / "package.json"
    path.write_text(json.dumps(doc))
    subprocess.run([sys.argv[1], str(path), "1", "12", "0"], check=True, timeout=5)
    server.setblocking(False)
    try:
        connection, _ = server.accept()
    except BlockingIOError:
        pass
    else:
        connection.close()
        raise AssertionError("HTTP-off validation performed network I/O")
    print("HTTP disabled: native Unsupported before I/O; no transport dependency required")

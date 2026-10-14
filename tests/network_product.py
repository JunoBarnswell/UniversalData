"""Native streaming transfer with actual HTTP requests and ADLS publish sequence.
External FTP/SFTP/SMTP/S3/Azure acceptance has a separate resource gate.
"""
import http.server
import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import urllib.parse

with tempfile.TemporaryDirectory(prefix="ud-network-product-") as folder:
    root = pathlib.Path(folder); data = bytes(range(256)) * 1000
    operations = []; storage = {}
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *args): pass
        def request(self):
            parsed = urllib.parse.urlsplit(self.path); query = urllib.parse.parse_qs(parsed.query)
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            operations.append((self.command, self.path, payload, self.headers.get("If-None-Match"), self.headers.get("x-ms-blob-type")))
            if self.command == "GET": response = storage.get(parsed.path, data)
            elif query.get("resource") == ["file"]: storage[parsed.path] = b""; response = b""
            elif query.get("action") == ["append"]: storage[parsed.path] += payload; response = b""
            elif query.get("action") == ["flush"]:
                assert int(query["position"][0]) == len(storage[parsed.path]); response = b""
            else: storage[parsed.path] = payload; response = b""
            self.send_response(200 if self.command == "GET" else 201)
            self.send_header("Content-Length", str(len(response))); self.end_headers(); self.wfile.write(response)
        do_GET = do_PUT = do_PATCH = request
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    upload = root / "upload.bin"; upload.write_bytes(data)
    def run(driver, operation, file, name):
        package = root / f"{name}.json"
        doc = dict(version=11, id="network-product", connections=[dict(id="remote", driver=driver, resource=f"http://127.0.0.1:{server.server_port}/{name}"), dict(id="local", driver="file", resource=str(file))],
            controlFlow=dict(tasks=[dict(id="transfer", kind=f"{driver}.{operation}", config=dict(connection="remote", file="local"))], constraints=[]), dataFlows=[])
        package.write_text(json.dumps(doc)); result = subprocess.run([sys.argv[1], str(package), "0"], capture_output=True, text=True, timeout=40)
        assert result.returncode == 0, (result.stdout,result.stderr)
    try:
        for driver in ("s3","azureBlob","adlsGen2"):
            name = driver + "-upload"; run(driver,"upload",upload,name)
            assert storage["/"+name] == data
            target = root / (driver+"-download.bin"); run(driver,"download",target,name)
            assert target.read_bytes() == data
        adls = [(method,path,body) for method,path,body,*_ in operations if path.startswith("/adlsGen2-upload")]
        assert [method for method,_,_ in adls[:3]] == ["PUT","PATCH","PATCH"]
        assert adls[1][2] == data and not adls[0][2] and not adls[2][2]
        blob = next(op for op in operations if op[0] == "PUT" and op[1].startswith("/azureBlob-upload"))
        assert blob[3] == "*" and blob[4] == "BlockBlob"
    finally: server.shutdown(); server.server_close()

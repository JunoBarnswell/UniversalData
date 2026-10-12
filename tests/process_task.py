import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

with tempfile.TemporaryDirectory(prefix="ud-process-task-") as temporary:
    root = pathlib.Path(temporary)
    def fixture(name, script="import sys;sys.exit(0)", extra=None):
        folder = root / name
        folder.mkdir()
        marker = folder / "marker.json"
        config = {"connection": "program", "arguments": ["-c", script], "workingDirectory": "cwd", "timeoutMs": 3000, "exitCodeVariable": "exit", **(extra or {})}
        doc = {"version": 6, "id": "process", "variables": {"exit": {"type": "int64", "default": -1}}, "connections": [{"id": "program", "driver": "process", "resource": sys.executable}, {"id": "cwd", "driver": "file", "resource": str(folder)}], "controlFlow": {"tasks": [{"id": "launch", "kind": "process.execute", "config": config}], "constraints": []}, "dataFlows": []}
        return folder, marker, doc
    def run(folder, marker, doc, mode=0, status=0, code=0, checkpoint=False, resume=0, exit=0):
        package = folder / "package.json"
        package.write_text(json.dumps(doc, ensure_ascii=False))
        result = subprocess.run([sys.argv[1], str(package), str(mode), str(status), str(code), str(marker), str(folder / "state") if checkpoint else "", str(resume)], capture_output=True, text=True, timeout=10)
        assert result.returncode == exit, (folder.name, result.returncode, result.stderr)
    folder, marker, doc = fixture("arguments")
    arguments = ["a b", 'quote"here', "", "中文", "backslash\\", "; echo not-a-shell"]
    doc["controlFlow"]["tasks"][0]["config"]["arguments"] = ["-c", "import json,pathlib,sys,os;pathlib.Path('marker.json').write_text(json.dumps({'args':sys.argv[1:],'cwd':os.getcwd()}))", *arguments]
    run(folder, marker, doc)
    actual = json.loads(marker.read_text())
    assert actual["args"] == arguments and actual["cwd"] == str(folder)
    for name, mode in [("default-deny", 1), ("execute-denied", 2)]:
        folder, marker, doc = fixture(name, "import pathlib;pathlib.Path('marker.json').write_text('launched')")
        run(folder, marker, doc, mode=mode, status=13, code=-1)
        assert not marker.exists()
    folder, marker, doc = fixture("exit-failure", "import sys;sys.exit(7)")
    run(folder, marker, doc, status=3, code=7)
    folder, marker, doc = fixture("expected-exit", "import sys;sys.exit(7)", {"expectedExitCode": 7})
    run(folder, marker, doc, code=7)
    folder, marker, doc = fixture("missing-executable", extra={"expectedExitCode": 127})
    doc["connections"][0]["resource"] = str(folder / "no-program")
    run(folder, marker, doc, status=3, code=-1 if os.name=="nt" else 127)
    if True:
        script = "import subprocess,sys,os,pathlib,json,time;child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);pathlib.Path('marker.json').write_text(json.dumps({'pid':os.getpid(),'child':child.pid}));time.sleep(30)"
        for name, mode, status, code in [("timeout", 0, 18, -1 if os.name=="nt" else -9), ("cancel", 3, 8, -1), ("package-deadline", 5, 18, -1 if os.name=="nt" else -9)]:
            folder, marker, doc = fixture(name, script, {"timeoutMs": 2000 if mode == 0 else 5000})
            run(folder, marker, doc, mode=mode, status=status, code=code)
            pids = json.loads(marker.read_text())
            def alive(pid):
                if os.name == "nt":
                    import ctypes
                    kernel=ctypes.WinDLL("kernel32",use_last_error=True)
                    kernel.OpenProcess.restype=ctypes.c_void_p
                    kernel.OpenProcess.argtypes=[ctypes.c_uint32,ctypes.c_int,ctypes.c_uint32]
                    kernel.GetExitCodeProcess.argtypes=[ctypes.c_void_p,ctypes.POINTER(ctypes.c_uint32)]
                    kernel.CloseHandle.argtypes=[ctypes.c_void_p]
                    handle=kernel.OpenProcess(0x1000,0,pid)
                    if not handle:return False
                    code=ctypes.c_uint32();ok=kernel.GetExitCodeProcess(handle,ctypes.byref(code));kernel.CloseHandle(handle)
                    if not ok:raise RuntimeError("Cannot read process exit status")
                    return code.value==259
                path = pathlib.Path(f"/proc/{pid}/stat")
                if not path.exists():
                    return False
                return path.read_text().split()[2] != "Z"
            limit = time.monotonic() + 2
            while any(alive(pid) for pid in pids.values()) and time.monotonic() < limit:
                time.sleep(0.01)
            assert not any(alive(pid) for pid in pids.values()), pids
    folder, marker, doc = fixture("bad-arguments")
    doc["controlFlow"]["tasks"][0]["config"]["arguments"] = [1]
    run(folder, marker, doc, status=5, code=-1)
    folder, marker, doc = fixture("readonly-output")
    doc["variables"] = {}
    doc["parameters"] = {"exit": {"type": "int64", "default": -1}}
    run(folder, marker, doc, status=13, code=-1)
    folder, marker, doc = fixture("effect-crash", "import pathlib;path=pathlib.Path('marker.json');path.write_text(str(int(path.read_text())+1) if path.exists() else '1')")
    run(folder, marker, doc, mode=4, checkpoint=True, exit=91)
    assert marker.read_text() == "1"
    run(folder, marker, doc, checkpoint=True, resume=1, status=16, code=-1)
    assert marker.read_text() == "1"
    folder, marker, doc = fixture("completed-resume", "import pathlib;path=pathlib.Path('marker.json');path.write_text(str(int(path.read_text())+1) if path.exists() else '1')")
    run(folder, marker, doc, checkpoint=True)
    run(folder, marker, doc, checkpoint=True, resume=1)
    assert marker.read_text() == "1"
    print("Execute Process: direct Unicode/empty/quoted arguments, cwd, explicit execute authorization/default deny, exit policy versus exec failure, local/package timeout, live child+descendant cancellation cleanup, type/read-only rejection and actual effect/crash/unknown/completed resume passed")

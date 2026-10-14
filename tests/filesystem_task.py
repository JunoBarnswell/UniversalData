import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-file-task-") as temporary:
    root = pathlib.Path(temporary)
    data = bytes(range(256)) * 8192
    source = root / "源文件.bin"
    source.write_bytes(data)
    def make(name, operation="copy", source_path=source):
        folder = root / name
        folder.mkdir()
        target = folder / "目标.bin"
        doc = {"version": 10, "id": "filesystem", "connections": [{"id": "source", "driver": "file", "resource": str(source_path)}, {"id": "target", "driver": "file", "resource": str(target)}], "controlFlow": {"tasks": [{"id": "operation", "kind": "file.system", "config": {"operation": operation, **({"source": "source"} if operation != "mkdir" else {}), **({"target": "target"} if operation != "delete" else {})}}], "constraints": []}, "dataFlows": []}
        return folder, target, doc
    def run(folder, doc, mode=0, resume=0, status=0, tasks=1, exit=0):
        path = folder / "package.json"
        path.write_text(json.dumps(doc, ensure_ascii=False))
        result = subprocess.run([sys.argv[1], str(path), str(mode), "" if mode == 5 else str(folder / "state"), str(resume), str(status), str(tasks)], capture_output=True, text=True)
        assert result.returncode == exit, (folder.name, result.returncode, result.stderr)
    folder, target, doc = make("copy")
    run(folder, doc)
    run(folder, doc, resume=1)
    assert target.read_bytes() == data and source.read_bytes() == data
    assert not list(folder.glob("*.part"))
    for name, mode, status in [("denied", 1, 13), ("no-authorizer", 5, 13), ("cancel", 2, 8), ("cancel-mid-copy", 7, 8), ("revoked", 3, 13)]:
        folder, target, doc = make(name)
        run(folder, doc, mode=mode, status=status)
        assert not target.exists() and not list(folder.glob("*.part"))
    folder, target, doc = make("existing-target")
    target.write_bytes(b"user-data")
    run(folder, doc, status=15)
    assert target.read_bytes() == b"user-data"
    folder, target, doc = make("existing-part")
    part = target.with_name(target.name + ".ud-task.part")
    part.write_bytes(b"user-part")
    run(folder, doc, status=15)
    assert part.read_bytes() == b"user-part" and not target.exists()
    folder, target, doc = make("move", operation="move", source_path=root / "move-source")
    moving = pathlib.Path(doc["connections"][0]["resource"])
    moving.write_bytes(data)
    run(folder, doc)
    run(folder, doc, resume=1)
    assert not moving.exists() and target.read_bytes() == data
    folder, target, doc = make("delete", operation="delete", source_path=root / "delete-source")
    deleting = pathlib.Path(doc["connections"][0]["resource"])
    deleting.write_bytes(b"delete-me")
    run(folder, doc)
    run(folder, doc, resume=1)
    assert not deleting.exists()
    folder, target, doc = make("delete-denied", operation="delete", source_path=root / "protected")
    protected = pathlib.Path(doc["connections"][0]["resource"])
    protected.write_bytes(b"keep-me")
    run(folder, doc, mode=6, status=13)
    assert protected.read_bytes() == b"keep-me"
    folder, target, doc = make("mkdir", operation="mkdir")
    run(folder, doc)
    run(folder, doc, resume=1)
    assert target.is_dir()
    folder, target, doc = make("unknown-operation")
    doc["controlFlow"]["tasks"][0]["config"]["operation"] = "recursiveDelete"
    run(folder, doc, status=5)
    assert not (folder / "state").exists()
    folder, target, doc = make("unknown-connection")
    doc["controlFlow"]["tasks"][0]["config"]["source"] = "absent"
    run(folder, doc, status=5)
    assert not (folder / "state").exists()
    folder, target, doc = make("missing-source", source_path=root / "missing-source")
    run(folder, doc, status=3)
    assert not target.exists()
    folder, target, doc = make("traversal", source_path=root / "copy" / ".." / source.name)
    run(folder, doc, status=2)
    assert not target.exists()
    if os.name != "nt":
        symlink = root / "symlink"
        symlink.symlink_to(source)
        folder, target, doc = make("symlink-source", source_path=symlink)
        run(folder, doc, status=5)
        fifo = root / "fifo"
        os.mkfifo(fifo)
        folder, target, doc = make("fifo-source", source_path=fifo)
        run(folder, doc, status=5)
        directory_link = root / "dir-link"
        directory_link.symlink_to(root / "copy", target_is_directory=True)
        folder, target, doc = make("symlink-parent", source_path=directory_link / "目标.bin")
        run(folder, doc, status=5)
    folder, target, doc = make("copy-crash")
    run(folder, doc, mode=4, exit=91)
    assert target.read_bytes() == data
    run(folder, doc, resume=1, status=16)
    assert target.read_bytes() == data
    print("File System Task: Unicode/binary copy, atomic no-overwrite move, delete/mkdir, manager/auth/delete grants, completed resume, real post-effect crash CommitUnknown, cancellation/revocation cleanup, target/part preservation, invalid config/path/symlink/special-file rejection passed")

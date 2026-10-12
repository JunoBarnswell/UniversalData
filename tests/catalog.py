import pathlib
import shutil
import subprocess
import sys
import tempfile


def run(folder, mode):
    subprocess.run([sys.argv[1], str(folder), mode], check=True)


with tempfile.TemporaryDirectory(prefix="ud-catalog-") as temporary:
    root = pathlib.Path(temporary) / "store"
    run(root, "create")
    run(root, "read")
    package_files = sorted(root.glob("catalog-2-*-00000000000000000001.udc"))
    assert len(package_files) == 2
    # Tenant strings are length-prefixed in the checksummed envelope.
    file = next(p for p in package_files if b"tenant-a" in p.read_bytes())
    original = file.read_bytes()
    broken = bytearray(original)
    broken[-1] ^= 1
    file.write_bytes(broken)
    run(root, "corrupt")
    file.write_bytes(original)
    copy = root / "renamed.udc"
    shutil.copyfile(file, copy)
    run(root, "renamed")
    copy.unlink()
    part = root / (file.name.replace("00000000000000000001", "00000000000000000003") + ".part")
    part.write_bytes(b"preexisting-user-file")
    run(root, "staging")
    assert part.read_bytes() == b"preexisting-user-file"
    assert not part.with_suffix("").exists()
    print("Catalog: immutable revisions, restart, tenant isolation, default deny/revocation, native Package validation, event envelope, corruption/rename detection and staging preservation passed")

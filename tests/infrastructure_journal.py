"""Run actual native segmented recovery, corruption and retirement acceptance."""
import hashlib
import os
import pathlib
import shutil
import struct
import subprocess
import sys
import tempfile


def prior_tail(directory):
    tail = (directory / "journal.tail").read_bytes()
    sequence, = struct.unpack_from("<Q", tail, 8)
    segment = sorted(directory.glob("segment-*.udg"))[-1].read_bytes()
    offset = 0
    while offset < len(segment):
        length, = struct.unpack_from("<Q", segment, offset)
        offset += 8
        if struct.unpack_from("<Q", segment, offset + 8)[0] == sequence:
            previous = tail[:8] + struct.pack("<Q", sequence - 1) + segment[offset + 16:offset + 48]
            return previous + hashlib.sha256(previous).digest()
        offset += length
    raise AssertionError("last acknowledged frame was not found")


def reopen_with_tail_reader(directory, run):
    if os.name != "nt":
        return run("journal-read", directory)

    import ctypes
    from ctypes import wintypes
    import msvcrt

    tail = directory / "journal.tail"
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    open_file = kernel.CreateFileW
    open_file.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                          wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
    open_file.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    generic_read = 0x80000000
    share_read_write_delete = 0x7
    open_existing = 3
    handle = open_file(str(tail), generic_read, share_read_write_delete, None, open_existing, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except OSError:
        kernel.CloseHandle(handle)
        raise
    with os.fdopen(descriptor, "rb") as reader:
        original = reader.read()
        message = run("journal-read", directory)
        reader.seek(0)
        assert reader.read() == original
        assert tail.read_bytes() != original
    return message


runner = pathlib.Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="ud-journal-") as folder:
    root = pathlib.Path(folder)
    live = root / "live"
    def run(mode, directory):
        result = subprocess.run([runner, mode, directory], capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, (mode, result.returncode, result.stdout, result.stderr)
        return result.stdout.strip()
    print(run("journal", live))
    confirmation = root / "confirmation"
    shutil.copytree(live, confirmation)
    tail = confirmation / "journal.tail"
    expected_tail = tail.read_bytes()
    tail.write_bytes(prior_tail(confirmation))
    run("journal-confirm", confirmation)
    assert tail.read_bytes() == expected_tail
    print(reopen_with_tail_reader(live, run))
    assert len(list(live.glob("*.udg"))) <= 1
    assert 1 <= len(list(live.glob("*.uds"))) <= 2
    assert not list(live.glob("*.udj"))
    for scenario in ("tail", "missing-segment", "snapshot", "head", "acknowledgement"):
        target = root / scenario
        shutil.copytree(live, target)
        if scenario == "missing-segment":
            list(target.glob("*.udg"))[-1].unlink()
        else:
            path = (list(target.glob("*.udg"))[-1] if scenario == "tail" else
                    sorted(target.glob("*.uds"))[-1] if scenario == "snapshot" else
                    target / ("journal.head" if scenario == "head" else "journal.tail"))
            data = bytearray(path.read_bytes())
            if scenario == "tail":
                del data[-3:]
            else:
                data[-1] ^= 1
            path.write_bytes(data)
        run("journal-corrupt", target)
    print("Journal corruption: torn frame, missing acknowledged segment, altered snapshot/head/tail fail closed")

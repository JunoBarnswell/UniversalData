"""Exercise Windows application-local native dependencies with a system-only PATH."""

import argparse
import ctypes as c
import os
from pathlib import Path
import subprocess
import sys


def load(library, version, plugins):
    c.windll.kernel32.SetErrorMode(3)  # Report loader failures as test errors, without a dialog.
    native = c.CDLL(str(library.resolve()))
    native.ud_version.restype = c.c_char_p
    assert native.ud_version().decode() == version, f"Unexpected SDK version in {library}"
    if not plugins:
        return

    context = c.c_void_p()
    native.ud_error.argtypes = [c.c_void_p]
    native.ud_error.restype = c.c_char_p
    native.ud_plugin_load.argtypes = [c.c_void_p, c.c_char_p, c.c_uint64, c.POINTER(c.c_void_p)]
    assert native.ud_context_create(None, c.byref(context)) == 0
    try:
        for path in plugins:
            plugin = c.c_void_p()
            status = native.ud_plugin_load(context, str(path.resolve()).encode("utf-8"), 0, c.byref(plugin))
            assert status == 0, f"{path.name} failed to load: {native.ud_error(context).decode()}"
            assert native.ud_plugin_unload(plugin) == 0
    finally:
        assert native.ud_context_destroy(context) == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", nargs="+", required=True, type=lambda path: Path(path).resolve())
    parser.add_argument("--version", required=True)
    parser.add_argument("--plugins", nargs="*", default=[], type=lambda path: Path(path).resolve())
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()
    if args.child:
        load(args.library[0], args.version, args.plugins)
        return

    windows = Path(os.environ["SystemRoot"])
    environment = {**os.environ, "PATH": os.pathsep.join((str(windows / "System32"), str(windows)))}
    for index, library in enumerate(args.library):
        # Separate processes prevent a previously loaded DLL from masking a missing file.
        plugins = args.plugins if index == 0 else []
        subprocess.run([sys.executable, "-I", __file__, "--child", "--library", str(library),
                        "--version", args.version, "--plugins", *(str(path) for path in plugins)],
                       cwd=windows, env=environment, timeout=20, check=True)
        print(f"{library} loaded with system-only PATH ({len(plugins)} SQL plugins)")


if __name__ == "__main__":
    main()

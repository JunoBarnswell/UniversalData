"""Repository-specific contract and packaging checks; never invokes a compiler."""
import json
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
import ast
from product_model import generate

root = pathlib.Path(__file__).resolve().parents[1]
ignored = {".git", "build", "bin", "obj", "target", "artifacts", "_deps", "work", "output", "node_modules", "wwwroot"}


def source_files(directory):
    for folder, subdirectories, filenames in os.walk(directory):
        subdirectories[:] = [name for name in subdirectories if name not in ignored]
        for name in filenames:
            if name not in ignored:
                yield pathlib.Path(folder) / name


errors = []
errors.extend("Product projection differs: " + name for name in generate(True))
for path in source_files(root):
    if path.suffix in {".json"}: json.loads(path.read_text(encoding="utf-8"))
    if path.suffix in {".xml", ".csproj"}: ET.parse(path)
    if path.suffix == ".py": ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    if path.suffix in {".c", ".cpp", ".h"}:
        content = path.read_text(encoding="utf-8")
        for match in re.finditer(r'ud_bytes_put\([^,]*,\s*("(?:[^"\\]|\\.)*")\s*,\s*(\d+)\s*\)', content):
            # C/Python literals share the simple escapes used in codec constants.
            literal = ast.literal_eval(match.group(1)).encode("utf-8")
            count = int(match.group(2))
            # The explicit empty-string/one-byte append writes the C NUL
            # terminator after a separately counted text payload.
            if len(literal) != count and not (literal == b"" and count == 1):
                errors.append(f"wire literal length mismatch: {path}:{content[:match.start()].count(chr(10)) + 1}")
        if "components/foundation/src" in path.as_posix() and re.search(r'#include.*ud/(data|compute|etl|query|plugin)\.h', content):
            errors.append(f"foundation reverse dependency: {path}")
        if "examples" in path.parts and re.search(r'#include.*(internal|components/)', content):
            errors.append(f"example uses private API: {path}")
sources = "\n".join(p.read_text(encoding="utf-8") for folder in ("components", "engines", "connectors", "catalog") for p in source_files(root / folder) if p.suffix in {".c", ".cpp"})
for header in (root / "include" / "ud").glob("*.h"):
    for symbol in re.findall(r'UD_API\s+[^;]*?\b(ud_\w+)\s*\(', header.read_text(encoding="utf-8")):
        if not re.search(r'\b' + symbol + r'\s*\([^;]*?\)\s*\{', sources, re.S): errors.append(f"missing implementation: {symbol}")
forbidden = re.compile(r'\bud_(?:job|dag|run)_\w+|\bud_snapshot_latest\b')
for folder in ('include', 'components', 'engines', 'connectors', 'hosts', 'bindings'):
    for path in source_files(root / folder):
        if path.suffix in {'.c', '.h', '.cs', '.java'}:
            if forbidden.search(path.read_text(encoding='utf-8')): errors.append(f'removed execution API present: {path}')
if not (root / "licenses" / "Apache-2.0.txt").is_file(): errors.append("missing Arrow license")
if errors:
    print("\n".join(errors)); sys.exit(1)
print("static contract checks passed: public functions, component boundaries, JSON/XML, notices")

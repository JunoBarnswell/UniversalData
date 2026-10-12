"""Repository-specific contract and packaging checks; never invokes a compiler."""
import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
import ast

root = pathlib.Path(__file__).resolve().parents[1]
errors = []
for path in root.rglob("*"):
    if not path.is_file() or any(p in {".git", "build", "bin", "obj", "target", "artifacts", "_deps", "work", "output"} for p in path.relative_to(root).parts):
        continue
    if path.suffix in {".json"}: json.loads(path.read_text(encoding="utf-8"))
    if path.suffix in {".xml", ".csproj"}: ET.parse(path)
    if path.suffix == ".py": ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    if path.suffix in {".c", ".h"}:
        content = path.read_text(encoding="utf-8")
        for match in re.finditer(r'ud_bytes_put\([^,]*,("(?:[^"\\]|\\.)*"),(\d+)\)', content):
            # C/Python literals share the simple escapes used in codec constants.
            literal = ast.literal_eval(match.group(1)).encode("utf-8")
            if len(literal) != int(match.group(2)):
                errors.append(f"wire literal length mismatch: {path}:{content[:match.start()].count(chr(10)) + 1}")
        if "components/foundation/src" in path.as_posix() and re.search(r'#include.*ud/(data|compute|etl|query|plugin)\.h', content):
            errors.append(f"foundation reverse dependency: {path}")
        if "examples" in path.parts and re.search(r'#include.*(internal|components/)', content):
            errors.append(f"example uses private API: {path}")
sources = "\n".join(p.read_text(encoding="utf-8") for folder in ("components", "engines") for p in (root / folder).rglob("*.c"))
for header in (root / "include" / "ud").glob("*.h"):
    for symbol in re.findall(r'UD_API\s+[^;]*?\b(ud_\w+)\s*\(', header.read_text(encoding="utf-8")):
        if not re.search(r'\b' + symbol + r'\s*\([^;]*?\)\s*\{', sources, re.S): errors.append(f"missing implementation: {symbol}")
if not (root / "licenses" / "Apache-2.0.txt").is_file(): errors.append("missing Arrow license")
if errors:
    print("\n".join(errors)); sys.exit(1)
print("static contract checks passed: public functions, component boundaries, JSON/XML, notices")

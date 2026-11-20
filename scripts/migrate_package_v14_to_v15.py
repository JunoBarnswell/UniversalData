"""Write an ABI 15 Package copy. Checkpoint and Wire state belong to their original runtime."""

import argparse
import json
from pathlib import Path

# Schema envelopes can occur in built-in SQL/typed components, parameter options,
# cached reference definitions and managed component port settings. Actual data,
# defaults and user-supplied layout values can also contain a property 'version'.
SCHEMA_KEYS = {
    "schema", "schemas", "sourceSchema", "targetSchema", "inputSchema", "outputSchema",
    "expectedSchema", "resultSchema", "keySchema", "beforeSchema", "afterSchema",
}
VALUE_KEYS = {"rows", "default", "value", "values", "layout"}


def migrate_document(value, schema=False):
    if isinstance(value, list):
        for item in value:
            migrate_document(item, schema)
    if not isinstance(value, dict):
        return
    if schema and value.get("version") == 14 and isinstance(value.get("fields"), list):
        value["version"] = 15
    for key, child in value.items():
        if key == "config" and value.get("kind") == "executePackage" and isinstance(child, dict) and "package" in child:
            migrate_package(child["package"])
        elif key not in VALUE_KEYS:
            migrate_document(child, key in SCHEMA_KEYS)


def migrate_package(package):
    if (not isinstance(package, dict) or package.get("version") != 14
            or not isinstance(package.get("id"), str)
            or not isinstance(package.get("controlFlow"), dict)):
        raise ValueError("Input must be a version 14 UniversalData Package")
    flows = package.get("dataFlows")
    if (not isinstance(flows, list)
            and not (flows is None and isinstance(package.get("templates"), list)
                     and isinstance(package.get("instances"), list))):
        raise ValueError("Input must be a version 14 UniversalData Package")
    package["version"] = 15
    migrate_document(package)
    return package


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        package = migrate_package(json.loads(args.source.read_text(encoding="utf-8-sig")))
        content = json.dumps(package, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        with args.destination.open("x", encoding="utf-8", newline="\n") as output:
            output.write(content)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")


if __name__ == "__main__":
    main()

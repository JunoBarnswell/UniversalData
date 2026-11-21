"""Copy the exact restored dependency closure into the distributable local feed."""
import argparse
import json
import pathlib
import shutil

parser = argparse.ArgumentParser()
parser.add_argument("--assets", required=True, action="append", type=pathlib.Path)
parser.add_argument("--output", required=True, type=pathlib.Path)
args = parser.parse_args()
folders = []
packages = {}
# The installed consumer targets the same .NET framework as the public SDK.
# restore. These exact packs already participated in the actual SDK build.
for path in args.assets:
    assets = json.loads(path.read_text(encoding="utf-8"))
    folders.extend(pathlib.Path(folder) for folder in assets["packageFolders"])
    packages.update({key.lower(): value["path"] for key, value in assets["libraries"].items()
                     if value["type"] == "package"})
    for framework in assets.get("project", {}).get("frameworks", {}).values():
        for package in framework.get("downloadDependencies", []):
            versions = [part.strip() for part in package["version"].strip("[]").split(",")]
            assert len(versions) == 2 and versions[0] == versions[1], "Targeting/host pack must be locked"
            identity = package["name"].lower() + "/" + versions[0]
            packages[identity] = identity
args.output.mkdir(parents=True, exist_ok=True)
copied = []
for identity, relative in sorted(packages.items()):
    name, version = identity.rsplit("/", 1)
    filename = name + "." + version + ".nupkg"
    source = next((folder / relative / filename for folder in folders if (folder / relative / filename).is_file()), None)
    if source is None:
        raise FileNotFoundError("Resolved dependency archive is absent: " + identity)
    shutil.copyfile(source, args.output / filename)
    copied.append(dict(id=name, version=version, archive=filename))
print(json.dumps(copied))

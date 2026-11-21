"""Package the managed SDK and the current platform's actual, self-contained Worker."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "bindings/dotnet/UniversalData/UniversalData.csproj"
WORKER = ROOT / "products/UniversalData.Worker/UniversalData.Worker.csproj"
RIDS = ("win-x64", "linux-x64")
DRIVERS = ("sqlite", "postgresql", "mysql", "mariadb", "odbc", "oracle_oci")
ICEBERG_RUNTIMES = {
    "linux-x64": {"libiceberg.so", "libiceberg_data.so", "libiceberg_bundle.so", "libiceberg_rest.so",
                  "libarrow.so.2500", "libparquet.so.2500"},
    "win-x64": {"iceberg.dll", "iceberg_data.dll", "iceberg_bundle.dll", "iceberg_rest.dll"},
}


def nuspec(path, identity, version, description, files, dependencies=()):
    package = ET.Element("package")
    metadata = ET.SubElement(package, "metadata")
    for name, value in (("id", identity), ("version", version), ("authors", "UniversalData"),
                        ("description", description)):
        ET.SubElement(metadata, name).text = value
    ET.SubElement(metadata, "license", type="expression").text = "MIT"
    if dependencies:
        group = ET.SubElement(ET.SubElement(metadata, "dependencies"), "group", targetFramework="net8.0")
        for name, dependency_version in dependencies:
            ET.SubElement(group, "dependency", id=name, version=f"[{dependency_version}]")
    mapping = ET.SubElement(package, "files")
    for source, target in files:
        ET.SubElement(mapping, "file", src=source, target=target)
    ET.ElementTree(package).write(path, encoding="utf-8", xml_declaration=True)


def pack(spec, output, base):
    subprocess.run(["dotnet", "pack", str(SDK), "--no-build", "--no-restore", "-c", "Release",
                    "-o", str(output), f"-p:NuspecFile={spec}", f"-p:NuspecBasePath={base}"],
                   cwd=ROOT, check=True)


def assemble(args):
    product = json.loads((ROOT / "metadata/product.json").read_text())["product"]
    version = product["sdkVersion"]
    identity = "UniversalData.Runtime." + args.rid
    extension = ".dll" if args.rid == "win-x64" else ".so"
    library = "ud_data.dll" if args.rid == "win-x64" else "libud_data.so"
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ud-sdk-pack-") as folder:
        root = Path(folder)
        worker = root / "worker"
        subprocess.run(["dotnet", "publish", str(WORKER), "-c", "Release", "-r", args.rid,
                        "--self-contained", "true", "-o", str(worker)], cwd=ROOT, check=True)
        native = root / "native"
        native.mkdir()
        shutil.copyfile(args.native / library, native / library)

        enabled = []
        for driver in DRIVERS:
            plugin = args.plugins / ("ud_connector_" + driver + extension)
            if not plugin.is_file():
                if args.complete or driver == "sqlite":
                    raise FileNotFoundError(f"No packaged {driver} connector for {args.rid}")
                continue
            enabled.append(driver)
            shutil.copyfile(plugin, native / plugin.name)
        if args.dependencies:
            for dependency in sorted(args.dependencies.iterdir()):
                if not dependency.is_file():
                    continue
                target = native / dependency.name
                if target.exists() and target.read_bytes() != dependency.read_bytes():
                    raise ValueError(f"Conflicting runtime dependency {dependency.name}")
                shutil.copyfile(dependency, target)
        if args.complete:
            available = {path.name.lower() for path in native.iterdir()}
            missing = ICEBERG_RUNTIMES[args.rid] - available
            if missing:
                raise FileNotFoundError(f"Missing {args.rid} Iceberg runtime files: {', '.join(sorted(missing))}")
        for asset in native.iterdir():
            shutil.copyfile(asset, worker / asset.name)

        targets = root / f"{identity}.targets"
        targets.write_text(f"""<Project>
  <ItemGroup Condition="'$(RuntimeIdentifier)' == '' or '$(RuntimeIdentifier)' == '{args.rid}'">
    <None Include="$(MSBuildThisFileDirectory)../runtimes/{args.rid}/worker/**/*"
      Link="runtimes/{args.rid}/worker/%(RecursiveDir)%(Filename)%(Extension)"
      CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
  </ItemGroup>
  <Target Name="UniversalDataSetWorkerExecutable{args.rid.replace('-', '')}" AfterTargets="CopyFilesToOutputDirectory;Publish"
    Condition="'{args.rid}' == 'linux-x64' and $([MSBuild]::IsOSPlatform('Linux'))">
    <Exec Command="chmod u+x &quot;$(TargetDir)runtimes/{args.rid}/worker/ud_worker&quot;"
      Condition="Exists('$(TargetDir)runtimes/{args.rid}/worker/ud_worker')" />
    <Exec Command="chmod u+x &quot;$(PublishDir)runtimes/{args.rid}/worker/ud_worker&quot;"
      Condition="'$(PublishDir)' != '' and Exists('$(PublishDir)runtimes/{args.rid}/worker/ud_worker')" />
  </Target>
</Project>
""", encoding="utf-8")
        spec = root / "runtime.nuspec"
        files = [("worker/**/*", f"runtimes/{args.rid}/worker"),
                 ("native/*", f"runtimes/{args.rid}/native"), (targets.name, "buildTransitive")]
        if args.licenses:
            shutil.copytree(args.licenses, root / "licenses")
            files.append(("licenses/**/*", "licenses"))
        elif args.complete:
            raise ValueError("Complete runtime packages require native dependency notices")
        nuspec(spec, identity, version, "Self-contained UniversalData Worker and native runtime", files)
        pack(spec, args.output, root)

        subprocess.run(["dotnet", "build", str(SDK), "-c", "Release"], cwd=ROOT, check=True)
        if args.single_rid_feed:
            sdk_spec = ET.parse(SDK.with_suffix(".nuspec"))
            sdk_spec.find("metadata/version").text = version
            group = sdk_spec.find("metadata/dependencies/group")
            for dependency in list(group):
                package = dependency.get("id")
                if package == identity:
                    dependency.set("version", f"[{version}]")
                elif package in product["dotnetPackages"]:
                    dependency.set("version", f'[{product["dotnetPackages"][package]}]')
                elif package in {"UniversalData.Runtime." + rid for rid in RIDS}:
                    group.remove(dependency)
            local_spec = root / "UniversalData.nuspec"
            sdk_spec.write(local_spec, encoding="utf-8", xml_declaration=True)
            pack(local_spec, args.output, SDK.parent)
            subprocess.run([sys.executable, str(ROOT / "packaging/nuget_feed.py"),
                            "--assets", str(SDK.parent / "obj/project.assets.json"),
                            "--output", str(args.output)], cwd=ROOT, check=True)
        else:
            subprocess.run(["dotnet", "pack", str(SDK), "-c", "Release", "--no-build", "--no-restore",
                            "-p:IncludeUniversalDataRuntimeDependencies=true", "-o", str(args.output)], cwd=ROOT, check=True)
    archives = [args.output / f"{name}.{version}.nupkg" for name in ("UniversalData", identity)]
    manifest = {"version": version, "rid": args.rid, "drivers": enabled,
                "packages": [{"name": path.name, "bytes": path.stat().st_size,
                              "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in archives]}
    if any(item["bytes"] > 250_000_000 for item in manifest["packages"]):
        raise ValueError("A NuGet package exceeds 250 MB")
    (args.output / f"sdk-packages-{args.rid}.json").write_text(json.dumps(manifest, indent=2) + "\n",
                                                               encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rid", choices=RIDS, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--plugins", type=Path, required=True)
    parser.add_argument("--dependencies", type=Path)
    parser.add_argument("--licenses", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--complete", action="store_true")
    parser.add_argument("--single-rid-feed", action="store_true",
                        help="Build a local feed whose SDK depends only on the selected runtime")
    assemble(parser.parse_args())

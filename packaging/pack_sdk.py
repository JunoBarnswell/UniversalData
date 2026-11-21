"""Package the managed SDK and the current platform's actual, self-contained Worker."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "bindings/dotnet/UniversalData/UniversalData.csproj"
WORKER = ROOT / "products/UniversalData.Worker/UniversalData.Worker.csproj"
RIDS = ("win-x64", "linux-x64")
DRIVERS = ("sqlite", "postgresql", "mysql", "mariadb", "odbc", "oracle_oci",
           "kafka", "rabbitmq", "rocketmq", "activemq")
ICEBERG_RUNTIMES = {
    "linux-x64": {"libiceberg.so", "libiceberg_data.so", "libiceberg_bundle.so", "libiceberg_rest.so",
                  "libarrow.so.2500", "libparquet.so.2500"},
    "win-x64": {"iceberg.dll", "iceberg_data.dll", "iceberg_bundle.dll", "iceberg_rest.dll"},
}


def nuspec(path, identity, version, description, files, dependencies=(), *, target_framework):
    package = ET.Element("package")
    metadata = ET.SubElement(package, "metadata")
    for name, value in (("id", identity), ("version", version), ("authors", "UniversalData"),
                        ("description", description)):
        ET.SubElement(metadata, name).text = value
    ET.SubElement(metadata, "license", type="expression").text = "MIT"
    if dependencies:
        group = ET.SubElement(ET.SubElement(metadata, "dependencies"), "group", targetFramework=target_framework)
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


def package_tree(source, root, target_root):
    """Map each input file to a unique target while preserving its relative path."""
    files = []
    for path in sorted(source.rglob("*")):
        if path.is_file():
            relative = path.relative_to(source)
            target = "/".join((target_root, *relative.parts))
            files.append((path.relative_to(root).as_posix(), target))
    return files


def copy_asset(source, destination):
    """Copy one supplied runtime asset, rejecting same-name replacements."""
    if destination.exists():
        if destination.read_bytes() != source.read_bytes():
            raise ValueError(f"Conflicting runtime dependency {destination.name}")
        return
    shutil.copyfile(source, destination)


def assemble(args):
    product = json.loads((ROOT / "metadata/product.json").read_text())["product"]
    version = product["sdkVersion"]
    target_framework = product["dotnetTargetFramework"]
    identity = "UniversalData.Runtime." + args.rid
    extension = ".dll" if args.rid == "win-x64" else ".so"
    library = "ud_data.dll" if args.rid == "win-x64" else "libud_data.so"
    args.output.mkdir(parents=True, exist_ok=True)
    subprocess.run(["dotnet", "build", str(SDK), "-c", "Release"], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix="ud-sdk-pack-") as folder:
        root = Path(folder)
        worker = root / "worker"
        subprocess.run(["dotnet", "publish", str(WORKER), "-c", "Release", "-r", args.rid,
                        "--self-contained", "true", "-o", str(worker)], cwd=ROOT, check=True)
        native = root / "native"
        native.mkdir()
        shutil.copyfile(args.native / library, native / library)

        enabled = []
        plugin_names = set()
        for driver in DRIVERS:
            plugin = args.plugins / ("ud_connector_" + driver + extension)
            if not plugin.is_file():
                if args.complete or driver == "sqlite":
                    raise FileNotFoundError(f"No packaged {driver} connector for {args.rid}")
                continue
            enabled.append(driver)
            plugin_names.add(plugin.name)
            copy_asset(plugin, native / plugin.name)
        # Windows CMake installs the DLLs discovered from the enabled plugin
        # import tables beside the plugin. They are part of the real closure,
        # so carry every platform binary supplied by that install tree.
        for asset in sorted(args.plugins.iterdir()):
            if (asset.is_file() and asset.name not in plugin_names
                    and asset.suffix.lower() in {".dll", ".so", ".dylib"}):
                copy_asset(asset, native / asset.name)
        if args.dependencies:
            for dependency in sorted(args.dependencies.iterdir()):
                if not dependency.is_file():
                    continue
                target = native / dependency.name
                copy_asset(dependency, target)
        if args.complete:
            available = {path.name.lower() for path in native.iterdir()}
            missing = ICEBERG_RUNTIMES[args.rid] - available
            if missing:
                raise FileNotFoundError(f"Missing {args.rid} Iceberg runtime files: {', '.join(sorted(missing))}")
        for asset in native.iterdir():
            copy_asset(asset, worker / asset.name)

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
        runtime_root = f"runtimes/{args.rid}"
        files = package_tree(worker, root, f"{runtime_root}/worker")
        files.extend(package_tree(native, root, f"{runtime_root}/native"))
        files.append((targets.name, "buildTransitive"))
        if args.licenses:
            licenses = root / "licenses"
            shutil.copytree(args.licenses, licenses)
            license_files = package_tree(licenses, root, "licenses")
            if args.complete and not license_files:
                raise ValueError("Complete runtime packages require non-empty native dependency notices")
            files.extend(license_files)
        elif args.complete:
            raise ValueError("Complete runtime packages require native dependency notices")
        nuspec(spec, identity, version, "Self-contained UniversalData Worker and native runtime", files,
               target_framework=target_framework)
        pack(spec, args.output, root)

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
        native_assets = sorted(path.name for path in native.iterdir())
    archives = [args.output / f"{name}.{version}.nupkg" for name in ("UniversalData", identity)]
    worker_executable = "ud_worker.exe" if args.rid == "win-x64" else "ud_worker"
    required_entries = {f"{runtime_root}/worker/{worker_executable}", f"{runtime_root}/native/{library}"}
    required_entries.update(f"{runtime_root}/native/ud_connector_{driver}{extension}" for driver in enabled)
    with zipfile.ZipFile(archives[1]) as runtime:
        missing = required_entries.difference(runtime.namelist())
        if missing:
            raise RuntimeError(f"The {args.rid} runtime package is missing: {', '.join(sorted(missing))}")
    manifest = {"version": version, "rid": args.rid, "drivers": enabled,
                "nativeAssets": native_assets,
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

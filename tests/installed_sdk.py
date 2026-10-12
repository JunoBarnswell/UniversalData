"""Consume the actual distributable ZIP outside the source/build tree."""
import argparse
import ctypes
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--bundle", required=True, type=pathlib.Path)
parser.add_argument("--work", required=True, type=pathlib.Path)
parser.add_argument("--evidence", required=True, type=pathlib.Path)
parser.add_argument("--cmake", default="cmake")
parser.add_argument("--dotnet", default="dotnet")
parser.add_argument("--maven", default="mvn")
parser.add_argument("--java", default="java")
arguments = parser.parse_args()
work = arguments.work.resolve()
bundle = arguments.bundle.resolve()
assert not work.is_relative_to(ROOT) and not work.exists(), "Use a fresh directory outside the repository."
work.mkdir(parents=True)
commands = []
native_name = "ud_data.dll" if os.name == "nt" else "libud_data.so"
environment = dict(os.environ)
environment["PYTHONUTF8"] = "1"
environment["NUGET_PACKAGES"] = str(work / "nuget-cache")
environment["PATH"] = os.pathsep.join(p for p in environment.get("PATH", "").split(os.pathsep)
                                    if p and not pathlib.Path(p).resolve().is_relative_to(ROOT)
                                    and not (pathlib.Path(p) / native_name).is_file())
environment.pop("LD_LIBRARY_PATH", None)
for tool in ("cmake", "dotnet", "maven", "java"):
    executable_path = shutil.which(getattr(arguments, tool), path=environment["PATH"])
    if not executable_path:
        raise FileNotFoundError("Missing consumer tool: " + tool)
    setattr(arguments, tool, executable_path)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(name, command, env=None):
    command = list(map(str, command))
    log = work / (name + ".log")
    with log.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, cwd=work, env=environment if env is None else env,
                                stdout=output, stderr=subprocess.STDOUT, timeout=240)
    commands.append({"step": name, "command": command, "exitCode": result.returncode})
    if result.returncode:
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
        errors = [line for line in lines if re.search(r"error|failed|exception|assert", line, re.I)]
        raise RuntimeError(f"{name} exit={result.returncode}; log={log}\n" + "\n".join((errors or lines[-12:])[:12]))
    print(name + " exit=0", flush=True)


unpacked = work / "bundle"
unpacked.mkdir()
with zipfile.ZipFile(bundle) as archive:
    names = [entry.filename for entry in archive.infolist() if not entry.is_dir()]
    assert len(names) == len(set(names)), "ZIP contains duplicate paths."
    for name in names:
        path = pathlib.PurePosixPath(name.replace("\\", "/"))
        assert not path.is_absolute() and ".." not in path.parts and ":" not in name, "Invalid ZIP path."
    archive.extractall(unpacked)
manifest = json.loads((unpacked / "manifest.json").read_text(encoding="utf-8"))
version = manifest["sdkVersion"]
assert manifest["schemaVersion"] == 1 and manifest["abiVersion"] == 4 and manifest["configuration"] == "Release"
assert manifest["runtimeIdentifier"] == ("win-x64" if os.name == "nt" else "linux-x64")
fingerprint = subprocess.check_output([sys.executable, str(ROOT / "scripts/release_guard.py"), "--fingerprint"],
                                      env=environment, text=True).strip()
assert manifest["sourceFingerprint"] == fingerprint, "Bundle was produced from a different source state."
listed = {entry["path"] for entry in manifest["files"]}
assert len(listed) == len(manifest["files"]) and set(names) == listed | {"manifest.json"}
for entry in manifest["files"]:
    path = unpacked / entry["path"]
    assert path.stat().st_size == entry["bytes"] and digest(path) == entry["sha256"], entry["path"]
sdk = unpacked / "sdk"
packages = unpacked / "packages"
native = sdk / ("bin" if os.name == "nt" else "lib")
library = native / native_name
assert library.is_file() and not (sdk / "include/ud/query.h").exists()
facade = ctypes.CDLL(str(library))
facade.ud_version.restype = ctypes.c_char_p
assert facade.ud_version().decode("ascii") == version
for symbol in ("ud_job_create", "ud_dag_create", "ud_run_create", "ud_plugin_execute", "ud_snapshot_latest"):
    assert not hasattr(facade, symbol), "Removed ABI export remains: " + symbol


class ContextOptions(ctypes.Structure):
    _fields_ = [("size", ctypes.c_uint32), ("abi", ctypes.c_uint32), ("quota", ctypes.c_uint64),
                ("user", ctypes.c_void_p), ("allocate", ctypes.c_void_p), ("deallocate", ctypes.c_void_p)]


facade.ud_context_create.argtypes = [ctypes.POINTER(ContextOptions), ctypes.POINTER(ctypes.c_void_p)]
facade.ud_context_destroy.argtypes = [ctypes.c_void_p]
options = ContextOptions(ctypes.sizeof(ContextOptions), 3, 0, None, None, None)
context = ctypes.c_void_p()
assert facade.ud_context_create(ctypes.byref(options), ctypes.byref(context)) == 10 and not context.value
options.abi = 4
assert facade.ud_context_create(ctypes.byref(options), ctypes.byref(context)) == 0 and context.value
assert facade.ud_context_destroy(context) == 0
print(f"ZIP integrity: {len(listed)} files; native version {version}; ABI 4 accepted, ABI 3 rejected", flush=True)

# These real consumers use public installed headers and the dynamic Data target.
c_source = work / "c-source"
c_source.mkdir()
for origin, filename in [("hosts/cli.c", "package.c"), ("tests/typed_data_runner.c", "data.c"),
                         ("tests/core_runtime.c", "resources.c"), ("tests/package_sql.c", "sql.c"),
                         ("tests/check.h", "check.h")]:
    shutil.copyfile(ROOT / origin, c_source / filename)
(c_source / "CMakeLists.txt").write_text(
    "cmake_minimum_required(VERSION 3.24)\nproject(InstalledSDK C)\n"
    "find_package(UniversalData " + version + " EXACT CONFIG REQUIRED COMPONENTS Data)\n"
    "foreach(name package data resources sql)\n"
    "  add_executable(consumer_\u0024{name} \u0024{name}.c)\n"
    "  target_link_libraries(consumer_\u0024{name} PRIVATE UniversalData::Data)\n"
    "  if(MSVC)\n    target_compile_options(consumer_\u0024{name} PRIVATE /utf-8 /experimental:c11atomics)\n"
    "  else()\n    target_compile_definitions(consumer_\u0024{name} PRIVATE _POSIX_C_SOURCE=200809L)\n"
    "  endif()\nendforeach()\n", encoding="utf-8", newline="\n")
c_build = work / "c-build"
run("c-configure", [arguments.cmake, "-S", c_source, "-B", c_build, "-DCMAKE_PREFIX_PATH=" + str(sdk), "-DCMAKE_BUILD_TYPE=Release"])
run("c-build", [arguments.cmake, "--build", c_build, "--config", "Release", "--parallel", "4"])
c_output = c_build / "Release" if os.name == "nt" else c_build
executable = ".exe" if os.name == "nt" else ""
native_environment = dict(environment)
native_environment["PATH"] = str(native) + os.pathsep + environment["PATH"]
native_environment["LD_LIBRARY_PATH"] = str(native)
run("c-dynamic-resources", [sys.executable, ROOT / "tests/core_runtime.py", c_output / ("consumer_resources" + executable)], native_environment)
run("installed-host-pipeline", [sys.executable, ROOT / "tests/core_pipeline.py", sdk / "bin" / ("ud_cli" + executable)], native_environment)
if manifest["capabilities"]["sqlite"]:
    connector = sdk / "lib/UniversalData/plugins" / ("ud_connector_sqlite.dll" if os.name == "nt" else "ud_connector_sqlite.so")
    run("c-dynamic-sqlite", [sys.executable, ROOT / "tests/package_sql.py", c_output / ("consumer_sql" + executable), connector], native_environment)

nupkg = packages / f"UniversalData.{version}.nupkg"
with zipfile.ZipFile(nupkg) as archive:
    runtime_path = "runtimes/" + manifest["runtimeIdentifier"] + "/native/" + native_name
    assert hashlib.sha256(archive.read(runtime_path)).hexdigest() == digest(library), "NuGet contains a different native DLL."
dotnet_outputs = []
for name, source, assembly in [("package", "examples/dotnet/Program.cs", "Consumer"),
                               ("data", "examples/dotnet-data/Program.cs", "DataConsumer")]:
    client = work / ("dotnet-" + name)
    client.mkdir()
    shutil.copyfile(ROOT / source, client / "Program.cs")
    project = client / (assembly + ".csproj")
    project.write_text('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType>'
                       '<TargetFramework>net8.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings>'
                       '</PropertyGroup><ItemGroup><PackageReference Include="UniversalData" Version="' + version +
                       '" /></ItemGroup></Project>', encoding="utf-8")
    run("nuget-restore-" + name, [arguments.dotnet, "restore", project, "--source", packages,
                                "--packages", work / "nuget-cache", "--nologo", "--verbosity:minimal"])
    run("nuget-build-" + name, [arguments.dotnet, "build", project, "--configuration", "Release",
                              "--no-restore", "--nologo", "--verbosity:minimal"])
    output = client / "bin/Release/net8.0"
    assert digest(output / runtime_path) == digest(library), "Consumer did not receive the packaged native asset."
    dotnet_outputs.append(output / (assembly + ".dll"))

java_repository = work / "maven-repository"
jar = packages / f"universal-data-{version}.jar"
run("maven-install", [arguments.maven, "-B", "-q", "-f", packages / f"universal-data-{version}.pom",
                     "install:install-file", "-Dfile=" + str(jar),
                     "-DpomFile=" + str(packages / f"universal-data-{version}.pom"),
                     "-DlocalRepositoryPath=" + str(java_repository)])
java_client = work / "java-consumer"
java_sources = java_client / "src/main/java"
java_sources.mkdir(parents=True)
for filename in ("Consumer.java", "DataConsumer.java"):
    shutil.copyfile(ROOT / "examples/java" / filename, java_sources / filename)
(java_client / "pom.xml").write_text(
    '<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
    '<groupId>acceptance</groupId><artifactId>installed-sdk</artifactId><version>1</version>'
    '<properties><maven.compiler.release>17</maven.compiler.release><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties>'
    '<repositories><repository><id>bundle</id><url>' + java_repository.as_uri() + '</url></repository></repositories>'
    '<dependencies><dependency><groupId>org.universaldata</groupId><artifactId>universal-data</artifactId><version>' +
    version + '</version></dependency></dependencies><build><plugins><plugin><groupId>org.apache.maven.plugins</groupId>'
    '<artifactId>maven-compiler-plugin</artifactId><version>3.14.0</version></plugin></plugins></build></project>',
    encoding="utf-8")
run("maven-consumer", [arguments.maven, "-B", "-q", "-f", java_client / "pom.xml",
                       "-Dmaven.repo.local=" + str(work / "java-cache"), "package"])
installed_jar = java_repository / f"org/universaldata/universal-data/{version}/universal-data-{version}.jar"
assert digest(installed_jar) == digest(jar)
run("bundle-language-parity",
    [sys.executable, ROOT / "tests/package_language_parity.py",
     c_output / ("consumer_package" + executable), c_output / ("consumer_data" + executable),
     arguments.dotnet, *dotnet_outputs, arguments.java, java_client / "target/classes", installed_jar, native,
     "--managed-native-local"])
record = {"exitCode": 0, "sdkVersion": version, "abiVersion": 4, "configuration": "Release",
          "sourceFingerprint": fingerprint, "archive": str(bundle), "archiveSha256": digest(bundle),
          "verifiedFiles": len(listed), "unpackedDirectory": str(unpacked), "logs": str(work),
          "nativeLibrary": {"path": str(library), "sha256": digest(library)},
          "nuget": {"path": str(nupkg), "sha256": digest(nupkg), "nativeFromPackage": True, "sdkLoaderPath": False},
          "maven": {"path": str(jar), "sha256": digest(jar), "nativeFromBundle": True},
          "c": {"cmakeTarget": "UniversalData::Data", "dynamicPackage": True, "dynamicResources": True,
                "installedHostPipeline": True, "dynamicSQLite": manifest["capabilities"]["sqlite"]},
          "languageParity": "10003 rows / 20006 input and output / 8 tasks; Package/Project/child and scalar/nested/wire equality",
          "commands": commands}
arguments.evidence.parent.mkdir(parents=True, exist_ok=True)
arguments.evidence.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
print("Actual distributable C/NuGet/Maven/JNI consumers passed; evidence=" + str(arguments.evidence))

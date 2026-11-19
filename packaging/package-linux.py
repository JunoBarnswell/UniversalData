"""Seal a Release Linux SDK, licensed runtimes and thin NuGet/JNI bindings."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--build', required=True, type=pathlib.Path)
parser.add_argument('--output', required=True, type=pathlib.Path)
parser.add_argument('--runtime-dependencies', required=True, type=pathlib.Path,
                    help='Explicit licensed native runtime inventory; no vendor binaries are inferred.')
parser.add_argument('--patchelf', default='patchelf')
parser.add_argument('--maven-settings', type=pathlib.Path)
a = parser.parse_args()
build, output = a.build.resolve(), a.output.resolve()
product = json.loads((ROOT / 'metadata/product.json').read_text())['product']
version = product['sdkVersion']
assert sys.platform.startswith('linux') and not output.exists(), 'Use a new Linux artifact directory'
cache = (build / 'CMakeCache.txt').read_text()
assert 'CMAKE_BUILD_TYPE:STRING=Release' in cache and 'UD_SANITIZE:BOOL=ON' not in cache
assert re.search(r'^CMAKE_PROJECT_VERSION:STATIC=' + re.escape(version) + r'$', cache, re.M), \
    'Reconfigure the build from the authoritative product version before packaging'
inventory = json.loads(a.runtime_dependencies.read_text())
assert isinstance(inventory, list) and inventory, 'Explicit runtime inventory is required'

def elf(path):
    text = subprocess.check_output(['readelf', '-d', str(path)], text=True)
    needed = re.findall(r'\(NEEDED\).*?\[([^\]]+)\]', text)
    soname = re.findall(r'\(SONAME\).*?\[([^\]]+)\]', text)
    return needed, soname[0] if soname else None

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

# Validate every source and notice before producing an install tree.
validated, names = [], set()
for entry in inventory:
    source = pathlib.Path(entry['library']).resolve(strict=True)
    needed, name = elf(source)
    assert name and pathlib.PurePath(name).name == name and name not in names
    assert source.read_bytes()[:5] == b'\x7fELF\x02', 'Runtime must be a genuine ELF64 library'
    assert entry['project'] and entry['version'] and entry['licenseFiles']
    licenses = [pathlib.Path(p).resolve(strict=True) for p in entry['licenseFiles']]
    assert all(p.is_file() and p.stat().st_size for p in licenses)
    names.add(name)
    validated.append((entry, source, needed, name, licenses))

output.mkdir(parents=True)
sdk, packages = output / 'sdk', output / 'packages'
packages.mkdir()
subprocess.run(['cmake', '--install', str(build), '--prefix', str(sdk)], check=True)
native = sdk / 'lib'
notices = sdk / 'share/UniversalData/licenses/native-runtimes'
notices.mkdir(parents=True)
runtimes = []
for entry, source, needed, name, licenses in validated:
    target = native / name
    assert not target.exists(), 'Native runtime would overwrite an installed SDK file'
    shutil.copyfile(source, target)
    # RUNPATH is relative to the relocated runtime itself, including transitive
    # dependencies such as ICU's data library. Never mutate the vendor SDK.
    subprocess.run([a.patchelf, '--set-rpath', '$ORIGIN', str(target)], check=True)
    folder = notices / entry['project']
    assert pathlib.PurePath(entry['project']).name == entry['project']
    folder.mkdir(exist_ok=True)
    for license in licenses:
        shutil.copyfile(license, folder / license.name)
    runtimes.append(dict(project=entry['project'], version=entry['version'], soname=name,
                         originalSha256=digest(source), sha256=digest(target), needed=needed,
                         modification='Relocatable $ORIGIN RUNPATH only',
                         licenseFiles=[p.relative_to(output).as_posix() for p in sorted(folder.iterdir())]))

# The same files become NuGet native assets, with the same notices. Hard links
# keep staging bounded; there is no second native implementation or loader.
with tempfile.TemporaryDirectory(prefix='ud-nuget-native-', dir=output.parent) as temp:
    staging = pathlib.Path(temp)
    for runtime in runtimes:
        os.link(native / runtime['soname'], staging / runtime['soname'])
    subprocess.run(['dotnet', 'pack', str(ROOT / 'bindings/dotnet/UniversalData/UniversalData.csproj'),
                    '-c', 'Release', '-o', str(packages), '-p:NativeLibraryDirectory=' + str(native),
                    '-p:NativeDependencyDirectory=' + str(staging),
                    '-p:NativePluginDirectory=' + str(native / 'UniversalData/plugins'),
                    '-p:NativeLicenseDirectory=' + str(sdk / 'share/UniversalData/licenses'),
                    '-p:NativeRuntimeIdentifier=linux-x64', '--nologo'], check=True)
subprocess.run(['dotnet', 'restore', str(ROOT / 'examples/dotnet/Consumer.csproj'), '--nologo'], check=True)
nuget_dependencies = json.loads(subprocess.check_output([sys.executable, str(ROOT / 'packaging/nuget_feed.py'),
    '--assets', str(ROOT / 'bindings/dotnet/UniversalData/obj/project.assets.json'),
    '--assets', str(ROOT / 'examples/dotnet/obj/project.assets.json'), '--output', str(packages)], text=True))
maven = ['mvn', '-B', '-q']
if a.maven_settings:
    maven += ['-s', str(a.maven_settings.resolve())]
subprocess.run(maven + ['-f', str(ROOT / 'bindings/java/pom.xml'), 'package'], check=True)
shutil.copyfile(ROOT / f'bindings/java/target/universal-data-{version}.jar', packages / f'universal-data-{version}.jar')
shutil.copyfile(ROOT / 'bindings/java/pom.xml', packages / f'universal-data-{version}.pom')

# Prove the runtime contains no checkout paths. Proprietary database clients
# remain deployment prerequisites of their optional connector modules.
roots = [native / 'libud_data.so', native / 'libud_java.so']
for host in ('cli', 'worker', 'management'):
    host_native = sdk / 'bin/universaldata' / host / 'libud_data.so'
    assert host_native.is_file(), 'Product Native facade missing'
    subprocess.run([a.patchelf, '--set-rpath', '$ORIGIN:$ORIGIN/../../../lib', str(host_native)], check=True)
    root = sdk / 'bin/universaldata' / host
    assert (root / ('ud_cli.dll' if host == 'cli' else 'ud_worker.dll' if host == 'worker' else 'UniversalData.Management.dll')).is_file(), 'Product host missing'
assert (sdk / 'bin/universaldata/management/wwwroot/index.html').is_file(), 'Designer assets missing'
external = set()
for path in roots:
    dependencies, _ = elf(path)
    external.update(dependencies)
    dynamic = subprocess.check_output(['readelf', '-d', str(path)], text=True)
    assert not re.search(r'\((?:RUNPATH|RPATH)\).*?/workspace/', dynamic), 'Checkout runtime path leaked into SDK'
for runtime in runtimes:
    external.update(runtime['needed'])
external.difference_update(names | {'libud_data.so.' + str(product['abiVersion'])})
plugins = []
for path in sorted((native / 'UniversalData/plugins').glob('*.so')):
    dependencies, _ = elf(path)
    plugins.append(dict(path=path.relative_to(output).as_posix(), dependencies=dependencies,
                        vendorClientsBundled=False))
fingerprint = subprocess.check_output([sys.executable, str(ROOT / 'scripts/release_guard.py'), '--fingerprint'], text=True).strip()
files = [dict(path=p.relative_to(output).as_posix(), bytes=p.stat().st_size, sha256=digest(p))
         for p in sorted(output.rglob('*')) if p.is_file()]
drivers = ('sqlite', 'postgresql', 'odbc', 'mysql', 'mariadb', 'oracle_oci',
           'activemq', 'rabbitmq', 'rocketmq', 'kafka')
features = {key: ('UD_WITH_' + key.upper() + ':BOOL=ON') in cache
            for key in (*drivers, 'arrow_files', 'http', 'xml', 'network')}
if 'UD_BUILD_PROFILE:STRING=PRODUCT' in cache:
    assert all(features.values()), 'Complete product dependencies cannot silently downgrade'
for driver in drivers:
    if features[driver]:
        expected = native / 'UniversalData/plugins' / f'ud_connector_{driver}.so'
        assert expected.is_file(), f'Missing enabled connector: {expected.name}'
manifest = dict(schemaVersion=product['protocolVersion'],protocolVersion=product['protocolVersion'], buildProfile='PRODUCT' if 'UD_BUILD_PROFILE:STRING=PRODUCT' in cache else 'CORE', sdkVersion=version, abiVersion=product['abiVersion'],
                configuration='Release', runtimeIdentifier='linux-x64', sourceFingerprint=fingerprint,
                capabilities=features, managedRuntime='Microsoft.AspNetCore.App 10', hosts=['cli','worker','management','designer'],
                nativeRuntimes=runtimes, nugetDependencies=nuget_dependencies, systemDependencies=sorted(external), optionalConnectors=plugins, files=files)
(output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
archive = output.parent / f'UniversalData-{version}-linux-x64.zip'
assert not archive.exists()
with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as z:
    for p in sorted(output.rglob('*')):
        if p.is_file():
            z.write(p, p.relative_to(output).as_posix())
print(json.dumps(dict(archive=str(archive), sha256=digest(archive), manifest=str(output / 'manifest.json'), sourceFingerprint=fingerprint)))

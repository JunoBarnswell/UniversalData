"""Verify current core delivery evidence. Does not publish or merge."""
import argparse
import hashlib
import json
import pathlib
import sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
BASE=ROOT/'docs/acceptance'
REQUIRED={'windows-msvc','windows-native','windows-asan','sqlite-transactions-recovery',
          'memory-quota-spill-concurrency','language-parity','installed-c',
          'installed-nuget','installed-maven','source-contracts',
          'windows-release','distributable-bundle'}
def source_hash():
    files=[ROOT/name for name in ('CMakeLists.txt','CMakePresets.json','VERSION','.github/workflows/ci.yml',
                                 'Directory.Build.props','Directory.Build.targets','UniversalData.sln')]
    for folder in ('include','components','engines','connectors','catalog','plugins','hosts','bindings','cmake','tests','examples','scripts','packaging'):
        files.extend(p for p in (ROOT/folder).rglob('*') if p.is_file()
                     and not set(p.relative_to(ROOT).parts)&{'bin','obj','target','build','__pycache__'}
                     and p.suffix in {'.c','.h','.cs','.java','.py','.ps1','.cmake','.json','.xml','.csproj','.vcxproj','.filters','.txt'})
    digest=hashlib.sha256()
    for path in sorted(set(files)):
        digest.update(path.relative_to(ROOT).as_posix().encode());digest.update(b'\0')
        digest.update(path.read_bytes());digest.update(b'\0')
    return digest.hexdigest()
def check():
    fingerprint=source_hash();errors=[]
    gates=json.loads((BASE/'gates.json').read_text(encoding='utf-8'))
    if gates.get('sourceFingerprint')!=fingerprint:errors.append('Source changed after sealed evidence')
    by_id={g['id']:g for g in gates.get('gates',[])}
    for identifier in sorted(REQUIRED):
        gate=by_id.get(identifier)
        if not gate or gate.get('status')!='Passed':errors.append(identifier+': required core gate did not pass');continue
        for evidence in gate.get('evidence',[]):
            path=(ROOT/evidence['path']).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():errors.append(identifier+': missing evidence');continue
            if hashlib.sha256(path.read_bytes()).hexdigest()!=evidence['sha256']:errors.append(identifier+': evidence hash mismatch')
        if not gate.get('evidence'):errors.append(identifier+': no actual evidence')
    if gates.get('linux')!='Exempt by current user decision':errors.append('Platform decision changed')
    delivery=gates.get('delivery',{})
    archive=(ROOT/delivery.get('archive','')).resolve()
    manifest=(ROOT/delivery.get('manifest','')).resolve()
    if not archive.is_relative_to(ROOT) or not archive.is_file():
        errors.append('Missing current distributable archive')
    elif hashlib.sha256(archive.read_bytes()).hexdigest()!=delivery.get('sha256'):
        errors.append('Distributable archive hash mismatch')
    if not manifest.is_relative_to(ROOT) or not manifest.is_file():
        errors.append('Missing current artifact manifest')
    else:
        bundle=json.loads(manifest.read_text(encoding='utf-8'))
        if (bundle.get('sourceFingerprint')!=fingerprint or bundle.get('abiVersion')!=4
                or bundle.get('sdkVersion')!=(ROOT/'VERSION').read_text().strip()
                or bundle.get('configuration')!='Release' or bundle.get('runtimeIdentifier')!='win-x64'):
            errors.append('Artifact manifest differs from current source/ABI')
        expected={'sdk/bin/ud_data.dll','sdk/bin/ud_java.dll','sdk/bin/ud_cli.exe','sdk/bin/ud_worker.exe',
                  'packages/UniversalData.1.0.0.nupkg','packages/universal-data-1.0.0.jar',
                  'sdk/include/ud/package.h','sdk/lib/ud_data.lib'}
        entries=bundle.get('files',[])
        if not expected.issubset({entry['path'] for entry in entries}):
            errors.append('Incomplete artifact manifest')
        for entry in entries:
            path=(manifest.parent/entry['path']).resolve()
            if not path.is_relative_to(manifest.parent) or not path.is_file():
                errors.append('Missing delivered file: '+entry['path']);continue
            if path.stat().st_size!=entry['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest()!=entry['sha256']:
                errors.append('Delivered file changed: '+entry['path'])
    return fingerprint,errors
if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--fingerprint',action='store_true')
    if parser.parse_args().fingerprint:
        print(source_hash());sys.exit(0)
    fingerprint,errors=check()
    if errors:
        print('Core delivery evidence incomplete:');print('\n'.join(errors));sys.exit(1)
    print('Core delivery evidence passed for source '+fingerprint)

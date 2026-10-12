"""Fail-closed release/merge gate. Does not compile, publish, or change GitHub."""
import hashlib
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE = ROOT / 'docs' / 'acceptance'

def source_hash():
    files = [ROOT / 'CMakeLists.txt', ROOT / 'VERSION', ROOT / 'CMakePresets.json']
    for folder in ('include', 'components', 'engines', 'hosts', 'plugins', 'bindings', 'tests', 'examples', 'scripts', 'packaging', 'cmake', '.github'):
        files.extend(p for p in (ROOT / folder).rglob('*') if p.is_file() and not set(p.relative_to(ROOT).parts) & {'bin', 'obj', 'target', '__pycache__', 'work', 'build'})
    result = hashlib.sha256()
    for path in sorted(set(files), key=lambda p: p.relative_to(ROOT).as_posix()):
        relative = path.relative_to(ROOT).as_posix().encode('utf-8')
        body = path.read_bytes()
        result.update(len(relative).to_bytes(8, 'little')); result.update(relative)
        result.update(len(body).to_bytes(8, 'little')); result.update(body)
    return result.hexdigest()

def validate_evidence(evidence, expected_source, errors, owner):
    if not evidence:
        errors.append(f'{owner}: missing execution evidence')
        return
    for item in evidence:
        if not isinstance(item, dict):
            errors.append(f'{owner}: evidence must include path, sha256 and sourceTreeHash')
            continue
        runtime_kinds = {'native-build', 'native-test', 'language-build', 'language-test',
                         'integration-test', 'memory-check', 'benchmark', 'package-consumption', 'uat'}
        if item.get('kind') not in runtime_kinds:
            errors.append(f'{owner}: static / environment checks cannot satisfy execution evidence')
        if owner not in item.get('claims', []):
            errors.append(f'{owner}: execution evidence does not identify this requirement/case/gate')
        command = item.get('command', '')
        command_text = ' '.join(command) if isinstance(command, list) else str(command)
        if any(name in command_text for name in ('static_check.py', 'c_syntax_check.py', 'language_syntax_check.py', 'release_guard.py')):
            errors.append(f'{owner}: compiler-free checker is not runtime acceptance')
        relative = item.get('path', '')
        path = (ROOT / relative).resolve()
        if not relative or not path.is_relative_to(BASE / 'evidence') or not path.is_file():
            errors.append(f'{owner}: missing evidence file')
            continue
        if hashlib.sha256(path.read_bytes()).hexdigest() != item.get('sha256'):
            errors.append(f'{owner}: evidence hash mismatch')
        if item.get('sourceTreeHash') != expected_source:
            errors.append(f'{owner}: evidence belongs to a different source tree')
        if item.get('exitCode') != 0 or not item.get('command') or not item.get('startedAt') or not item.get('finishedAt'):
            errors.append(f'{owner}: incomplete / failed execution metadata')

def check():
    gates = json.loads((BASE / 'gates.json').read_text(encoding='utf-8'))
    requirements = json.loads((BASE / 'requirements.json').read_text(encoding='utf-8'))['requirements']
    cases = json.loads((BASE / 'cases.json').read_text(encoding='utf-8'))['cases']
    errors = []; fingerprint = source_hash()
    if set(gates.get('phases', {})) != set('ABCDEFGH'):
        errors.append('phase baseline must include A–H')
    for phase in 'ABCDEFGH':
        if gates.get('codeStatus', {}).get(phase) != 'Implemented': errors.append(f'{phase}: phase code is incomplete')
        if gates.get('phases', {}).get(phase) != 'Complete': errors.append(f'{phase}: acceptance is incomplete')
    if {r['domain'] for r in requirements} != {f'{n:02}' for n in range(1, 25)}:
        errors.append('24-domain baseline is incomplete')
    by_case = {case['id']: case for case in cases}
    if not {'S01', 'S02', 'S03', 'S04', 'S05'} <= set(by_case): errors.append('S01–S05 are mandatory')
    for requirement in requirements:
        label = requirement['id']
        if requirement['status'] != 'Passed': errors.append(f'{label}: requirement has not passed')
        for field in ('interfaces', 'implementation', 'cases', 'uat', 'evidence', 'releaseGates'):
            if not requirement.get(field): errors.append(f'{label}: missing {field} traceability')
        for relative in requirement.get('implementation', []):
            path = (ROOT / relative).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file(): errors.append(f'{label}: implementation path is missing')
        for case in requirement.get('cases', []):
            if case not in by_case: errors.append(f'{label}: unknown case {case}')
        validate_evidence(requirement.get('evidence'), fingerprint, errors, label)
    for case in cases:
        label = case['id']
        if case['status'] != 'Passed': errors.append(f'{label}: {case["status"]}')
        if not case.get('actual') or not case.get('fixtures') or not case.get('steps') or not case.get('expected'):
            errors.append(f'{label}: incomplete fixture / actual-result record')
        validate_evidence(case.get('evidence'), fingerprint, errors, label)
    required_gates = {'linux-gcc', 'windows-msvc', 'asan-ubsan', 'postgresql-isolated-containers',
        'sqlite-transactions', 'c-dotnet8-java17-equivalence', 'native-nuget-maven-install-consumption',
        'quota-backpressure-spill', 'benchmark-recorded-hardware', 's01-s05'}
    if not required_gates <= {gate['id'] for gate in gates.get('gates', [])}: errors.append('mandatory release gates are missing')
    for gate in gates.get('gates', []):
        if gate['id'] in required_gates and not gate.get('required', True):
            errors.append(f'{gate["id"]}: mandatory runtime gate cannot be made optional')
        if not gate.get('required', True):
            continue # CI availability is recorded; actual OS/runtime evidence is mandatory.
        if gate.get('status') != 'Passed': errors.append(f'{gate["id"]}: {gate.get("status", "Missing")}')
        validate_evidence(gate.get('evidence'), fingerprint, errors, gate['id'])
    return fingerprint, errors

if __name__ == '__main__':
    fingerprint, errors = check()
    if errors:
        print('Release / merge denied: required implementation or execution evidence is incomplete.')
        print('\n'.join(errors))
        raise SystemExit(1)
    print(f'Release / merge evidence gate passed for source tree {fingerprint}')

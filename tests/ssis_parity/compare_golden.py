"""Compare captured Windows SSIS output against Native observations.

A manifest is evidence transport. This program never generates SSIS expected
values and rejects example/unversioned/unhashed/reference-program artifacts.
"""
import argparse
import hashlib
import json
import os
import pathlib
import sys

REQUIRED = ('ssisVersion','sqlServerVersion','ssdtVersion','windowsVersion','localeId','codePage','timeZone','command','packageSha256')
KEYS = ('caseId','rowId','componentId','outputColumnId')
VALUE_KEYS = ('value','ssisType','isNull','length','precision','scale','codePage','errorCode','errorColumn','outputPort')

def load(path): return json.loads(path.read_text(encoding='utf-8'))
def observations(path):
    result={}
    for line in path.read_text(encoding='utf-8').splitlines():
        value=json.loads(line);key=tuple(value.get(k) for k in KEYS)
        if any(part is None or part=='' for part in key) or key in result:raise ValueError('Missing or duplicate observation identity')
        if any(k not in value for k in VALUE_KEYS):raise ValueError('Incomplete typed observation')
        result[key]=value
    if not result:raise ValueError('An empty corpus cannot establish Golden evidence')
    return result

def compare(manifest_path,native_path,output):
    manifest=load(manifest_path)
    if manifest.get('producer')!='Microsoft SSIS' or manifest.get('platform')!='Windows':raise ValueError('Golden must be captured from Microsoft SSIS on Windows')
    environment=manifest.get('environment',{})
    if any(k not in environment for k in REQUIRED):raise ValueError('Incomplete captured SSIS environment')
    for key in ('ssisVersion','sqlServerVersion','ssdtVersion','windowsVersion','command'):
        value=str(environment[key])
        if not value or any(word in value.lower() for word in ('example','captured-from','placeholder','mock','not-run')):raise ValueError('Example values cannot establish Golden evidence')
    package_hash=environment['packageSha256']
    if 'dtexec' not in str(environment['command']).lower():raise ValueError('The actual SSIS execution command is required')
    if len(package_hash)!=64 or any(c not in '0123456789abcdef' for c in package_hash):raise ValueError('Invalid package SHA256')
    root=manifest_path.parent.resolve();artifacts={}
    for item in manifest.get('artifacts',[]):
        path=(root/item['path']).resolve()
        if not path.is_relative_to(root) or not path.is_file():raise ValueError('Artifact is outside captured evidence directory')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:raise ValueError('Golden artifact checksum differs')
        artifacts[item['role']]=path
    if not {'package','observations','executionLog'}<=artifacts.keys():raise ValueError('Actual package, observations and execution log are required')
    if artifacts['package'].suffix.lower()!='.dtsx' or not artifacts['executionLog'].stat().st_size:raise ValueError('Actual DTSX package and nonempty execution log are required')
    if hashlib.sha256(artifacts['package'].read_bytes()).hexdigest()!=package_hash:raise ValueError('Captured package differs')
    expected,actual=observations(artifacts['observations']),observations(native_path)
    differences=[]
    for key in sorted(expected.keys()|actual.keys(),key=str):
        left,right=expected.get(key),actual.get(key)
        lvalue=None if left is None else {k:left.get(k) for k in VALUE_KEYS}
        rvalue=None if right is None else {k:right.get(k) for k in VALUE_KEYS}
        if lvalue!=rvalue:
            differences.append(dict(zip(KEYS,key),ssisVersion=environment['ssisVersion'],packageSha256=package_hash,expression=(left or right).get('expression'),ssisExpected=lvalue,nativeActual=rvalue,severity='ParityMismatch',source=(left or right).get('source')))
    output.write_text(json.dumps(dict(environment=environment,observations=len(expected),differences=differences,status='GoldenVerified' if not differences else 'ParityMismatch'),ensure_ascii=False,indent=2)+'\n')
    return 1 if differences else 0

def main():
    p=argparse.ArgumentParser();p.add_argument('--manifest',type=pathlib.Path,default=os.environ.get('UD_SSIS_GOLDEN_MANIFEST'));p.add_argument('--native',type=pathlib.Path,default=os.environ.get('UD_SSIS_NATIVE_OBSERVATIONS'));p.add_argument('--output',type=pathlib.Path,default=pathlib.Path('ssis-differences.json'));args=p.parse_args()
    if not args.manifest or not args.native:print('VerificationBlocked: real Windows SSIS capture and matching Native observations are required');return 77
    return compare(args.manifest,args.native,args.output)
if __name__=='__main__':sys.exit(main())

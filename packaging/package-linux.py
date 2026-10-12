"""Package the already built Release SDK without rebuilding or hidden fallbacks."""
import argparse,hashlib,json,pathlib,shutil,subprocess,sys,zipfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser();parser.add_argument('--build',required=True,type=pathlib.Path);parser.add_argument('--output',required=True,type=pathlib.Path)
a=parser.parse_args();build=a.build.resolve();output=a.output.resolve();model=json.loads((ROOT/'metadata/product.json').read_text());product=model['product'];version=product['sdkVersion']
assert sys.platform.startswith('linux') and not output.exists(),'Use a new Linux artifact directory'
output.mkdir(parents=True);sdk=output/'sdk';packages=output/'packages';packages.mkdir()
subprocess.run(['cmake','--install',str(build),'--prefix',str(sdk)],check=True)
subprocess.run(['dotnet','pack',str(ROOT/'bindings/dotnet/UniversalData/UniversalData.csproj'),'-c','Release','-o',str(packages),'-p:NativeLibraryDirectory='+str(sdk/'lib'),'-p:NativeRuntimeIdentifier=linux-x64','--nologo'],check=True)
subprocess.run(['mvn','-B','-q','-f',str(ROOT/'bindings/java/pom.xml'),'package'],check=True)
shutil.copyfile(ROOT/f'bindings/java/target/universal-data-{version}.jar',packages/f'universal-data-{version}.jar')
shutil.copyfile(ROOT/'bindings/java/pom.xml',packages/f'universal-data-{version}.pom')
fingerprint=subprocess.check_output([sys.executable,str(ROOT/'scripts/release_guard.py'),'--fingerprint'],text=True).strip()
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
files=[{'path':p.relative_to(output).as_posix(),'bytes':p.stat().st_size,'sha256':digest(p)} for p in sorted(output.rglob('*')) if p.is_file()]
manifest={'schemaVersion':1,'sdkVersion':version,'abiVersion':product['abiVersion'],'configuration':'Release','runtimeIdentifier':'linux-x64','sourceFingerprint':fingerprint,'capabilities':{'sqlite':(sdk/'lib/UniversalData/plugins/ud_connector_sqlite.so').exists()},'files':files}
(output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
archive=output.parent/f'UniversalData-{version}-linux-x64.zip'
assert not archive.exists()
with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
 for p in sorted(output.rglob('*')):
  if p.is_file():z.write(p,p.relative_to(output).as_posix())
print(json.dumps({'archive':str(archive),'sha256':digest(archive),'manifest':str(output/'manifest.json'),'sourceFingerprint':fingerprint}))

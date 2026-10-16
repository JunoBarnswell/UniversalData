"""Start an authenticated real product host for the portable browser journey."""
import argparse
import base64
import hashlib
import os
import pathlib
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import json

parser=argparse.ArgumentParser()
parser.add_argument('--dotnet',required=True)
parser.add_argument('--management',required=True,type=pathlib.Path)
parser.add_argument('--worker',required=True,type=pathlib.Path)
parser.add_argument('--browser-python',default=sys.executable)
parser.add_argument('--browser-executable',type=pathlib.Path)
parser.add_argument('--evidence',required=True,type=pathlib.Path)
args=parser.parse_args()
args.evidence.mkdir(parents=True,exist_ok=True)
available=subprocess.run([args.browser_python,'-c','import playwright.sync_api'],capture_output=True)
if available.returncode:
    print('Verification Blocked: the browser Python has no Playwright');sys.exit(77)
with tempfile.TemporaryDirectory(prefix='ud-browser-host-') as directory:
    root=pathlib.Path(directory);resources=root/'resources';resources.mkdir()
    private=root/'private';private.mkdir(mode=0o700)
    password=secrets.token_urlsafe(32);salt=os.urandom(32)
    encoded='pbkdf2-sha256$600000$'+base64.b64encode(salt).decode()+'$'+base64.b64encode(hashlib.pbkdf2_hmac('sha256',password.encode(),salt,600000)).decode()
    identity=private/'identity.json';identity.write_text(json.dumps(dict(version=1,users=[dict(id='acceptance',tenant='browser',passwordHash=encoded,stamp=secrets.token_hex(32),roles=['operator'])],roles=dict(operator=[dict(permission='*',resource='*')]))));identity.chmod(0o600)
    policy=private/'policy.json';policy.write_text(json.dumps(dict(dataDirectory=str(root/'state'),fileRoots=[str(resources)],networkOrigins=[],pluginRoots=[],credentialFiles={},identityFile=str(identity),tenants=dict(browser=dict(fileRoots=[str(resources)],networkOrigins=[],pluginRoots=[],credentialReferences=[])))));policy.chmod(0o600)
    with socket.socket() as reserve:reserve.bind(('127.0.0.1',0));port=reserve.getsockname()[1]
    address=f'http://127.0.0.1:{port}'
    with (args.evidence/'host.log').open('w') as log:
        host=subprocess.Popen([args.dotnet,str(args.management),'--policy',str(policy),'--worker',str(args.worker),'--urls',address],stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                if host.poll() is not None:raise RuntimeError('Product host did not start; see host.log')
                try:urllib.request.build_opener(urllib.request.ProxyHandler({})).open(address+'/api/health',timeout=2)
                except urllib.error.HTTPError as error:
                    if error.code==401:break
                    raise
                except urllib.error.URLError:time.sleep(.1)
            else:raise TimeoutError('Product host start timed out')
            command=[args.browser_python,str(pathlib.Path(__file__).with_name('web_product.py')),'--url',address,'--root',str(resources),'--evidence',str(args.evidence),'--subject','acceptance']
            if args.browser_executable:command+=['--browser-executable',str(args.browser_executable)]
            subprocess.run(command,env={**os.environ,'UD_BROWSER_PASSWORD':password},check=True,timeout=600)
        finally:
            host.terminate()
            try:host.wait(timeout=15)
            except subprocess.TimeoutExpired:host.kill();host.wait(timeout=5)

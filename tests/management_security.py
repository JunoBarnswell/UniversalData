"""Actual Management + Worker + Native + SQLite authorization and protected-parameter flow."""
import base64
import hashlib
import http.cookiejar
import json
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

dotnet, management, worker = sys.argv[1:4]

def encoded(password, salt):
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000)
    return 'pbkdf2-sha256$600000$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(digest).decode()

with tempfile.TemporaryDirectory(prefix='ud-management-security-') as directory:
    root = pathlib.Path(directory)
    resource = root / 'resources'
    resource.mkdir()
    (resource / 'a').mkdir()
    (resource / 'b').mkdir()
    modules = root / 'modules'
    modules.mkdir()
    identities = root / 'identities.json'
    identities.write_text(json.dumps({'version': 1, 'users': [
        {'id': name, 'tenant': tenant, 'passwordHash': encoded('password-for-' + name, name.encode().ljust(32, b'0')),
         'stamp': name.ljust(64, '0'), 'roles': [role]}
        for name, tenant, role in [('admin-a', 'a', 'admin'), ('reader-a', 'a', 'reader'), ('admin-b', 'b', 'admin')]],
        'roles': {'admin': [{'permission': '*', 'resource': '*'}], 'reader': [{'permission': 'package.read', 'resource': '*'}, {'permission': 'component.read', 'resource': '*'}]}}))
    policy = root / 'policy.json'
    data = root / 'product'
    policy.write_text(json.dumps({'dataDirectory': str(data), 'fileRoots': [str(resource)], 'networkOrigins': [], 'pluginRoots': [str(modules)], 'credentialFiles': {},
        'identityFile': str(identities), 'tenants': {tenant: {'fileRoots': [str(resource / tenant)], 'networkOrigins': [], 'pluginRoots': [str(modules)], 'credentialReferences': []} for tenant in ['a', 'b']}}))
    with socket.socket() as reserve:
        reserve.bind(('127.0.0.1', 0))
        port = reserve.getsockname()[1]
    address = f'http://127.0.0.1:{port}'
    clients = {}
    def request(client, path, method='GET', value=None, csrf=True):
        opener, token = clients.get(client, (urllib.request.build_opener(urllib.request.ProxyHandler({})), ''))
        headers = {'Content-Type': 'application/json'}
        if token and csrf:
            headers['X-UD-CSRF'] = token
        body = json.dumps(value).encode() if value is not None else None
        try:
            response = opener.open(urllib.request.Request(address + '/api' + path, data=body, headers=headers, method=method), timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        text = response.read().decode()
        return response.code, json.loads(text) if text and 'application/json' in response.headers.get('Content-Type', '') else text
    for client in ['admin-a', 'reader-a', 'admin-b']:
        clients[client] = (urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())), '')
    process = subprocess.Popen([dotnet, management, '--policy', str(policy), '--worker', worker, '--urls', address], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 30
        while True:
            if process.poll() is not None:
                raise AssertionError(process.communicate())
            try:
                status, _ = request('anonymous', '/health')
                assert status == 401, status
                break
            except (urllib.error.URLError, OSError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.1)
        for name in clients:
            status, result = request(name, '/auth/login', 'POST', {'subject': name, 'password': 'password-for-' + name})
            assert status == 200, (status, result)
            clients[name] = (clients[name][0], result['csrf'])
        package = {'version': 10, 'id': 'protected-package', 'parameters': {'password': {'type': 'utf8'}},
            'variables': {'counter': {'type': 'int64', 'default': 0}},
            'controlFlow': {'tasks': [{'id': 'set', 'kind': 'set', 'config': {'name': 'counter', 'value': {'literal': 1, 'type': 'int64'}}}], 'constraints': []}, 'dataFlows': []}
        assert request('reader-a', '/packages/protected-package/versions/1', 'PUT', package)[0] == 403
        assert request('admin-a', '/packages/protected-package/versions/1', 'PUT', package, csrf=False)[0] == 403
        assert request('admin-a', '/packages/protected-package/versions/1', 'PUT', package)[0] == 200
        assert request('admin-b', '/packages/protected-package/versions/1')[0] == 404
        assert request('reader-a', '/executions', 'POST', {'packageId': 'protected-package', 'packageRevision': '1'})[0] == 403
        assert request('reader-a', '/scripts/compile', 'POST', {'source': 'invalid'})[0] == 403
        secret = 'runtime-password-781394-sensitive'
        status, run = request('admin-a', '/executions', 'POST', {'packageId': 'protected-package', 'packageRevision': '1', 'parameters': json.dumps({'password': secret})})
        assert status == 202 and secret not in json.dumps(run), (status, run)
        ticket = run['id']
        deadline = time.monotonic() + 25
        while True:
            status, state = request('admin-a', '/executions/' + ticket)
            assert status == 200, (status, state)
            assert secret not in json.dumps(state)
            if state['state'] not in ['Queued', 'Claimed', 'Running']:
                break
            assert time.monotonic() < deadline, state
            time.sleep(.1)
        assert state['state'] == 'Succeeded', state
        assert request('admin-b', '/executions/' + ticket)[0] == 404
        assert request('admin-b', '/logs?executionId=' + state['executionId'])[0] == 403
        status, logs = request('admin-a', '/logs?executionId=' + state['executionId'])
        assert status == 200 and secret not in json.dumps(logs)
        for path in data.rglob('*'):
            if path.is_file():
                assert secret.encode() not in path.read_bytes(), str(path)
        assert request('admin-a', '/auth/password', 'POST', {'currentPassword': 'password-for-admin-a', 'password': 'rotated-password-for-admin-a'})[0] == 204
        assert request('admin-a', '/auth/session')[0] == 401
        assert request('reader-a', '/auth/logout', 'POST')[0] == 204
        assert request('reader-a', '/auth/session')[0] == 401
    finally:
        process.terminate()
        try:
            process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
print('Actual Management/Worker/Native: authentication, CSRF, object roles, tenant isolation, execution, protected parameters, logs and rotation passed')

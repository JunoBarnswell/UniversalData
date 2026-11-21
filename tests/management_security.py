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
from product_version import PROTOCOL_VERSION

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
        for name, tenant, role in [('admin-a', 'a', 'admin'), ('peer-a', 'a', 'admin'), ('reader-a', 'a', 'reader'), ('admin-b', 'b', 'admin')]],
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
    def request(client, path, method='GET', value=None, csrf=True, raw=None, extra_headers=None):
        opener, token = clients.get(client, (urllib.request.build_opener(urllib.request.ProxyHandler({})), ''))
        headers = {'Content-Type': 'application/json'}
        if token and csrf:
            headers['X-UD-CSRF'] = token
        headers.update(extra_headers or {})
        body = raw if raw is not None else json.dumps(value).encode() if value is not None else None
        try:
            response = opener.open(urllib.request.Request(address + '/api' + path, data=body, headers=headers, method=method), timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        text = response.read().decode()
        return response.code, json.loads(text) if text and 'application/json' in response.headers.get('Content-Type', '') else text
    for client in ['admin-a', 'peer-a', 'reader-a', 'admin-b']:
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
        package = {'version': PROTOCOL_VERSION, 'id': 'protected-package', 'parameters': {'password': {'type': 'utf8'}},
            'variables': {'counter': {'type': 'int64', 'default': 0}},
            'controlFlow': {'tasks': [{'id': 'set', 'kind': 'set', 'config': {'name': 'counter', 'value': {'literal': 1, 'type': 'int64'}}}], 'constraints': []}, 'dataFlows': []}
        assert request('reader-a', '/versions', 'POST', {'document': package, 'expectedPackageRevision': '0'})[0] == 403
        assert request('admin-a', '/versions', 'POST', {'document': package, 'expectedPackageRevision': '0'}, csrf=False)[0] == 403
        assert request('admin-a', '/versions', 'POST', {'document': package, 'expectedPackageRevision': '0'})[0] == 200
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
        # SSE projects the exact persisted Native facts and event revisions.
        assert request('reader-a','/executions/'+ticket+'/events')[0] == 403
        assert request('admin-b','/executions/'+ticket+'/events')[0] == 404
        def feed(cursor=None):
            opener,_=clients['admin-a']
            headers={'Accept':'text/event-stream'}
            if cursor:headers['Last-Event-ID']=cursor
            with opener.open(urllib.request.Request(address+'/api/executions/'+ticket+'/events',headers=headers),timeout=20) as response:
                assert response.headers['Content-Type'].startswith('text/event-stream')
                text=response.read().decode()
            frames=[]
            for block in text.split('\n\n'):
                lines=block.splitlines()
                payload=next((line[6:] for line in lines if line.startswith('data: ')),None)
                if payload:frames.append((next(line[4:] for line in lines if line.startswith('id: ')),json.loads(payload)))
            return frames
        frames=feed();assert frames and frames[-1][1]['terminal'],frames
        assert frames[-1][1]['source']=='native-catalog' and frames[-1][1]['run']['executionId']==state['executionId']
        public_run=frames[-1][1]['run']
        assert public_run.keys()==state.keys(), (public_run.keys(),state.keys())
        assert not {'subjectStamp','controlReference','parametersReference','journalTicket'}.intersection(public_run)
        resumed=feed(frames[-1][0]);assert resumed[-1][1]['events']==[] and not resumed[-1][1]['gap'],resumed
        reset=feed('999999999999999.999999999999999.999999999999999');assert reset[0][1]['gap'],reset
        assert request('admin-b', '/executions/' + ticket)[0] == 404
        assert request('admin-b', '/logs?executionId=' + state['executionId'])[0] == 403
        status, logs = request('admin-a', '/logs?executionId=' + state['executionId'])
        assert status == 200 and secret not in json.dumps(logs)
        # Designer inspections are real Native reads in the same Worker; source
        # uploads stay subject-owned and publication never starts execution.
        status, imported = request('admin-a', '/files/upload', 'POST', raw=b'Amount,Customer\n12,VIP\n3,Other\n',
            extra_headers={'Content-Type': 'application/octet-stream', 'X-File-Name': 'orders.csv'})
        assert status == 200, (status, imported)
        target = resource / 'a' / 'never-written.csv'
        target.write_text('untouched\n')
        draft_package = {'version': PROTOCOL_VERSION, 'id': 'designer-draft', 'connections': [
            dict(id='input', driver='file', resource=imported['resource']), dict(id='output', driver='file', resource=str(target))],
            'controlFlow': {'tasks': [dict(id='run',kind='dataFlow',config=dict(pipeline='main'))], 'constraints': []}, 'dataFlows': []}
        inspect = dict(package=draft_package,connection='input',format='csv',options=dict(header=True))
        status, discovered = request('admin-a', '/files/inspect', 'POST', inspect)
        assert status == 200, (status, discovered)
        assert [field['name'] for field in discovered['schema']['fields']] == ['Amount', 'Customer']
        assert request('peer-a','/files/inspect','POST',inspect)[0] == 403
        assert request('admin-b','/files/inspect','POST',inspect)[0] == 403
        source = dict(id='read',component='file.csv.source',config=dict(connection='input',schema=discovered['schema'],fileFormat=discovered['metadata']['options']))
        draft_package['dataFlows'] = [dict(id='main',components=[source,
            dict(id='calculation',component='derive',config=dict(columns=[dict(action='add',name='Doubled',expression='[Amount] * 2L')])),
            dict(id='write',component='file.csv.destination',config=dict(connection='output',fileMode='overwrite'))],
            paths=[dict(**{'from':'read','to':'calculation'},output=0,input=0),dict(**{'from':'calculation','to':'write'},output=0,input=0)])]
        preview_request = dict(package=draft_package,pipelineId='main',componentId='write',sourceRows=2,sampleRows=2)
        status, preview = request('admin-a','/preview','POST',preview_request)
        assert status == 200 and preview['result'] == 0, (status, preview)
        edges = preview['snapshot']['preview']
        assert next(edge for edge in edges if edge['component']=='calculation')['totalRows'] == 2
        assert target.read_text() == 'untouched\n'
        assert request('reader-a','/preview','POST',preview_request)[0] == 403
        message_package = {**draft_package, 'id': 'message-shape',
            'connections': [dict(id='orders', driver='kafka', resource='preview-only-broker')],
            'dataFlows': [dict(id='messages', components=[dict(id='receive', component='mq.source',
                config=dict(connection='orders', targetType='topic', address='orders', group='preview',
                            format='json', includeMetadata=False, schema=discovered['schema']))], paths=[])]}
        message_request = dict(package=message_package, pipelineId='messages', componentId='receive')
        status, preview = request('admin-a', '/preview', 'POST', message_request)
        assert status == 200 and preview['result'] == 0 and preview['sampled'] is False, (status, preview)
        payload = next(edge for edge in preview['snapshot']['preview'] if edge['port'] == 0)
        assert payload['totalRows'] == 0
        assert [field['name'] for field in payload['schema']['fields']] == ['Amount', 'Customer']
        write = dict(document=draft_package,expectedRevision='0',baseRevision='0')
        assert request('admin-a','/drafts/designer-draft','PUT',write)[0] == 200
        assert request('admin-a','/drafts/designer-draft','PUT',write)[0] == 409
        status, drafts = request('admin-a','/drafts')
        assert status == 200 and any(draft['packageId']=='designer-draft' for draft in drafts)
        assert request('peer-a','/drafts')[1] == []
        assert request('admin-b','/drafts')[1] == []
        assert request('peer-a','/drafts/designer-draft')[0] == 404
        assert request('admin-b','/drafts/designer-draft')[0] == 404
        assert request('reader-a','/drafts/designer-draft','PUT',write)[0] == 403
        status, version = request('admin-a','/versions','POST',dict(document=draft_package,expectedPackageRevision='0'))
        assert status == 200 and version['revision'] == '1', (status, version)
        draft_package['dataFlows'][0]['components'][1]['config']['columns'][0]['expression'] = '[Amount] * 3L'
        publication_request = dict(requestId='designer-publication', document=draft_package,
                                   expectedPackageRevision='1', schedules=[], run=False)
        status, publication = request('admin-a','/publications','POST',publication_request)
        assert status == 200 and publication['revision'] == '2', (status, publication)
        status, replay = request('admin-a','/publications','POST',publication_request)
        assert status == 200 and replay == publication, (status, replay)
        status, stored = request('admin-a','/packages/designer-draft/versions/2')
        assert status == 200 and stored['dataFlows'][0]['components'][1]['config']['columns'][0]['expression'] == '[Amount] * 3L'
        assert target.read_text() == 'untouched\n'
        draft_package['dataFlows'][0]['components'][1]['config']['columns'][0]['expression'] = '[Amount] * 4L'
        assert request('admin-a','/publications','POST',publication_request)[0] == 409
        status, current_runs = request('admin-a','/executions')
        assert status == 200 and all(run['packageId']!='designer-draft' for run in current_runs)
        assert request('admin-a','/tests','POST',dict(package=draft_package,targetConnection='output',tables={}))[0] == 403
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
    # A live Catalog can hold mandatory file locks on Windows.
    for path in data.rglob('*'):
        if path.is_file():
            assert secret.encode() not in path.read_bytes(), str(path)
print('Actual Management/Worker/Native: authentication, CSRF, roles, upload ownership, Native discovery/preview, draft/version CAS, current-draft publication without execution, protected parameters, logs and rotation passed')

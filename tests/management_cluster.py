"""Two real management processes share Native claims and independent Workers.

The HTTP service records actual POST side effects. A killed dispatcher must
never cause another node to replay an uncertain POST, including manual resume.
"""
import base64
import collections
import hashlib
import http.cookiejar
import http.server
import json
import pathlib
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

dotnet, management, worker = sys.argv[1:4]
lock = threading.Lock()
calls = collections.Counter()
active = 0
maximum = 0
uncertain = threading.Event()

class Endpoint(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        global active, maximum
        body = self.rfile.read(int(self.headers['Content-Length'])).decode()
        with lock:
            calls[body] += 1
            active += 1
            maximum = max(maximum, active)
        try:
            if body == 'uncertain':
                uncertain.set()
                time.sleep(8)
            else:
                time.sleep(.35)
            self.send_response(200)
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'{}')
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with lock:
                active -= 1
    def log_message(self, *_):
        pass

endpoint = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Endpoint)
threading.Thread(target=endpoint.serve_forever, daemon=True).start()
origin = f'http://127.0.0.1:{endpoint.server_port}'
with tempfile.TemporaryDirectory(prefix='ud-management-cluster-') as directory:
    root = pathlib.Path(directory)
    resources = root / 'resources'
    resources.mkdir()
    salt = b'cluster-identity-salt'.ljust(32, b'0')
    password = 'cluster-password-2985671'
    password_hash = 'pbkdf2-sha256$600000$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000)).decode()
    identity = root / 'identity.json'
    identity.write_text(json.dumps({'version': 1, 'users': [{'id': 'operator', 'tenant': 'cluster', 'passwordHash': password_hash,
        'stamp': '1' * 64, 'roles': ['operator']}], 'roles': {'operator': [{'permission': '*', 'resource': '*'}]}}))
    policy = root / 'policy.json'
    policy.write_text(json.dumps({'dataDirectory': str(root / 'product'), 'fileRoots': [str(resources)], 'networkOrigins': [origin],
        'pluginRoots': [], 'credentialFiles': {}, 'maxConcurrency': 2, 'executionTimeoutSeconds': 90, 'cancellationGraceSeconds': 2,
        'identityFile': str(identity), 'tenants': {'cluster': {'fileRoots': [str(resources)], 'networkOrigins': [origin],
        'pluginRoots': [], 'credentialReferences': []}}}))
    processes = []
    clients = []
    addresses = []
    logs = []
    def request(index, path, method='GET', value=None):
        opener, token = clients[index]
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['X-UD-CSRF'] = token
        try:
            response = opener.open(urllib.request.Request(addresses[index] + '/api' + path, method=method, headers=headers,
                data=None if value is None else json.dumps(value).encode()), timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        data = response.read().decode()
        return response.code, json.loads(data) if data else None
    def wait_state(index, ticket, predicate, seconds=40):
        deadline = time.monotonic() + seconds
        while True:
            status, state = request(index, '/executions/' + ticket)
            assert status == 200, (status, state)
            if predicate(state):
                return state
            assert time.monotonic() < deadline, state
            time.sleep(.15)
    def enqueue(index, name):
        package = {'version': 12, 'id': name, 'connections': [{'id': 'http', 'driver': 'http', 'resource': origin + '/effects'}],
            'controlFlow': {'tasks': [{'id': 'post', 'kind': 'http.request', 'config': {'connection': 'http', 'method': 'POST',
                'body': name, 'timeoutMs': 30000}}], 'constraints': []}, 'dataFlows': []}
        assert request(index, '/packages/' + name + '/versions/1', 'PUT', package)[0] == 200
        status, run = request(index, '/executions', 'POST', {'packageId': name, 'packageRevision': '1'})
        assert status == 202, (status, run)
        return run['id']
    try:
        for index in range(2):
            with socket.socket() as reserve:
                reserve.bind(('127.0.0.1', 0))
                addresses.append(f'http://127.0.0.1:{reserve.getsockname()[1]}')
            clients.append((urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())), ''))
            log = open(root / f'manager-{index}.txt', 'w+')
            logs.append(log)
            processes.append(subprocess.Popen([dotnet, management, '--policy', str(policy), '--worker', worker, '--node', f'node-{index}',
                '--urls', addresses[index]], stdout=log, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + 30
            while True:
                assert processes[index].poll() is None, (root / f'manager-{index}.txt').read_text()
                try:
                    if request(index, '/health')[0] == 401:
                        break
                except (urllib.error.URLError, OSError):
                    pass
                assert time.monotonic() < deadline
                time.sleep(.1)
            status, session = request(index, '/auth/login', 'POST', {'subject': 'operator', 'password': password})
            assert status == 200, (status, session)
            clients[index] = (clients[index][0], session['csrf'])
        tickets = [enqueue(index % 2, f'work-{index}') for index in range(8)]
        results = [wait_state(index % 2, ticket, lambda state: state['state'] not in ['Queued', 'Claimed', 'Running']) for index, ticket in enumerate(tickets)]
        assert all(run['state'] == 'Succeeded' for run in results), results
        assert len({run['executionId'] for run in results}) == 8
        with lock:
            assert calls == collections.Counter({f'work-{index}': 1 for index in range(8)}), calls
            assert maximum <= 2, maximum
        # Real unknown-effect boundary: the service accepted POST, then the owning
        # management process dies. Standby only fences/reconciles; no auto retry.
        ticket = enqueue(0, 'uncertain')
        assert uncertain.wait(25)
        running = wait_state(1, ticket, lambda state: state['state'] == 'Running')
        owner = int(running['workerNode'].removeprefix('node-'))
        standby = 1 - owner
        native_id = running['executionId']
        processes[owner].kill()
        processes[owner].wait(timeout=10)
        recovered = wait_state(standby, ticket, lambda state: state['state'] == 'RecoveryRequired', seconds=45)
        assert recovered['executionId'] == native_id, recovered
        with lock:
            assert calls['uncertain'] == 1, calls
        # Native Journal remains the recovery authority and refuses the pending
        # response-confirmed write; the dispatcher cannot reinterpret it as retry.
        deadline=time.monotonic()+5
        while True:
            status, resumed = request(standby, '/executions/' + ticket + '/resume', 'POST')
            if status!=400 or time.monotonic()>=deadline:break
            # Reconciliation may have saved RecoveryRequired just before it
            # releases its lease. The recovery guard must continue to fence it.
            assert request(standby,'/executions/'+ticket)[1]['state']=='RecoveryRequired'
            time.sleep(.1)
        assert status == 202 and resumed['executionId'] == native_id, (status, resumed)
        terminal = wait_state(standby, ticket, lambda state: state['state'] == 'RecoveryRequired')
        assert terminal['executionId'] == native_id, terminal
        with sqlite3.connect(root / 'product' / 'tenants' / 'cluster' / 'catalog' / 'catalog.sqlite3') as native_catalog:
            snapshots = [json.loads(row[0]) for row in native_catalog.execute(
                'SELECT document FROM documents WHERE tenant=? AND kind=5 AND id=? ORDER BY revision', ('cluster', native_id))]
        # A host preflight failure retaining RecoveryRequired cannot satisfy this:
        # the resumed Native execution must actually publish its terminal fact.
        assert len(snapshots) >= 3 and snapshots[-1]['result'] == 16, snapshots
        with lock:
            assert calls['uncertain'] == 1, calls
        print(json.dumps({'managers': 2, 'workers': 8, 'maximumConcurrentEffects': maximum, 'nativeIds': len({run['executionId'] for run in results}),
            'unknownPostEffects': calls['uncertain'], 'sameNativeId': True, 'automaticReplay': False, 'manualUnknownReplay': False,
            'nativeSnapshots': len(snapshots), 'nativeTerminalResult': snapshots[-1]['result']}))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        for log in logs:
            log.close()
endpoint.shutdown()

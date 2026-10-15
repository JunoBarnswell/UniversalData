"""Real local Designer -> Management -> Worker -> Native editing and recovery.

The running host must authorize --root and the selected subject. No JSON editor,
JavaScript evaluator, simulated Worker, or mocked data is used by this journey.
"""
import argparse
import http.cookiejar
import json
import os
import pathlib
import signal
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from playwright.sync_api import sync_playwright, expect
from browser_canvas import connect as connect_nodes

parser = argparse.ArgumentParser()
parser.add_argument('--url', required=True)
parser.add_argument('--root', required=True, type=pathlib.Path)
parser.add_argument('--evidence', required=True, type=pathlib.Path)
parser.add_argument('--browser-executable', type=pathlib.Path)
parser.add_argument('--subject', default='acceptance')
args = parser.parse_args()
password = os.environ['UD_BROWSER_PASSWORD']
args.evidence.mkdir(parents=True, exist_ok=True)
identity = 'browser-' + uuid.uuid4().hex
root = args.root / identity
root.mkdir()
source = root / 'input.csv'
source.write_text('id,label\n1,first\n2,second\n', encoding='utf-8')
output = root / 'output.ndjson'
client = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
csrf = ''
def api(method, path, value=None):
    request = urllib.request.Request(args.url + '/api' + path, method=method,
        data=None if value is None else json.dumps(value).encode(),
        headers={'Content-Type': 'application/json', 'X-UD-CSRF': csrf})
    with client.open(request, timeout=65) as response:
        data = response.read()
        return json.loads(data) if data else None

def wait_run(ticket, state):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        run = api('GET', '/executions/' + ticket)
        if run['state'] == state:
            return run
        if run['state'] in ('Failed', 'Cancelled', 'Succeeded'):
            raise AssertionError((state, run))
        time.sleep(.1)
    raise AssertionError((state, run))

def paused(ticket, samples=False):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        snapshot = api('GET', f'/executions/{ticket}/debug')
        if snapshot['paused'] and (not samples or snapshot['preview']):
            return snapshot
        time.sleep(.1)
    raise AssertionError(snapshot)

csrf = api('POST', '/auth/login', {'subject': args.subject, 'password': password})['csrf']
errors, console, http = [], [], []
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(executable_path=str(args.browser_executable) if args.browser_executable else None,
        headless=True, args=['--no-sandbox'])
    page = browser.new_page(viewport=dict(width=1600, height=1050))
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('console', lambda message: console.append(dict(level=message.type,text=message.text)) if message.type in ('error','warning') else None)
    page.on('response', lambda response: http.append(dict(status=response.status,path=urllib.parse.urlsplit(response.url).path)) if response.status >= 400 else None)
    def capture_native(response):
        if response.url.endswith('/api/preview'):
            try:
                result=response.json()
                if result.get('result'):
                    (args.evidence/'native-preview-failure.json').write_text(json.dumps(dict(request=response.request.post_data_json,result=result),indent=2))
            except (ValueError,KeyError):
                pass
    page.on('response',capture_native)
    def save(name):
        page.screenshot(path=str(args.evidence/name), full_page=True)
    def select_node(identifier):
        page.get_by_role('button', name='Fit View', exact=True).click()
        node = page.locator(f'.react-flow__node[data-id="{identifier}"]')
        node.click()
        return node
    def add_component(component):
        page.get_by_label('搜索组件', exact=True).fill(component)
        page.locator('.catalog-row').filter(has_text=component).click()
        return page.locator('.react-flow__node.selected').get_attribute('data-id')
    def connect(left, right):
        connect_nodes(page, left, right)
    try:
        page.goto(args.url)
        page.get_by_role('textbox', name='账户', exact=True).fill(args.subject)
        page.get_by_label('密码', exact=True).fill(password)
        page.get_by_role('button', name='登录', exact=True).click()
        page.get_by_role('heading', name='项目与数据任务', exact=True).wait_for()
        page.get_by_role('button', name='从 CSV / Excel 导入', exact=True).click()
        wizard=page.get_by_role('dialog', name='文件导入向导')
        wizard.get_by_label('上传数据文件').set_input_files(source)
        expect(wizard.get_by_label('授权文件路径')).not_to_have_value('')
        wizard.get_by_role('button', name='发现字段与样本', exact=True).click()
        expect(wizard.get_by_label('确认文件格式与字段')).to_be_enabled(timeout=30000)
        save('file-discovery.png')
        wizard.get_by_label('确认文件格式与字段').check()
        wizard.get_by_role('button', name='添加数据源', exact=True).click()
        page.get_by_label('作业包名称').fill(identity)
        source_id=page.locator('.react-flow__node').first.get_attribute('data-id')
        derived_id=add_component('derive')
        connect(source_id,derived_id)
        select_node(derived_id)
        page.get_by_role('button',name='编辑派生列',exact=True).click()
        dialog=page.locator('dialog[open]')
        dialog.get_by_role('button',name='新增列写入',exact=True).click()
        dialog.get_by_label('输出列 1',exact=True).fill('plus')
        formula=dialog.locator('.cm-content').first
        formula.fill('(DT_I8)[id] + 1L')
        dialog.get_by_role('button',name='计算真实样本',exact=True).click()
        expect(dialog.locator('.row-comparison tbody tr').first).to_be_visible(timeout=30000)
        save('native-derived-before-after.png')
        formula.fill('"中文" + [missing]')
        dialog.get_by_role('button',name='Native 编译',exact=True).click()
        dialog.get_by_role('button',name='定位',exact=False).wait_for(timeout=15000)
        dialog.get_by_role('button',name='定位',exact=False).click()
        formula.fill('(DT_I8)[id] + 1L')
        dialog.get_by_role('button',name='完成',exact=True).click()
        page.get_by_role('button',name='连接中心',exact=True).click()
        center=page.get_by_role('dialog',name='连接中心')
        center.get_by_role('button',name='新建连接',exact=True).click()
        center.get_by_label('数据源类型',exact=True).select_option('file')
        center.get_by_label('授权资源',exact=True).fill(str(output))
        output_connection=center.get_by_label('连接名称',exact=True).input_value()
        center.get_by_role('button',name='关闭连接中心',exact=True).click()
        destination_id=add_component('file.ndjson.destination')
        page.get_by_label('组件连接',exact=True).select_option(output_connection)
        connect(derived_id,destination_id)
        select_node(derived_id)
        node=page.locator(f'.react-flow__node[data-id="{derived_id}"]')
        position=node.bounding_box()
        page.mouse.move(position['x']+35,position['y']+15);page.mouse.down();page.mouse.move(position['x']+55,position['y']+50,steps=8);page.mouse.up()
        page.get_by_role('button',name='撤销',exact=True).click()
        page.get_by_role('button',name='重做',exact=True).click()
        select_node(destination_id)
        page.get_by_role('button',name='预览',exact=True).click()
        page.get_by_text('Native 真实样本 · 目标未执行',exact=True).wait_for(timeout=30000)
        assert not output.exists()
        with page.expect_response(lambda response: response.url.endswith('/api/publications') and response.request.method=='POST') as publication:
            page.get_by_role('button',name='发布版本',exact=True).click()
        package_id=publication.value.json()['deployment']['packageId']
        page.get_by_text('已发布 v1，等待正式执行',exact=True).wait_for(timeout=30000)
        assert not output.exists() and not [run for run in api('GET','/executions') if run['packageId']==package_id]
        select_node(derived_id)
        page.get_by_role('button',name='编辑派生列',exact=True).click()
        dialog=page.locator('dialog[open]');dialog.locator('.cm-content').first.fill('(DT_I8)[id] + 2L');dialog.get_by_role('button',name='完成',exact=True).click()
        page.get_by_role('button',name='发布版本',exact=True).click()
        page.get_by_text('已发布 v2，等待正式执行',exact=True).wait_for(timeout=30000)
        document=api('GET',f'/packages/{package_id}/versions/2')
        assert next(node for node in document['dataFlows'][0]['components'] if node['id']==derived_id)['config']['columns'][0]['expression']=='(DT_I8)[id] + 2L'
        save('designer.png')
        page.get_by_role('button',name='正式执行',exact=True).click()
        confirm=page.get_by_role('dialog',name='确认正式执行')
        expect(confirm.get_by_role('button',name='确认正式执行',exact=True)).to_be_disabled()
        confirm.get_by_label('开启 Native 调试与有界数据查看').check()
        confirm.get_by_label('run-main',exact=True).check()
        confirm.get_by_label('确认正式业务写入').check()
        with page.expect_response(lambda response: response.url.endswith('/api/executions') and response.request.method=='POST') as sent:
            confirm.get_by_role('button',name='确认正式执行',exact=True).click()
        ticket=sent.value.json()['id']
        active=wait_run(ticket,'Running');paused(ticket)
        assert not output.exists()
        page.locator('.run-monitor').get_by_role('button',name='单批次',exact=True).click()
        paused(ticket, samples=True)
        save('debug-step.png')
        os.kill(active['processId'],getattr(signal,'SIGKILL',signal.SIGTERM))
        uncertain=wait_run(ticket,'RecoveryRequired');assert uncertain['executionId']==active['executionId']
        page.get_by_role('button',name='执行',exact=True).click()
        page.get_by_role('row').filter(has_text=active['executionId']).get_by_role('button',name='查看',exact=True).click()
        page.get_by_role('button',name='恢复',exact=True).click()
        resumed=wait_run(ticket,'Running');assert resumed['executionId']==active['executionId']
        paused(ticket)
        page.get_by_role('button',name='继续',exact=True).first.click()
        recovered=wait_run(ticket,'Succeeded')
        assert [json.loads(line) for line in output.read_text().splitlines()]==[dict(id=1,label='first',plus=3),dict(id=2,label='second',plus=4)]
        logs=api('GET','/logs?executionId='+recovered['executionId']);assert logs and len({entry['revision'] for entry in logs})==len(logs)
        save('executions.png')
        page.get_by_role('button',name='作业',exact=True).click();page.get_by_role('heading',name='作业管理',exact=True).wait_for()
        page.get_by_role('button',name='项目',exact=True).click();page.get_by_role('row').filter(has_text=identity).get_by_role('button',name='打开工作台').click()
        expect(page.get_by_label('作业包名称')).to_have_value(identity)
        for width,height,name in [(768,950,'designer-compact.png'),(390,844,'designer-phone.png')]:
            page.set_viewport_size(dict(width=width,height=height));save(name)
        assert not errors, errors
        unexpected=[value for value in http if not(value['status']==401 and value['path']=='/api/auth/session' or value['status']==404 and '/api/drafts/' in value['path'])]
        # A killed Worker has no final debug snapshot. Its explicit recovery
        # operation is still available; a live control endpoint is not invented.
        unexpected=[value for value in unexpected if not(value['status'] in (400,503) and '/executions/' in value['path'] and (value['path'].endswith('/debug') or value['path'].endswith('/metrics')))]
        assert not unexpected,unexpected
        (args.evidence/'browser.json').write_text(json.dumps(dict(status='Passed',packageId=package_id,sourceDiscovery=True,noJsonEditor=True,
            nativeFormulaCompile=True,nativeBeforeAfter=True,dragUndoRedo=True,currentDraftPublication=True,publishDoesNotExecute=True,previewDoesNotWrite=True,
            definitionBreakpoint=True,nativeStep=True,actualWorkerTermination=True,resumeSameNativeId=True,outputSignature=__import__('hashlib').sha256(output.read_bytes()).hexdigest(),
            browserErrors=errors,consoleMessages=console,httpErrors=http,browserPlugin='Browser plugin not available'),indent=2))
    except BaseException:
        (args.evidence/'failure-runs.json').write_text(json.dumps(api('GET','/executions'),indent=2))
        (args.evidence/'failure-console.json').write_text(json.dumps(dict(errors=errors,console=console,http=http),indent=2))
        save('failure.png');(args.evidence/'failure.txt').write_text(page.locator('body').inner_text());(args.evidence/'failure-nodes.json').write_text(json.dumps(page.locator('.react-flow__node').evaluate_all('(nodes)=>nodes.map(node=>({id:node.dataset.id,style:node.getAttribute("style"),box:node.getBoundingClientRect().toJSON()}))'),indent=2));raise
    finally:
        browser.close()
print('Real browser file import, Native formulas/preview, current-draft publication, breakpoints/step, Worker kill and recovery passed')

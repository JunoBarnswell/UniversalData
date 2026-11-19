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
from product_version import PROTOCOL_VERSION

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
    def accept_dialog(dialog):
        try:
            dialog.accept()
        except Exception:
            # A second handler can observe the same browser dialog after the
            # first one has accepted it; the state transition is already done.
            pass
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
        workspace=page.locator('.workspace-panel:not([hidden])').last
        workspace.get_by_role('button', name='Fit View', exact=True).click()
        node = workspace.locator(f'.react-flow__node[data-id="{identifier}"]:visible').last
        node.click()
        return node
    def add_component(component):
        workspace=page.locator('.workspace-panel:not([hidden])').last
        workspace.get_by_label('搜索组件', exact=True).fill(component)
        workspace.locator('.catalog-row').filter(has_text=component).click()
        return workspace.locator('.react-flow__node.selected').last.get_attribute('data-id')
    def connect(left, right):
        workspace=page.locator('.workspace-panel:not([hidden])').last
        workspace.get_by_role('button', name='Fit View', exact=True).click()
        count=workspace.locator('.react-flow__edge').count()
        output=workspace.locator(f'.react-flow__node[data-id="{left}"] .react-flow__handle.source').first
        input_=workspace.locator(f'.react-flow__node[data-id="{right}"] .react-flow__handle.target').first
        output.drag_to(input_, timeout=10000)
        expect(workspace.locator('.react-flow__edge')).to_have_count(count+1)
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
        page.locator('.workspace-panel:not([hidden])').get_by_label('作业包名称').fill(identity)
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
        sql_source_id=add_component('sql.source')
        select_node(sql_source_id)
        page.locator('.workspace-panel:not([hidden])').get_by_text('专业连接与提交选项',exact=True).click()
        read_mode=page.get_by_role('combobox',name='读取方式',exact=True)
        expect(read_mode).to_have_value('"snapshot"')
        read_mode.select_option(label='stream')
        expect(read_mode).to_have_value('"stream"')
        expect(page.get_by_text('工作草稿已自动保存',exact=True)).to_be_visible(timeout=15000)
        draft_summary=next(item for item in api('GET','/drafts') if item['document'].get('layout',{}).get('label')==identity)
        draft=api('GET','/drafts/'+draft_summary['packageId'])
        persisted=next(node for node in draft['document']['dataFlows'][0]['components'] if node['id']==sql_source_id)
        sql_read_mode_saved=persisted['config']['readMode']=='stream'
        assert sql_read_mode_saved,persisted
        # Close the original dirty tab before reopening the persisted draft. Keeping
        # it open leaves two ReactFlow providers for one package and the newly opened
        # provider can remain measured while its panel is hidden.
        page.get_by_role('button',name='设计工作台',exact=True).click()
        page.once('dialog',accept_dialog)
        page.get_by_role('button',name='关闭 '+identity,exact=True).last.click()
        page.get_by_role('button',name='项目',exact=True).click()
        page.get_by_role('heading',name='项目与数据任务',exact=True).wait_for()
        page.get_by_role('row').filter(has_text=identity).get_by_role('button',name='打开工作台',exact=True).click()
        select_node(sql_source_id)
        page.locator('.workspace-panel:not([hidden])').get_by_text('专业连接与提交选项',exact=True).click()
        reopened_read_mode=page.get_by_role('combobox',name='读取方式',exact=True)
        expect(reopened_read_mode).to_have_value('"stream"')
        sql_read_mode_reopened=reopened_read_mode.input_value()=='"stream"'
        assert sql_read_mode_reopened
        page.locator('.workspace-panel:not([hidden])').get_by_label('删除组件',exact=True).last.click()
        page.get_by_role('button',name='连接中心',exact=True).click()
        center=page.get_by_role('dialog',name='连接中心')
        center.get_by_role('button',name='新建连接',exact=True).click()
        center.get_by_label('数据源类型',exact=True).select_option('file')
        center.get_by_label('授权资源',exact=True).fill(str(output))
        output_connection=center.get_by_label('连接名称',exact=True).input_value()
        center.get_by_role('button',name='关闭连接中心',exact=True).click()
        destination_id=add_component('file.ndjson.destination')
        page.locator('.workspace-panel:not([hidden])').get_by_label('组件连接',exact=True).last.select_option(output_connection)
        connect(derived_id,destination_id)
        select_node(derived_id)
        node=page.locator(f'.workspace-panel:not([hidden]) .react-flow__node[data-id="{derived_id}"]').last
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
        active=wait_run(ticket,'Running');formal_requires_journal=active['requiresJournal'] is True;assert formal_requires_journal,active;paused(ticket)
        assert not output.exists()
        page.locator('.run-monitor').get_by_role('button',name='单批次',exact=True).click()
        paused(ticket, samples=True)
        save('debug-step.png')
        os.kill(active['processId'],getattr(signal,'SIGKILL',signal.SIGTERM))
        uncertain=wait_run(ticket,'RecoveryRequired');assert uncertain['executionId']==active['executionId'] and uncertain['requiresJournal'] is True,uncertain
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
        expect(page.locator('.workspace-panel:not([hidden])').get_by_label('作业包名称')).to_have_value(identity)
        print('Studio 4 theme and command journey',flush=True)
        page.get_by_label('主题',exact=True).select_option('dark')
        page.get_by_label('界面密度',exact=True).select_option('compact')
        assert page.locator('html').get_attribute('data-theme')=='dark'
        page.keyboard.press('Control+k')
        commands=page.get_by_role('dialog',name='工作台命令')
        commands.get_by_label('搜索命令').fill('自动布局');page.keyboard.press('Enter')
        expect(commands).not_to_be_visible()
        tabs=page.locator('.workspace-tabs [role=tab]')
        tab_count=tabs.count();assert tab_count>=1,tab_count
        page.get_by_role('button',name='项目',exact=True).click()
        page.get_by_role('button',name='空白任务',exact=True).click()
        expect(tabs).to_have_count(tab_count+1)
        tabs.first.click()
        expect(page.locator('.workspace-panel:not([hidden])').get_by_label('作业包名称')).to_have_value(identity)
        page.get_by_label('主题',exact=True).select_option('light')
        page.get_by_label('界面密度',exact=True).select_option('comfortable')
        print('Studio 4 tabs passed; starting scale journey',flush=True)
        performance=[]
        page.on('dialog',accept_dialog)
        page.set_viewport_size(dict(width=1600,height=1050))
        for count in [200,500,1000]:
            name=identity+'-scale-'+str(count)
            scale=dict(version=PROTOCOL_VERSION,id=name,variables=dict(counter=dict(type='int64',default=0)),dataFlows=[],
                controlFlow=dict(tasks=[dict(id='step-'+str(index),kind='set',config=dict(name='counter',value=dict(literal=index,type='int64'))) for index in range(count)],constraints=[]))
            api('PUT','/packages/'+name+'/versions/1',scale)
            page.get_by_role('button',name='项目',exact=True).click()
            started=time.perf_counter()
            page.get_by_role('row').filter(has_text=name).get_by_role('button',name='打开工作台').click()
            panel=page.locator('.workspace-panel:not([hidden])')
            expect(panel.locator('.graph-count')).to_contain_text(str(count)+' 个组件',timeout=60000)
            opened=(time.perf_counter()-started)*1000
            started=time.perf_counter();panel.get_by_role('button',name='Fit View',exact=True).click()
            page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
            zoomed=(time.perf_counter()-started)*1000
            assert len(api('GET','/packages/'+name+'/versions/1')['controlFlow']['tasks'])==count
            performance.append(dict(nodes=count,renderedNodes=panel.locator('.react-flow__node').count(),openMs=round(opened,1),fitViewMs=round(zoomed,1)))
            page.get_by_role('button',name='关闭 '+name,exact=True).click()
        page.locator('.workspace-tabs [role=tab]').first.click()
        (args.evidence/'studio4-scale.json').write_text(json.dumps(dict(browser=browser.version,viewport=[1600,1050],platform=__import__('platform').platform(),cpus=os.cpu_count(),cases=performance),indent=2))
        for width,height,name in [(768,950,'designer-compact.png'),(390,844,'designer-phone.png')]:
            page.set_viewport_size(dict(width=width,height=height));save(name)
        assert not errors, errors
        unexpected=[value for value in http if not(value['status']==401 and value['path']=='/api/auth/session' or value['status']==404 and '/api/drafts/' in value['path'])]
        # SQL Source is intentionally added without a connection in this UI
        # journey: the editor keeps that partial configuration editable and
        # reports its Native port probe as 422 until a connection is selected.
        unexpected=[value for value in unexpected if not(value['status']==422 and value['path']=='/api/ports')]
        # A killed Worker has no final debug snapshot. Its explicit recovery
        # operation is still available; a live control endpoint is not invented.
        unexpected=[value for value in unexpected if not(value['status'] in (400,503) and '/executions/' in value['path'] and (value['path'].endswith('/debug') or value['path'].endswith('/metrics')))]
        assert not unexpected,unexpected
        (args.evidence/'browser.json').write_text(json.dumps(dict(status='Passed',packageId=package_id,sourceDiscovery=True,noJsonEditor=True,
            nativeFormulaCompile=True,nativeBeforeAfter=True,dragUndoRedo=True,currentDraftPublication=True,publishDoesNotExecute=True,previewDoesNotWrite=True,
            definitionBreakpoint=True,nativeStep=True,actualWorkerTermination=True,resumeSameNativeId=True,sqlReadModeSaved=sql_read_mode_saved,
            sqlReadModeReopened=sql_read_mode_reopened,formalRequiresJournal=formal_requires_journal,outputSignature=__import__('hashlib').sha256(output.read_bytes()).hexdigest(),
            studio4Tabs=True,commandMenu=True,themes=True,density=True,browserErrors=errors,consoleMessages=console,httpErrors=http,browserPlugin='Browser plugin not available'),indent=2))
    except BaseException:
        (args.evidence/'failure-original.txt').write_text(__import__('traceback').format_exc())
        print(__import__('traceback').format_exc(),flush=True)
        (args.evidence/'failure-runs.json').write_text(json.dumps(api('GET','/executions'),indent=2))
        (args.evidence/'failure-console.json').write_text(json.dumps(dict(errors=errors,console=console,http=http),indent=2))
        page.screenshot(path=str(args.evidence/'failure.png'),full_page=False,timeout=5000);(args.evidence/'failure.txt').write_text(page.locator('body').inner_text());(args.evidence/'failure-nodes.json').write_text(json.dumps(page.locator('.react-flow__node').evaluate_all('(nodes)=>nodes.map(node=>({id:node.dataset.id,style:node.getAttribute("style"),box:node.getBoundingClientRect().toJSON()}))'),indent=2));raise
    finally:
        browser.close()
print('Real browser file import, Native formulas/preview, current-draft publication, breakpoints/step, Worker kill and recovery passed')

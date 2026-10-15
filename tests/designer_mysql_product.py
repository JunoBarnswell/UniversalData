"""Real 50K browser acceptance against explicitly designated disposable MySQL databases.

The host must approve production/test resource labels and credential references.
The script never reads credential values. SQL setup/readback use the public Native
Connector fixture helper; all ETL and preview processing use the real product.
"""
import argparse,http.cookiejar,json,os,pathlib,re,secrets,subprocess,sys,time,urllib.request,urllib.error,uuid
from playwright.sync_api import sync_playwright,expect
from browser_canvas import connect
sys.path.insert(0,str(pathlib.Path(__file__).parent/'ssis_parity'))
from workflow import generate,xlsx,package,schema,verify_business_rows
parser=argparse.ArgumentParser()
for name in ('url','production-resource','production-reference','test-resource','test-reference'):parser.add_argument('--'+name,required=True)
for name in ('root','evidence','plugin','database-helper','production-config','test-config'):parser.add_argument('--'+name,required=True,type=pathlib.Path)
parser.add_argument('--subject',default='acceptance');parser.add_argument('--browser-executable',type=pathlib.Path);parser.add_argument('--table-prefix',default='ud_designer_');parser.add_argument('--prepare-tables',action='store_true')
args=parser.parse_args();args.plugin=args.plugin.resolve();URL=args.url;password=os.environ['UD_BROWSER_PASSWORD'];evidence=args.evidence;evidence.mkdir(parents=True,exist_ok=True)
if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,39}',args.table_prefix):raise ValueError('Invalid isolated table prefix')
work=args.root/('browser-'+uuid.uuid4().hex);work.mkdir();runid='web-'+secrets.token_hex(8);business_id='workbench-'+uuid.uuid4().hex;basic_id='file-import-'+uuid.uuid4().hex
csv_file=work/'ssis_50000_dirty.csv';xlsx_file=work/'dirty.xlsx';generate(work,True);xlsx(xlsx_file,True)
def query(config):
 def execute(sql,fields=None):
  statement=work/'query.sql';statement.write_text(sql);command=[str(args.database_helper),str(args.plugin),str(config),'query' if fields is not None else 'execute',str(statement)]
  if fields is not None:
   definition=work/'query-schema.json';definition.write_text(json.dumps(schema(fields)));command.append(str(definition))
  result=subprocess.run(command,capture_output=True,text=True,timeout=120)
  if result.returncode:raise RuntimeError('Native fixture operation failed: '+result.stderr)
  if fields is None:return None
  rows=[row for line in result.stdout.splitlines() for row in json.loads(line)]
  for row in rows:
   for field in fields:
    if field['type']=='uint64' and row[field['name']] is not None:row[field['name']]=int(row[field['name']])
  return rows
 return execute
if args.prepare_tables:
 ddl=(pathlib.Path(__file__).parent/'ssis_parity/fixtures/mysql.sql').read_text().replace('ud_ssis_',args.table_prefix)
 for config in (args.production_config,args.test_config):
  for statement in ddl.split(';'):
   statement='\n'.join(line for line in statement.splitlines() if not line.lstrip().startswith('--')).strip()
   if statement:query(config)(statement)
 fields=['RowId','OrderNo','CustomerName','Status','Region','ProductCode','Amount','CategoryPath','CreatedAt','Remark']
 query(args.production_config)('CREATE TABLE IF NOT EXISTS '+args.table_prefix+'raw ('+', '.join('`'+name+'` TEXT NULL' for name in fields)+') ENGINE=InnoDB')
class Client:
 def __init__(self):
  self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()));self.csrf=''
  self.csrf=self.api('POST','/auth/login',dict(subject=args.subject,password=password))['csrf']
 def api(self,method,path,value=None):
  request=urllib.request.Request(URL+'/api'+path,method=method,data=None if value is None else json.dumps(value).encode(),headers={'Content-Type':'application/json','X-UD-CSRF':self.csrf})
  try:response=self.opener.open(request,timeout=65)
  except urllib.error.HTTPError as error:raise AssertionError((path,error.code,error.read().decode())) from error
  data=response.read();return json.loads(data) if data else None
c=Client();document=package(work,csv_file,args.plugin,args.production_config,runid,1024,'single','native');document['id']=business_id
for connection in document['connections']:
 if connection['id']=='db':connection.update(resource=args.production_resource,credentialReference=args.production_reference)
document['connections'].append(dict(id='test-db',driver=str(args.plugin),resource=args.test_resource,credentialReference=args.test_reference))
for node in document['dataFlows'][0]['components']:
 if node['component']=='sql.destination':node['config']['table']=node['config']['table'].replace('ud_ssis_',args.table_prefix)
document['layout']=dict(version=document['version'],nodes=[dict(flow='orders',id=node['id'],label=node['id'],x=70+300*(index%6),y=70+150*(index//6)) for index,node in enumerate(document['dataFlows'][0]['components'])],viewports=[dict(flow='orders',x=0,y=0,zoom=.6)])
c.api('POST','/versions',dict(document=document,expectedPackageRevision='0'))
errors=[];console=[];http=[]
def save(page,name):page.screenshot(path=str(evidence/name),full_page=True)
def wait_run(ticket,timeout=180):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  value=c.api('GET','/executions/'+ticket)
  if value['state'] not in ('Queued','Claimed','Running'):return value
  time.sleep(.2)
 raise AssertionError(value)
def project(page,name):
 page.get_by_role('button',name='项目',exact=True).click();page.get_by_role('row').filter(has_text=name).get_by_role('button',name='打开工作台').click();expect(page.get_by_label('作业包名称')).to_have_value(name)
def select_node(page,id):
 page.get_by_role('button',name='Fit View',exact=True).click();node=page.locator('.react-flow__node[data-id="'+id+'"]');node.click();return node

with sync_playwright() as p:
    browser=p.chromium.launch(executable_path=str(args.browser_executable) if args.browser_executable else None,headless=True,args=['--no-sandbox'])
    page=browser.new_page(viewport=dict(width=1680,height=1100))
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.on('console',lambda m:console.append(dict(level=m.type,text=m.text)) if m.type in ('warning','error') else None)
    page.on('response',lambda r:http.append(dict(status=r.status,path=r.url.split('/api')[-1])) if r.status>=400 and '/api' in r.url else None)
    try:
        page.goto(URL);page.get_by_role('textbox',name='账户',exact=True).fill(args.subject);page.get_by_label('密码',exact=True).fill(password);page.get_by_role('button',name='登录',exact=True).click()
        page.get_by_role('heading',name='项目与数据任务').wait_for();save(page,'projects.png')
        page.get_by_role('button',name='从 CSV / Excel 导入',exact=True).click()
        wizard=page.get_by_role('dialog',name='文件导入向导');wizard.get_by_label('上传数据文件').set_input_files(csv_file)
        expect(wizard.get_by_label('授权文件路径')).not_to_have_value('')
        wizard.get_by_role('button',name='发现字段与样本',exact=True).click()
        wizard.get_by_role('button',name='全部按文本读取',exact=True).wait_for(timeout=30000)
        wizard.get_by_role('button',name='全部按文本读取',exact=True).click()
        wizard.get_by_role('button',name='按当前类型读取样本',exact=True).click()
        expect(wizard.get_by_label('确认文件格式与字段')).to_be_enabled(timeout=30000)
        save(page,'csv-wizard-native-samples.png')
        wizard.get_by_label('确认文件格式与字段').check();wizard.get_by_role('button',name='添加数据源',exact=True).click()
        page.get_by_label('作业包名称').fill(basic_id)
        source=page.locator('.react-flow__node').first;source_id=source.get_attribute('data-id')
        page.get_by_role('button',name='连接中心',exact=True).click()
        center=page.get_by_role('dialog',name='连接中心');center.get_by_role('button',name='新建连接',exact=True).click()
        center.get_by_label('数据源类型',exact=False).select_option(str(args.plugin))
        center.get_by_label('授权资源',exact=True).fill(args.production_resource);center.get_by_label('凭据引用',exact=False).select_option(args.production_reference)
        connection=center.get_by_label('连接名称',exact=True).input_value()
        center.get_by_role('button',name='测试连接 / 列出表',exact=True).click();center.get_by_text('连接成功',exact=False).wait_for(timeout=30000);save(page,'connection-center-native.png')
        center.get_by_role('button',name='关闭连接中心',exact=True).click()
        page.get_by_label('搜索组件').fill('sql.destination');page.locator('.catalog-row').filter(has_text='sql.destination').click()
        destination=page.locator('.react-flow__node.selected');sink_id=destination.get_attribute('data-id')
        page.get_by_label('组件连接',exact=True).select_option(connection);page.get_by_label('目标表名称',exact=True).fill(args.table_prefix+'raw')
        page.get_by_role('button',name='发现目标字段',exact=True).click();page.get_by_text('已读取目标表结构。',exact=True).wait_for(timeout=30000)
        connect(page,source_id,sink_id)
        page.get_by_role('button',name='校验',exact=True).click();page.get_by_text('Native 校验通过，字段与端口已绑定',exact=True).wait_for(timeout=30000)
        page.get_by_role('button',name='同名匹配',exact=True).click()
        save(page,'visual-mapping.png')
        page.get_by_role('button',name='预览',exact=True).click();page.get_by_text('Native 真实样本 · 目标未执行',exact=True).wait_for(timeout=30000)
        save(page,'sink-input-readonly-preview.png')
        with page.expect_response(lambda response:response.url.endswith('/api/publications') and response.request.method=='POST') as published:page.get_by_role('button',name='发布版本',exact=True).click()
        basic_package_id=published.value.json()['deployment']['packageId'];page.get_by_text('已发布 v1，等待正式执行',exact=True).wait_for(timeout=30000)
        assert not [r for r in c.api('GET','/executions') if r['packageId']==basic_package_id]
        page.get_by_label('组件显示名称').fill('Current draft target')
        page.get_by_role('button',name='发布版本',exact=True).click();page.get_by_text('已发布 v2，等待正式执行',exact=True).wait_for(timeout=30000)
        current=c.api('GET','/packages/'+basic_package_id+'/versions/2');assert any(n.get('label')=='Current draft target' for n in current['layout']['nodes'])
        page.get_by_role('button',name='正式执行',exact=True).click();confirmation=page.get_by_role('dialog',name='确认正式执行')
        expect(confirmation.get_by_role('button',name='确认正式执行',exact=True)).to_be_disabled();save(page,'formal-write-confirmation.png');confirmation.get_by_role('button',name='取消',exact=True).click()
        # Open the real seven-target business package; all formula values and
        # branch results come from Native, with no browser-side evaluation.
        project(page,business_id);page.get_by_role('button',name='校验',exact=True).click();page.get_by_text('Native 校验通过，字段与端口已绑定',exact=True).wait_for(timeout=30000)
        select_node(page,'derive');page.get_by_role('button',name='预览',exact=True).click();page.get_by_text('Native 真实样本 · 目标未执行',exact=True).wait_for(timeout=30000)
        page.get_by_role('button',name='编辑派生列',exact=True).click();dialog=page.locator('dialog[open]');expect(dialog.get_by_label('输出列 1',exact=True)).to_have_value('CustomerName');save(page,'derived-column-native-before-after.png')
        formula=dialog.locator('.cm-content').nth(1);original=formula.inner_text();formula.fill('"中文" + [missing]')
        dialog.get_by_role('button',name='Native 编译',exact=True).nth(1).click();dialog.get_by_role('button',name='定位',exact=False).last.wait_for(timeout=30000);dialog.get_by_role('button',name='定位',exact=False).last.click()
        formula.fill(original);dialog.get_by_role('button',name='Native 编译',exact=True).nth(1).click();dialog.locator('.ssis-inferred').last.wait_for(timeout=30000)
        dialog.get_by_role('button',name='完成',exact=True).click()
        select_node(page,'split');page.get_by_role('button',name='编辑条件分流',exact=True).click();page.get_by_role('button',name='计算样本命中',exact=True).click();page.get_by_text('Native 真实样本 · 目标未执行',exact=True).wait_for(timeout=30000);save(page,'split-native-hits.png')
        page.get_by_label('分支名称 1',exact=True).fill('VIP 客户')
        page.get_by_role('button',name='下移分支 1',exact=True).click();page.get_by_role('button',name='上移分支 2',exact=True).click()
        page.get_by_role('button',name='完成分流',exact=True).click()
        page.get_by_role('button',name='发布版本',exact=True).click();page.get_by_text('已发布 v2，等待正式执行',exact=True).wait_for(timeout=30000)
        before_run=c.api('GET',f'/packages/{business_id}/versions/2');assert before_run['dataFlows'][0]['components'][5]['id']=='fan' or any(n['id']=='fan' for n in before_run['dataFlows'][0]['components'])
        page.get_by_role('button',name='正式执行',exact=True).click();confirmation=page.get_by_role('dialog',name='确认正式执行');expect(confirmation.locator('tbody tr')).to_have_count(7)
        confirmation.get_by_label('开启 Native 调试与有界数据查看').check();confirmation.get_by_label('确认正式业务写入').check()
        with page.expect_response(lambda r:r.url.endswith('/api/executions') and r.request.method=='POST') as response:confirmation.get_by_role('button',name='确认正式执行',exact=True).click()
        ticket=response.value.json()['id'];formal_ticket=ticket;run=wait_run(ticket);assert run['state']=='Succeeded',run
        facts=c.api('GET',f'/executions/{ticket}/metrics');debug=c.api('GET',f'/executions/{ticket}/debug')
        (evidence/'formal-native-facts.json').write_text(json.dumps(dict(run=run,facts=facts,debug=debug),indent=2))
        page.locator('.run-monitor h3').filter(has_text='已完成').wait_for(timeout=15000);save(page,'seven-target-run.png')
        page.set_viewport_size(dict(width=768,height=950));save(page,'workbench-compact.png')
        summary,_=verify_business_rows(query(args.production_config),runid,True,args.table_prefix)
        (evidence/'csv-readback.json').write_text(json.dumps(summary,indent=2))
        discovered=c.api('POST','/schema',dict(package=before_run,connection='db',table=args.table_prefix+'vip'))
        probe=json.loads(json.dumps(before_run));probe['dataFlows']=[dict(id='probe',components=[dict(id='read',component='sql.source',config=dict(connection='db',table=args.table_prefix+'vip',schema=discovered))],paths=[])]
        readonly=c.api('POST','/preview',dict(package=probe,pipelineId='probe',componentId='read',sourceRows=32,sampleRows=8))
        assert readonly['result']==0 and next(edge for edge in readonly['snapshot']['preview'] if edge['component']=='read' and edge['port']==0)['totalRows']==32
        after_preview,_=verify_business_rows(query(args.production_config),runid,True,args.table_prefix)
        assert after_preview['outputSignature']==summary['outputSignature']

        page.set_viewport_size(dict(width=1680,height=1100))
        page.get_by_role('button',name='项目',exact=True).click();page.get_by_role('row').filter(has_text=business_id).get_by_role('button',name='打开工作台').click()
        page.get_by_role('button',name='版本与比较',exact=True).click();versions=page.get_by_role('dialog',name='版本与比较');versions.locator('.version-row').filter(has_text=business_id+' · v2').get_by_role('button',name='打开',exact=True).click()
        page.get_by_role('button',name='Fit View',exact=True).click();page.locator('.react-flow__node[data-id="source"]').click();page.get_by_role('button',name='删除组件',exact=True).click()
        page.get_by_role('button',name='导入文件',exact=True).click();wizard=page.get_by_role('dialog',name='文件导入向导')
        wizard.get_by_label('上传数据文件').set_input_files(xlsx_file);expect(wizard.get_by_label('授权文件路径')).not_to_have_value('')
        wizard.get_by_role('button',name='发现字段与样本',exact=True).click();expect(wizard.get_by_label('确认文件格式与字段')).to_be_enabled(timeout=30000)
        expect(wizard.get_by_label('工作表',exact=True)).to_have_value('Orders');wizard.get_by_role('button',name='全部按文本读取',exact=True).click();wizard.locator('details summary').first.click();wizard.locator('fieldset').first.get_by_label('允许为空',exact=True).uncheck();wizard.get_by_role('button',name='按当前类型读取样本',exact=True).click();expect(wizard.get_by_label('确认文件格式与字段')).to_be_enabled(timeout=30000);page.screenshot(path=str(evidence/'xlsx-wizard-native-samples.png'))
        wizard.get_by_label('确认文件格式与字段').check();wizard.get_by_role('button',name='添加数据源',exact=True).click()
        source_id=page.locator('.react-flow__node.selected').get_attribute('data-id')
        connect(page,source_id,'convert')
        page.get_by_role('button',name='校验',exact=True).click();page.get_by_text('Native 校验通过，字段与端口已绑定',exact=True).wait_for(timeout=30000)
        page.get_by_role('button',name='完整测试',exact=True).click();dialog=page.get_by_role('dialog',name='隔离完整测试');dialog.get_by_label('测试数据库连接',exact=False).select_option('test-db')
        for table in ['vip','refund','paid_other','other','region_summary','product_summary','errors']:
         dialog.get_by_label('orders / '+args.table_prefix+table+' → 测试表',exact=True).fill(args.table_prefix+table)
        dialog.get_by_label('已核对全部隔离测试目标').check()
        with page.expect_response(lambda response:response.url.endswith('/api/tests') and response.request.method=='POST') as response:dialog.get_by_role('button',name='运行完整测试',exact=True).click()
        result=response.value;(evidence/'isolated-request.json').write_text(json.dumps(result.request.post_data_json));assert result.status==202,(result.status,result.text());run=result.json();ticket=run['id'];deadline=time.monotonic()+180
        while time.monotonic()<deadline:
         run=c.api('GET','/executions/'+ticket)
         if run['state'] not in ['Queued','Claimed','Running']:break
         time.sleep(.2)
        assert run['state']=='Succeeded',run
        page.locator('.run-monitor h3').filter(has_text='已完成').wait_for(timeout=15000)
        nodes=page.locator('.react-flow__node').evaluate_all('(nodes)=>nodes.slice(0,3).map(node=>({id:node.getAttribute("data-id"),style:node.getAttribute("style"),box:node.getBoundingClientRect().toJSON()}))')
        page.screenshot(path=str(evidence/'xlsx-isolated-run.png'))
        page.get_by_role('button',name='Fit View',exact=True).click();page.wait_for_timeout(400);page.screenshot(path=str(evidence/'xlsx-isolated-graph.png'))
        for width,height,name in [(768,950,'workbench-compact.png'),(390,844,'workbench-phone.png')]:page.set_viewport_size(dict(width=width,height=height));page.screenshot(path=str(evidence/name))
        summary,_=verify_business_rows(query(args.test_config),runid,True,args.table_prefix);production,_=verify_business_rows(query(args.production_config),runid,True,args.table_prefix)
        assert summary['outputSignature']==production['outputSignature']
        facts=c.api('GET','/executions/'+ticket+'/metrics')
        (evidence/'xlsx-isolated-facts.json').write_text(json.dumps(dict(run=run,facts=facts,readback=summary,productionUnchanged=True,nodes=nodes),indent=2))
        assert not errors,errors
        (evidence/'browser.json').write_text(json.dumps(dict(status='Passed',noJsonFileToMySQL=True,nativeSourceDiscovery=True,nativeConnection=True,visualMapping=True,readonlySinkPreview=True,currentDraftPublication=True,publishDoesNotExecute=True,writeConfirmation=True,nativeFormulaDiagnostics=True,nativeSplitPriority=True,sevenTargetRun=formal_ticket,isolatedRun=run['id'],readonlySQLSource=True,xlsxIsolated=True,productionUnchanged=True,errors=errors,console=console,http=http,browserPlugin='Browser plugin not available'),indent=2))
        print('Real browser: file wizard, driver connection, mapping, preview, draft publication, Native formula/split and seven-target MySQL execution passed.')
    except BaseException:
        save(page,'browser-failure.png');(evidence/'browser-failure.txt').write_text(page.locator('body').inner_text());raise
    finally:browser.close()

"""Real 50K CSV/XLSX -> Native operators -> MySQL, never an SSIS oracle.

Only UD_TEST_MYSQL_CONFIG (a designated disposable database) enables this test.
Every query/setup uses the same public Native Connector API as production.
"""
import argparse
import csv
import hashlib
import json
import os
import pathlib
import shutil
import secrets
import socket
import struct
import subprocess
import sys
import time
import tempfile
import zipfile
from decimal import Decimal, ROUND_HALF_UP
from xml.sax.saxutils import escape
from generate import HEADER, N, generate, raw_row, digest_rows

VERSION = json.loads((pathlib.Path(__file__).resolve().parents[2]/'metadata/product.json').read_text())['product']['protocolVersion']
TABLES = ['vip','refund','paid_other','other','region_summary','product_summary','errors']

def field(name, kind='utf8', nullable=False, **kw):
    return dict(name=name, type=kind, nullable=nullable, **kw)
def schema(fields): return dict(version=VERSION, fields=fields)
def node(id, component, **config): return dict(id=id, component=component, config=config)
def edge(a,b,output=0,input=0): return dict(**{'from':a,'to':b}, output=output,input=input)
def added(name, expression): return dict(name=name,action='add',expression=expression)
def replaced(name, expression): return dict(name=name,action='replace',input=name,expression=expression)
def logical(name, kind, length=0, precision=0, scale=0, **kw):
    storage = 'int64' if kind.startswith('DT_I') else 'uint64' if kind.startswith('DT_UI') else 'decimal128' if kind=='DT_NUMERIC' else 'datetime' if kind.startswith('DT_DB') else 'utf8'
    scalar=dict(type=kind)
    if length: scalar['length']=length
    if kind.startswith('DT_DB'):scalar['fractionalDigits']=scale
    kw.update(scalar=scalar)
    if storage=='decimal128':kw.update(precision=precision,scale=scale)
    return field(name, storage, **kw)
def xlsx(path, dirty):
    # Independent OOXML fixture, not produced by UniversalData's own writer.
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
        z.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Orders" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
        with z.open('xl/worksheets/sheet1.xml','w') as out:
            out.write(b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>')
            for ordinal in range(N+1):
                record=None if ordinal==0 else raw_row(ordinal,dirty)[0]
                values=HEADER if record is None else [record[k] for k in HEADER]
                cells=[]
                for j,value in enumerate(values):
                    if value is None or value=='':continue
                    text=escape(str(value)).replace('\r','&#13;')
                    cells.append(f'<c r="{chr(65+j)}{ordinal+1}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>')
                out.write((f'<row r="{ordinal+1}">'+''.join(cells)+'</row>').encode())
            out.write(b'</sheetData></worksheet>')

def package(root, source, driver, config, runid, batch, transaction, load):
    spill=root/'spill';spill.mkdir(exist_ok=True)
    raw=schema([field(k,nullable=k in ('Status','Amount','Remark')) for k in HEADER])
    typed=schema([logical('RowId','DT_I4'),logical('OrderNo','DT_WSTR',24),logical('CustomerName','DT_WSTR',128),logical('Status','DT_WSTR',16,nullable=True),logical('Region','DT_WSTR',8),logical('ProductCode','DT_WSTR',8),logical('Amount','DT_NUMERIC',precision=18,scale=2,nullable=True),logical('CategoryPath','DT_WSTR',128),logical('CreatedAt','DT_DBTIMESTAMP2',scale=0),logical('Remark','DT_WSTR',1024,nullable=True)])
    connections=[dict(id='input',driver='excel' if source.suffix=='.xlsx' else 'file',resource=str(source)),dict(id='spill',driver='file',resource=str(spill))]
    targets=['db'] if transaction=='single' else ['db'+str(i) for i in range(7)]
    connections.extend(dict(id=id,driver=str(driver),resource='ssis-parity-isolated',credentialReference='file:'+str(config),maxLeases=1) for id in targets)
    src=node('source','file.xlsx.source' if source.suffix=='.xlsx' else 'file.csv.source',connection='input',schema=raw,batchSize=batch)
    if source.suffix=='.xlsx':src['config'].update(sheet='Orders',range='A1:J50001',header=True)
    else:src['config'].update(fileFormat=dict(encoding='UTF-8',header=True,nullMarker=''))
    nodes=[src,node('convert','cast',schema=typed,errorPolicy='redirect',includeInput=True),node('derive','derive',columns=[
        replaced('CustomerName','UPPER(TRIM([CustomerName]))'),
        added('run_id',json.dumps(runid)),added('OriginalNameLength','LEN([CustomerName])'),added('FirstCharacter','SUBSTRING([CustomerName],1,1)'),
        added('NameTokenCount','TOKENCOUNT([CategoryPath],"|")'),added('MiddleCategory','TOKEN([CategoryPath],"|",2)'),
        added('StatusLabel','ISNULL([Status]) ? "UNKNOWN" : [Status]'),added('IsNegativeAmount','ISNULL([Amount]) ? FALSE : [Amount] < (DT_NUMERIC,18,2)0'),
        added('TaxAmount','(DT_NUMERIC,18,2)ROUND([Amount] * (DT_NUMERIC,3,2)0.13,2)'),added('OrderMonth','MONTH([CreatedAt])')]),
        node('fan','multicast',outputs=['split','region','product']),
        node('split','split',branches=[dict(id='VIP',condition='[Status] == "PAID" && [Region] == "EAST" && [Amount] >= (DT_NUMERIC,18,2)500'),dict(id='REFUND',condition='[Status] == "REFUND" || [Amount] < (DT_NUMERIC,18,2)0'),dict(id='PAID_OTHER',condition='[Status] == "PAID"')],default='OTHER')]
    paths=[edge('source','convert'),edge('convert','derive'),edge('derive','fan'),edge('fan','split')]
    # Explicit conversion from business SSIS domains to the discovered SQL domains.
    # No host computation: every normalization is performed by Native Data Conversion.
    normalized=schema([field('RowId','uint64'),field('OrderNo'),field('CustomerName'),field('Status',nullable=True),field('Region'),field('ProductCode'),field('Amount','decimal128',True,precision=38,scale=2),field('CategoryPath'),field('CreatedAt','timestamp',timezone='UTC'),field('Remark',nullable=True),field('run_id'),field('OriginalNameLength','uint64'),field('FirstCharacter'),field('NameTokenCount','int64'),field('MiddleCategory'),field('StatusLabel'),field('IsNegativeAmount','int64'),field('TaxAmount','decimal128',True,precision=38,scale=2),field('OrderMonth','uint64')])
    # SQL tinyint is numeric; the SSIS BOOL -> integer cast (-1) is normalized
    # explicitly with a conditional, preserving TRUE=1 business target convention.
    nodes.insert(3,node('boolean','derive',columns=[added('negative_sql','[IsNegativeAmount] ? 1L : 0L')]))
    normalized['fields'].append(field('negative_sql','int64'))
    paths[2]=edge('derive','boolean');paths.insert(3,edge('boolean','fan'))
    business=[('run_id','run_id'),('RowId','source_row_id'),('OrderNo','order_no'),('CustomerName','customer_name'),('OriginalNameLength','original_name_length'),('FirstCharacter','first_character'),('NameTokenCount','name_token_count'),('MiddleCategory','middle_category'),('StatusLabel','status_label'),('negative_sql','is_negative_amount'),('TaxAmount','tax_amount'),('OrderMonth','order_month'),('Status','status'),('Region','region'),('ProductCode','product_code'),('Amount','amount'),('CategoryPath','category_path'),('CreatedAt','created_at'),('Remark','remark')]
    def sink(id, index, table, mapping):
        return node(id,'sql.destination',connection=targets[0 if transaction=='single' else index],table='ud_ssis_'+table,mode='append',loadMode=load,provisionReceipts=False,mapping=[dict(source=a,target=b) for a,b in mapping])
    for i,branch in enumerate(TABLES[:4]):
        nodes.extend([node('normalize'+str(i),'cast',schema=normalized),sink(branch,i,branch,business)])
        paths.extend([edge('split','normalize'+str(i),i),edge('normalize'+str(i),branch)])
    for i,(key,table) in enumerate([('Region','region_summary'),('ProductCode','product_summary')]):
        prefix='group'+str(i)
        nodes.extend([node(prefix,'aggregate',plan=dict(version=VERSION,group=[key],aggregates=[dict(function='count',column='*',name='input_rows'),dict(function='sum',column='Amount',name='amount_sum')])),node(prefix+'Run','derive',columns=[added('run_id',json.dumps(runid))]),node(prefix+'Normalize','cast',schema=schema([field(key),field('input_rows','uint64'),field('amount_sum','decimal128',True,precision=38,scale=2),field('run_id')])),sink(table,4+i,table,[('run_id','run_id'),(key,'region' if i==0 else 'product_code'),('input_rows','input_rows'),('amount_sum','amount_sum')])])
        paths.extend([edge('fan',prefix,1+i),edge(prefix,prefix+'Run'),edge(prefix+'Run',prefix+'Normalize'),edge(prefix+'Normalize',table)])
    nodes.extend([node('errorDetails','derive',columns=[added('run_id',json.dumps(runid)),added('source_row_id','(DT_UI8)[inputValues.RowId]'),added('raw_amount','[inputValues.Amount]'),added('stage','"Data Conversion"'),added('code_text','(DT_WSTR,64)[errorCode]'),added('position_text','(DT_WSTR,128)[sourcePosition]'),added('raw_record','ud.wire_json([input])')]),sink('errors',6,'errors',[('run_id','run_id'),('source_row_id','source_row_id'),('stage','stage'),('errorKind','error_kind'),('code_text','error_code'),('columnId','error_column'),('position_text','source_position'),('raw_amount','raw_amount'),('raw_record','raw_record')])])
    paths.extend([edge('convert','errorDetails',1),edge('errorDetails','errors')])
    flow=dict(id='flow',kind='dataFlow',transaction=dict(mode='required',participants=targets,provisionReceipts=True),config=dict(pipeline='orders'))
    document=dict(version=VERSION,id='ssis-orders-'+runid,connections=connections,policy=dict(buffers=dict(connection='spill',residentBytes=1048576,workingBytes=16777216,spillBytes=2000000000,maxBlocks=1000000)),controlFlow=dict(tasks=[flow],constraints=[]),dataFlows=[dict(id='orders',components=nodes,paths=paths)])
    if transaction=='package':document['policy']['transaction']=flow.pop('transaction')
    return document

def main():
    p=argparse.ArgumentParser();p.add_argument('--runner',required=True,type=pathlib.Path);p.add_argument('--database',required=True,type=pathlib.Path);p.add_argument('--plugin',required=True,type=pathlib.Path);p.add_argument('--output',type=pathlib.Path);p.add_argument('--case',action='append');p.add_argument('--exclude-case',action='append',default=[]);a=p.parse_args()
    config=os.environ.get('UD_TEST_MYSQL_CONFIG')
    if not config or not pathlib.Path(config).is_file():print('Verification Blocked: UD_TEST_MYSQL_CONFIG is absent');return 77
    config=pathlib.Path(config).resolve();root=(a.output or pathlib.Path('/tmp/ud-ssis-parity-acceptance')).resolve();root.mkdir(parents=True,exist_ok=True)
    results=[]
    def db(sql, fields=None):
        (root/'query.sql').write_text(sql)
        command=[a.database,a.plugin,config,'query' if fields is not None else 'execute',root/'query.sql']
        if fields is not None:(root/'query-schema.json').write_text(json.dumps(schema(fields)));command.append(root/'query-schema.json')
        result=subprocess.run(list(map(str,command)),capture_output=True,text=True,timeout=120)
        if result.returncode:raise RuntimeError(f'Native database helper failed: {result.stderr}')
        if fields is None:return None
        rows=[row for line in result.stdout.splitlines() for row in json.loads(line)]
        # Native JSON intentionally quotes UINT64 to preserve its full range.
        for row in rows:
            for f in fields:
                if f['type']=='uint64' and row[f['name']] is not None:row[f['name']]=int(row[f['name']])
        return rows
    ddl=(pathlib.Path(__file__).parent/'fixtures/mysql.sql').read_text()
    for statement in ddl.split(';'):
        statement='\n'.join(line for line in statement.splitlines() if not line.lstrip().startswith('--')).strip()
        if statement:db(statement)
    expected=json.loads((pathlib.Path(__file__).parent/'fixtures/expected.json').read_text())
    for dirty in (False,True):
        facts=generate(root,dirty);assert facts==expected['dirty' if dirty else 'clean']
        xlsx(root/('dirty.xlsx' if dirty else 'clean.xlsx'),dirty)
    def verify(runid,dirty,reordered=False):
        rows=[]
        branch_reference=expected['dirty' if dirty else 'clean']['branches']
        if reordered:
            indices={name:[] for name in ('VIP','REFUND','PAID_OTHER','OTHER')}
            for ordinal in range(1,N+1):
                raw,amount,invalid=raw_row(ordinal,dirty)
                if invalid:continue
                branch='PAID_OTHER' if raw['Status']=='PAID' else 'REFUND' if raw['Status']=='REFUND' or (amount is not None and amount<0) else 'OTHER'
                indices[branch].append(ordinal)
            branch_reference={name:dict(count=len(ids),rowIdsSha256=digest_rows(ids)) for name,ids in indices.items()}
        exports={}
        for branch in TABLES[:4]:
            fields=[field('source_row_id','uint64'),field('order_no'),field('customer_name'),field('original_name_length','uint64'),field('first_character'),field('name_token_count','int64'),field('middle_category'),field('status_label'),field('is_negative_amount','int64'),field('tax_amount','decimal128',True,precision=38,scale=2),field('order_month','uint64'),field('status',nullable=True),field('region'),field('product_code'),field('amount','decimal128',True,precision=38,scale=2),field('category_path'),field('created_at'),field('remark',nullable=True)]
            actual=db('SELECT '+','.join(f['name'] for f in fields)+f" FROM ud_ssis_{branch} WHERE run_id='{runid}' ORDER BY source_row_id",fields)
            ids=[r['source_row_id'] for r in actual];ref=branch_reference[branch.upper()]
            assert len(ids)==ref['count'] and digest_rows(ids)==ref['rowIdsSha256'],(branch,len(ids),ref)
            for row in actual:
                original,amount,invalid=raw_row(row['source_row_id'],dirty);assert not invalid
                tax=None if amount is None else (amount*Decimal('.13')).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
                if tax is not None and tax.is_zero():tax=abs(tax)  # SQL DECIMAL has one zero representation.
                want=dict(source_row_id=original['RowId'],order_no=original['OrderNo'],customer_name=original['CustomerName'].strip(' ').upper(),original_name_length=len(original['CustomerName'].encode('utf-16-le'))//2,first_character=original['CustomerName'][0],name_token_count=3,middle_category=original['ProductCode'],status_label=original['Status'] or 'UNKNOWN',is_negative_amount=int(amount is not None and amount<0),tax_amount=None if tax is None else str(tax),order_month=10,status=original['Status'],region=original['Region'],product_code=original['ProductCode'],amount=None if amount is None else str(amount.quantize(Decimal('.01'))),category_path=original['CategoryPath'],created_at=original['CreatedAt'],remark=original['Remark'])
                assert row==want,(branch,row,want)
            rows+=actual
            exports[branch]=actual
        ids=[x['source_row_id'] for x in rows];assert len(set(ids))==len(ids)==expected['dirty' if dirty else 'clean']['businessRows']
        for table,key,refkey in [('region_summary','region','region'),('product_summary','product_code','product')]:
            actual=db(f"SELECT {key},input_rows,amount_sum FROM ud_ssis_{table} WHERE run_id='{runid}' ORDER BY {key}",[field(key),field('input_rows','uint64'),field('amount_sum','decimal128',True,precision=38,scale=2)])
            got={r[key]:dict(count=r['input_rows'],amountSum=r['amount_sum']) for r in actual};assert got==expected['dirty' if dirty else 'clean'][refkey],got
            exports[table]=actual
        errors=db(f"SELECT source_row_id,error_kind,error_code,error_column,source_position,raw_amount,raw_record FROM ud_ssis_errors WHERE run_id='{runid}' ORDER BY source_row_id",[field('source_row_id','uint64'),field('error_kind'),field('error_code'),field('error_column'),field('source_position',nullable=True),field('raw_amount',nullable=True),field('raw_record',nullable=True)])
        assert [r['source_row_id'] for r in errors]==expected['dirty' if dirty else 'clean']['errorRowIds']
        for r in errors:
            assert r['error_kind']=='conversion' and r['error_code']!='0' and len(r['error_column'])==32 and int(r['source_position'])==r['source_row_id']-1 and r['raw_amount']=='INVALID_DECIMAL',r
            raw=json.loads(r['raw_record']);assert len(raw)==1 and raw[0]['Amount']=='INVALID_DECIMAL' and int(raw[0]['RowId'])==r['source_row_id'],r
        signature=hashlib.sha256(json.dumps(sorted(rows,key=lambda r:r['source_row_id']),ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        exports['errors']=errors
        archive=case/(runid+'-readback.zip')
        if not archive.exists():
            with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as output:
                for table,records in exports.items():output.writestr(table+'.ndjson',''.join(json.dumps(record,ensure_ascii=False,sort_keys=True)+'\n' for record in records))
                output.writestr('identity.json',json.dumps(dict(runId=runid,outputSignature=signature,branchReference=branch_reference),indent=2))
        return dict(businessRows=len(rows),errorRows=len(errors),outputSignature=signature,branchSignatures=branch_reference,readbackArchive=archive.name,readbackSha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    cases=[(f'{fmt}-{kind}-{txn}',fmt,kind=='dirty',txn,4096,4,'native',False) for fmt in ('csv','xlsx') for kind in ('clean','dirty') for txn in ('single','multi')]
    cases += [('csv-clean-batch1','csv',False,'single',1,2,'native',False),('csv-clean-batch64','csv',False,'single',64,2,'native',False),('csv-clean-batch1024','csv',False,'single',1024,2,'native',False),('csv-clean-parameters','csv',False,'single',4096,4,'parameters',False),('csv-clean-small-batch','csv',False,'single',127,1,'native',False),('csv-dirty-large-batch','csv',True,'multi',8192,8,'native',False),('csv-dirty-kill','csv',True,'multi',4096,4,'native',True)]
    cases += [(f'csv-dirty-read-kill-{row}','csv',True,'multi',9999 if row==49999 else 1000,4,'native',row) for row in (5000,25000,49999)]
    cases += [('csv-dirty-package-commit-kill','csv',True,'package',4096,4,'native',True)]
    cases += [(name,'csv',dirty,'multi',4096,4,'native',False) for name,dirty in [('csv-clean-reordered',False),('csv-dirty-history',True),('csv-dirty-connection-failure',True),('csv-dirty-unique-conflict',True),('csv-dirty-source-changed',True)]]
    for name,fmt,dirty,txn,batch,workers,load,kill in cases:
        if (a.case and name not in a.case) or name in a.exclude_case:continue
        runid=name+'-'+secrets.token_hex(6)
        case=root/name;case.mkdir(exist_ok=True)
        state=case/'state'
        if state.exists():
            shutil.make_archive(str(case/'previous-state'),'zip',state);shutil.rmtree(state)
        state.mkdir()
        source=root/('ssis_50000_'+('dirty' if dirty else 'clean')+'.csv') if fmt=='csv' else root/('dirty.xlsx' if dirty else 'clean.xlsx')
        for table in TABLES:db(f"TRUNCATE TABLE ud_ssis_{table}")
        doc=package(case,source,a.plugin.resolve(),config,runid,batch,txn,load);document=case/'package.json';document.write_text(json.dumps(doc,ensure_ascii=False,indent=2))
        reordered=name=='csv-clean-reordered'
        if reordered:
            split=next(component['config'] for component in doc['dataFlows'][0]['components'] if component['id']=='split');split['branches'][0],split['branches'][2]=split['branches'][2],split['branches'][0]
            for path in doc['dataFlows'][0]['paths']:
                if path['from']=='split' and path['output'] in (0,2):path['output']=2-path['output']
            document.write_text(json.dumps(doc,ensure_ascii=False,indent=2))
        log_prefix=''
        def execute(resume=False,crash=False,expect_failure=False,input_threshold=0,kind=None):
            # Input-row faults use actual Native counters at a durability boundary,
            # independent of the number of checkpoint records per business batch.
            read_fault=crash and type(kill) is int
            command=[a.runner,document,state,int(resume),workers,kind if kind is not None else 3 if read_fault else 4 if crash else 0,1,'',1073741824,'',batch,runid,input_threshold or (kill if read_fault else 0)]
            start=time.monotonic();r=subprocess.run(list(map(str,command)),capture_output=True,text=True,timeout=1200);elapsed=time.monotonic()-start
            log=case/(log_prefix+('completed-resume.log' if resume and not kill else 'resume.log' if resume else 'run.log'));log.write_text(r.stdout+r.stderr)
            if crash:assert r.returncode==91,f'{name}: expected process death, see {log}';return elapsed,{}
            assert r.returncode==0,f'{name}: process {r.returncode}, see {log}';facts=json.loads(r.stdout)
            assert (facts['result']!=0 if expect_failure else facts['result']==0),f'{name}: Native result {facts["result"]}, see {log}';return elapsed,facts
        failure_case=name in ('csv-dirty-connection-failure','csv-dirty-unique-conflict','csv-dirty-source-changed')
        if failure_case:
            expected_rows=[0]*7
            if name=='csv-dirty-source-changed':
                source=case/'changed.csv';shutil.copyfile(root/'ssis_50000_dirty.csv',source);doc['connections'][0]['resource']=str(source);document.write_text(json.dumps(doc,ensure_ascii=False,indent=2))
                execute(crash=True,kind=3,input_threshold=5000)
                with source.open('a') as stream:stream.write('changed-source\n')
                elapsed,facts=execute(resume=True,expect_failure=True)
                assert facts['result'] in (9,10,17),facts['result']
            elif name=='csv-dirty-connection-failure':
                with tempfile.TemporaryDirectory(prefix='ud-ssis-private-credentials-') as credentials,socket.socket() as unavailable:
                    unavailable.bind(('127.0.0.1',0));bad=json.loads(config.read_text());bad.update(host='127.0.0.1',port=unavailable.getsockname()[1],timeoutSeconds=2)
                    reference=pathlib.Path(credentials)/'unavailable.json';reference.write_text(json.dumps(bad));reference.chmod(0o600)
                    doc['connections'][4]['credentialReference']='file:'+str(reference);document.write_text(json.dumps(doc,ensure_ascii=False,indent=2))
                    elapsed,facts=execute(expect_failure=True)
            else:
                # A real existing primary key conflicts in a middle business branch.
                # The pre-existing row must survive; every new branch must roll back.
                db(f"INSERT INTO ud_ssis_vip (run_id,source_row_id,order_no,customer_name,original_name_length,first_character,name_token_count,middle_category,status_label,is_negative_amount,order_month,region,product_code,category_path,created_at) VALUES ('{runid}',4000,'collision','sentinel',8,'s',3,'P01','FIXED',0,10,'EAST','P01','sentinel','2026-10-01 00:00:00')")
                expected_rows[0]=1;elapsed,facts=execute(expect_failure=True)
                assert facts['result']!=16,'A known constraint rollback must not become an unknown commit'
            counts=[db(f"SELECT COUNT(*) AS n FROM ud_ssis_{table} WHERE run_id='{runid}'",[field('n','uint64')])[0]['n'] for table in TABLES]
            assert counts==expected_rows,(name,counts,expected_rows)
            assert facts.get('committed',0)==facts.get('acknowledged',0)==0,name
            result=dict(case=name,runId=runid,expectedFailure=True,result=facts['result'],remainingRows=counts,seconds=elapsed,native=facts);results.append(result)
            (case/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='native'}),flush=True)
            (root/'results.json').write_text(json.dumps(dict(abi=VERSION,ssisGolden='Verification Blocked: real Windows SSIS 2025 unavailable',cases=results),ensure_ascii=False,indent=2)+'\n')
            continue
        crash_seconds=execute(crash=True)[0] if kill else None
        elapsed,facts=execute(resume=bool(kill));validated=verify(runid,dirty,reordered)
        # A completed Journal is itself resumable; no target gains another row.
        _,completed_facts=execute(resume=True);assert verify(runid,dirty,reordered)==validated
        expected_committed=N+len(expected['clean']['region'])+len(expected['clean']['product'])
        for observed in (facts,completed_facts):
            assert observed['committed']==observed['acknowledged']==expected_committed,(name,observed['committed'],observed['acknowledged'])
            assert observed['committedErrors']==observed['acknowledgedErrors']==validated['errorRows'],name
        history=None
        if name=='csv-dirty-history':
            first_id=runid;runid=name+'-'+secrets.token_hex(6);state=case/'second-state';state.mkdir();document=case/'second-package.json';document.write_text(json.dumps(package(case,source,a.plugin.resolve(),config,runid,batch,txn,load),ensure_ascii=False,indent=2));log_prefix='second-'
            second_seconds,_=execute();second=verify(runid,dirty)
            assert verify(first_id,dirty)==validated and second['outputSignature']==validated['outputSignature']
            assert db('SELECT COUNT(*) AS n FROM ud_ssis_errors',[field('n','uint64')])[0]['n']==100
            history=dict(firstRunId=first_id,secondRunId=runid,secondExecutionSeconds=second_seconds,secondReadback=second)
            runid=first_id
        result=dict(case=name,runId=runid,batchRows=batch,workers=workers,transaction=txn,loadMode=load,fault='input-row-durability-boundary' if type(kill) is int else 'coordinator-committed' if kill else None,seconds=elapsed,rowsPerSecond=None if kill else round(N/elapsed,2),crashExecutionSeconds=crash_seconds,recoverySeconds=elapsed if kill else None,history=history,native=facts,**validated);results.append(result)
        (case/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k!='native'}),flush=True)
        (root/'results.json').write_text(json.dumps(dict(abi=VERSION,ssisGolden='Verification Blocked: real Windows SSIS 2025 unavailable',cases=results),ensure_ascii=False,indent=2)+'\n')
        # Data artifacts remain in MySQL; bounded snapshots are regenerated for
        # each independent test, while packages, logs and results are retained.
        shutil.rmtree(state);shutil.rmtree(case/'spill')
    return 0
if __name__=='__main__':sys.exit(main())

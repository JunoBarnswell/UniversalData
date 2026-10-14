"""Real error-path commits, new transforms, Window, CSV and shared spill."""
import base64, csv, json, pathlib, subprocess, sys, tempfile
with tempfile.TemporaryDirectory(prefix="ud-core-pipeline-") as folder:
    base=pathlib.Path(folder);root=base/'resources';root.mkdir()
    spill=root/"spill";spill.mkdir()
    source=root/"source.ndjson"
    source.write_text('\n'.join(json.dumps(r) for r in [dict(x=3,t=" a "),dict(x=None,t=" b "),dict(x=1,t=" c "),dict(x=2,t=" d ")])+"\n")
    package={"version":11,"id":"core-pipeline","variables":{"rows":{"type":"int64","default":0}},
        "policy":{"buffers":{"connection":"spill","residentBytes":1,"spillBytes":10000000,"maxBlocks":256}},
        "connections":[dict(id=id,driver="file",resource=str(path)) for id,path in [("in",source),("ok",root/"ok.csv"),("bad",root/"bad.ndjson"),("spill",spill)]],
        "controlFlow":{"tasks":[dict(id="flow",kind="dataFlow",config=dict(pipeline="p"))],"constraints":[]},
        "dataFlows":[{"id":"p","components":[
            dict(id="source",component="file.ndjson.source",config=dict(connection="in",schema={"version":11,"fields":[dict(name="x",type="int64",nullable=True),dict(name="t",type="utf8",nullable=False)]},batchSize=1)),
            dict(id="quality",component="quality",config={"errorPolicy":"redirect","includeInput":True,"rules":[dict(id="required",kind="notNull",columns=["x"])]}),
            dict(id="chars",component="characterMap",config={"columns":[dict(column="t",function="trim")]}),
            dict(id="copy",component="copyColumn",config={"columns":[dict(column="t",name="copied")]}),
            dict(id="window",component="window",config=dict(keys=[0],partitionKeys=0,function="runningSum",column=0,name="total")),
            dict(id="count",component="rowCount",config=dict(variable="rows")),
            dict(id="audit",component="audit",config={}),
            dict(id="ok",component="file.csv.destination",config=dict(connection="ok")),
            dict(id="bad",component="file.ndjson.destination",config=dict(connection="bad"))],
            "paths":[dict(**{"from":a,"output":port,"to":b,"input":0}) for a,port,b in [("source",0,"quality"),("quality",0,"chars"),("quality",1,"bad"),("chars",0,"copy"),("copy",0,"window"),("window",0,"count"),("count",0,"audit"),("audit",0,"ok")]]}]}
    path=root/"package.json";path.write_text(json.dumps(package))
    policy=base/"host-policy.json";policy.write_text(json.dumps(dict(dataDirectory=str(base/"native-state"),fileRoots=[str(root)],networkOrigins=[],pluginRoots=[],credentialFiles={})))
    result=subprocess.run([sys.argv[1],"execute",str(policy),str(path),"{}","core"],capture_output=True,text=True,timeout=60)
    assert result.returncode==0,(result.stdout,result.stderr)
    metrics=json.loads(result.stdout);assert metrics["state"]==5 and metrics["result"]==0 and metrics["spillBytes"]>0
    with (root/"ok.csv").open(newline="") as file: rows=list(csv.DictReader(file))
    assert rows==[dict(x=str(i),t=t,copied=t,total=str(total)) for i,t,total in [(1,"c",1),(2,"d",3),(3,"a",6)]],rows
    errors=[json.loads(s) for s in (root/"bad.ndjson").read_text().splitlines()]
    assert len(errors)==1 and errors[0]["ruleId"]=="required" and len(errors[0]["sourceRowId"])==32 and errors[0]["ruleVersion"]==1
    # Inspect the error payload through the same Native Wire codec used by SDKs.
    wire=root/'error.udw';wire.write_bytes(base64.b64decode(errors[0]['input'],validate=True))
    decoded=subprocess.run([sys.argv[2],str(wire)],capture_output=True,text=True,timeout=10)
    assert decoded.returncode==0,decoded.stderr
    payload=json.loads(decoded.stdout)
    assert payload['rows']==[dict(x=None,t=' b ')]
    assert payload['rowInfo']['id']==errors[0]['sourceRowId']
    wire.unlink()
    assert not list(root.glob("*.udw"))
    print("Quality normal/error commits, Character Map/Copy/RowCount/Audit, Window and CSV with actual shared spill passed")

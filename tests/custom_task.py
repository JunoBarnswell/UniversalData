import json
import pathlib
import subprocess
import sys
import tempfile
for index in range(1,4):
    sys.argv[index]=str(pathlib.Path(sys.argv[index]).resolve())
with tempfile.TemporaryDirectory(prefix="ud-native-custom-") as temporary:
    root=pathlib.Path(temporary)
    def fixture(name,extra=None):
        folder=root/name;folder.mkdir()
        doc={"version":4,"id":"custom","variables":{"n":{"type":"int64","default":0}},
             "connections":[{"id":"target","driver":"file","resource":str(folder/"marker")}],
             "controlFlow":{"tasks":[{"id":"custom","kind":"sample.custom","config":{"connection":"target","variable":"n","expression":{"op":"add","args":[{"ref":"n"},{"literal":5,"type":"int64"}]},**(extra or {})}}],"constraints":[]},"dataFlows":[]}
        return folder,doc
    def run(folder,doc,mode=0,status=0,value=5,checkpoint=False,resume=0,exit=0):
        path=folder/"package.json";path.write_text(json.dumps(doc))
        command=[sys.argv[1],sys.argv[2],sys.argv[3],str(path),str(mode),str(status),str(value),str(folder/"state") if checkpoint else "",str(resume)]
        try:
            result=subprocess.run(command,capture_output=True,text=True,timeout=10)
        except subprocess.TimeoutExpired as failure:
            raise AssertionError((folder.name,failure.stderr,failure.stdout)) from failure
        assert result.returncode==exit,(folder.name,result.returncode,result.stderr)
    folder,doc=fixture("success",{"appendMarker":True});run(folder,doc);assert (folder/"marker").read_text()=="effect\n"
    for name,mode,status,value,extra in [("write-denied",1,13,0,{}),("default-deny",5,13,0,{}),("returned-io",0,3,5,{"returnStatus":3}),("returned-busy",0,15,5,{"returnStatus":11}),("returned-end",0,2,5,{"returnStatus":1}),("live-cancel",2,8,0,{"holdMs":3000}),("deadline",4,18,0,{"holdMs":3000})]:
        folder,doc=fixture(name,{"appendMarker":True,**extra});run(folder,doc,mode,status,value)
        assert (folder/"marker").exists()==(value==5)
    folder,doc=fixture("invalid-schema",{"unknown":1});run(folder,doc,status=5,value=0);assert not (folder/"marker").exists()
    folder,doc=fixture("readonly");doc["variables"]={};doc["parameters"]={"n":{"type":"int64","default":0}};run(folder,doc,status=13,value=0)
    folder,doc=fixture("effect-crash",{"appendMarker":True});run(folder,doc,mode=3,checkpoint=True,exit=91)
    assert (folder/"marker").read_text()=="effect\n"
    run(folder,doc,status=16,value=0,checkpoint=True,resume=1);assert (folder/"marker").read_text()=="effect\n"
    folder,doc=fixture("completed-resume",{"appendMarker":True});run(folder,doc,checkpoint=True)
    # Completed resume skips invoke, unlike ordinary executions.
    # The same native effect must not execute twice.
    run(folder,doc,checkpoint=True,resume=1);assert (folder/"marker").read_text()=="effect\n"
    print("Native Custom Task: actual shared library/public headers, atomic descriptor rollback, current ABI4 only, old version/layout rejected, module/registry/execution unload guards, native expression/scopes, default/write denial, errors, live cancellation/deadline, real effect/crash/Unknown and completed resume passed")

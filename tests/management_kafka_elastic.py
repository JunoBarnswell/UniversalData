"""Verify online Kafka process count changes and every read_committed output record."""
import http.cookiejar
import json
import os
import pathlib
import sys
import time
import urllib.request
import uuid

from product_version import PROTOCOL_VERSION

path = os.getenv("UD_TEST_CLUSTER_KAFKA_ELASTIC_CONFIG")
if not path or not pathlib.Path(path).is_file():
    print("Blocked: two Worker hosts and an isolated four-partition Kafka fixture are required")
    raise SystemExit(77)

from confluent_kafka import Consumer, Producer, TopicPartition

fixture = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
base = fixture["managementUrl"].rstrip("/")
csrf = ""


def call(path, method="GET", body=None):
    request = urllib.request.Request(base+"/api"+path,method=method,
        headers={"X-UD-CSRF":csrf,"Content-Type":"application/json"},
        data=None if body is None else json.dumps(body).encode())
    with client.open(request,timeout=10) as response:
        text = response.read()
        return json.loads(text) if text else None


def wait_for(check, seconds):
    limit = time.monotonic()+seconds
    while time.monotonic()<limit:
        if value := check():
            return value
        time.sleep(.5)
    raise AssertionError(f"{check.__name__} timed out")


bundle = fixture["package"]
name = bundle["id"]
assert bundle["version"] == PROTOCOL_VERSION
csrf = call("/auth/login","POST",fixture["identity"])["csrf"]
nodes = [node for node in call("/workers") if node["online"] and node["remote"]]
assert len(nodes) >= 2, nodes
call(f"/packages/{name}/versions/1","PUT",bundle)
request = dict(packageId=name,packageRevision="1",requestId=uuid.uuid4().hex,
     elastic=dict(minProcesses=1,maxProcesses=3,targetLagPerProcess=10000,maxPartitionsPerProcess=4))
ticket = call("/executions","POST",request)["id"]


def read():
    deadline = time.monotonic() + 20
    while True:
        try:
            state = call("/executions/"+ticket)
            break
        except urllib.error.HTTPError as error:
            if error.code != 503 or time.monotonic() >= deadline:
                raise AssertionError(f"Execution status unavailable ({error.code}): {error.read().decode()}") from error
            time.sleep(.3)
    assert state["state"] in ("Running","Queued","Claimed"), state
    elastic = state.get("elastic") or {}
    if elastic.get("reason") == "KafkaPartitionRecoveryRequired":
        members = call("/executions/"+ticket+"/members")
        raise AssertionError(f"A partition entered recovery during elasticity: {members}")
    return elastic


def settled(plan, processes):
    return (plan.get("activeProcesses") == plan.get("targetProcesses") == processes
            and not plan.get("reassigning"))


wait_for(lambda: settled(read(),1),45)
rows = fixture.get("rows",10_000_000)
field = fixture.get("idField","id")
producer = Producer(fixture["broker"])
source = fixture["sourceTopic"]
target = fixture["targetTopic"]
max_wait = fixture.get("seconds",2400)
deadline = time.monotonic() + max_wait
largest = 1
hosts = set()


def observe():
    global largest
    plan = read()
    active = plan.get("activeProcesses",0)
    if not settled(plan,active):
        return
    largest = max(largest,active)
    if active == 3:
        hosts.update(member["workerNode"] for member in call("/executions/"+ticket+"/members")
                     if member["state"] == "Running" and member["processId"])


for sequence in range(rows):
    data = json.dumps({field:sequence},separators=(",",":")).encode()
    while True:
        try:
            producer.produce(source,partition=sequence%4,value=data,key=str(sequence))
            break
        except BufferError:
            producer.poll(.02)
    if sequence and sequence % 50000 == 0:
        observe()
assert producer.flush(60) == 0

seen = bytearray((rows+7)//8)
received = 0
consumer = Consumer({**fixture["broker"],"group.id":"verify-"+uuid.uuid4().hex,
    "auto.offset.reset":"earliest","enable.auto.commit":False,"isolation.level":"read_committed"})
consumer.subscribe([target])
try:
    sample = 0.0
    while received < rows and time.monotonic() < deadline:
        now = time.monotonic()
        if now >= sample:
            observe()
            sample = now + 2
        for message in consumer.consume(num_messages=2000,timeout=.5):
            assert not message.error(), message.error()
            sequence = json.loads(message.value())[field]
            assert 0 <= sequence < rows
            index, bit = divmod(sequence,8)
            assert not seen[index] & (1 << bit), f"Duplicate output {sequence}"
            seen[index] |= 1 << bit
            received += 1
    assert received == rows and largest == 3 and len(hosts) >= 2, (received, rows, largest, hosts)
    def fully_shrunk():
        plan = read()
        return plan if plan.get("planRevision",0) >= 5 and settled(plan,1) else None

    final = wait_for(fully_shrunk,190)
    assert consumer.poll(2) is None, "Unexpected additional output"
finally:
    consumer.close()

group = fixture["consumerGroup"]
consumer = Consumer({**fixture["broker"],"group.id":group,"isolation.level":"read_committed"})
try:
    positions = consumer.committed([TopicPartition(source,p) for p in range(4)],timeout=10)
    for entry in positions:
        assert entry.offset == (rows+3-entry.partition)//4, entry
finally:
    consumer.close()
call("/executions/"+ticket+"/stop","POST")
print(f"PASS: {rows:,} exact results and offsets with 1→3→1 settled Worker processes at revision {final['planRevision']}")

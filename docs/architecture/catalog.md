# Embedded Catalog foundation

`include/ud/catalog.h` defines an optional embedded, host-controlled immutable JSON document repository. It is compiled with the Package component; minimal Columnar/Foundation builds do not depend on it. Catalog never starts or owns an Execution. The host owns the context, serializes calls, supplies the tenant and authorization callback, and chooses whether execution-observer failures affect its run.

Document kinds separate Project, Package, Environment, Deployment, Execution, Event and Audit metadata. Keys are `(tenant, kind, id, revision)`; revisions are positive integers and cannot be overwritten. Repeating the same key and compact serialized JSON is idempotent; different content returns `CORRUPT`. JSON property ordering remains significant. Package documents pass the existing native structural loader and must match the key's id; Project, Environment and Deployment now use domain validation described in [Project binding](project-environment.md); Execution/Audit remain supplied JSON objects.

The embedded format uses version 3 length-prefixed fields and a SHA-256 integrity checksum. Physical names hash tenant/id and include kind/revision; reads check the filename against the envelope. Tenant-filtered lists omit other tenants. Checksums detect corruption, not malicious replacement by a filesystem administrator. A host authorization callback is mandatory and is checked again for read/write; the default denies access.

Writes take a repository writer lock, reserve a new temporary file exclusively, write and flush its envelope, and publish atomically without overwriting an existing revision. A preexisting `.part` returns `BUSY` and is preserved for host investigation. This first implementation does not promise automatic recovery of abandoned Catalog staging files, deletion/retention, cross-document transactions, deployment activation or query indexes.

`ud_catalog_record_event` stores explicit package/execution/task/pipeline/component ids, state, result, timestamp and event kind at a caller-assigned sequence. It does not persist row values, SQL or configuration from an Execution. Arbitrary supplied metadata documents remain the host's responsibility. This is a persistence primitive; Project/Environment binding and immutable Deployment validation/version selection are now implemented; activation routing, full run-history integration and lineage/trace remain outstanding G47/G48 work.

Behavioral evidence: `tests/catalog.c` and `tests/catalog.py` exercise restart, immutable revisions/conflicts, two-tenant isolation, authorization revocation, native Package validation, structured event storage, checksum/filename corruption and preservation of an existing staging file. Saved test results are in `docs/acceptance/evidence/ssis-catalog`.

Native Execution 快照与事件历史现已实现，见 [运行历史契约](execution-history.md)。完整 Audit/Parameter Set/trace/lineage 及更丰富检索仍是待办。

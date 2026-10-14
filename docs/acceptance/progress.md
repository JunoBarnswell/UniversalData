# 当前实施与验收进度

<!-- Generated from metadata/product.json. -->

SDK 2.0.0 / ABI 10；Draft PR #6 Linux delivery: complete PRODUCT build, 78 actual CTest passes with 4 Oracle environment skips, real four-driver SQL/CDC/native bulk, six performance workloads, browser and independent installed C/.NET/Java/CLI/Web/Worker acceptance passed. 25 audit items have current scoped behavior passes, 17 remain Partial, 8 are Blocked, and 039/040 are NotApplicable; 4 source items remain Partial. Complete 50-item certification is not claimed.。

门禁通过 4/11；Linux 与 Windows 均必验；历史 ABI 的结果不计入当前版本。

| 整改 | 当前状态 |
|---|---|
| ETL-001 Complete product dependency closure | Implemented / Partial |
| ETL-002 SQL parameter arrays and truthful bulk capabilities | Implemented / Partial |
| ETL-003 Native execution identity | Implemented / Passed |
| ETL-004 Source record error ports | Implemented / Passed |
| ETL-005 Expression transform row errors | Implemented / Passed |
| ETL-006 Destination isolation and error barrier | Implemented / Partial |
| ETL-007 Complete unsigned integer domain | Implemented / Passed |
| ETL-008 Duration and calendar Interval | Implemented / Passed |
| ETL-009 JSON and dictionary fidelity | Implemented / Passed |
| ETL-010 CDC TRUNCATE semantics | Implemented / Partial |
| ETL-011 Recoverable transaction participants | Partial / Partial |
| ETL-012 Durable coordinator decisions | Implemented / Partial |
| ETL-013 PostgreSQL COPY loading | Implemented / Passed |
| ETL-014 MySQL and MariaDB native loading | Implemented / Passed |
| ETL-015 SQL Server BCP loading | Implemented / Partial |
| ETL-016 OCI staged Direct Path loading | Implemented / Blocked |
| ETL-017 Enterprise file layouts and publication | Implemented / Passed |
| ETL-018 Incremental overlap target capability proof | Implemented / Partial |
| ETL-019 Unified file and database discovery | Implemented / Passed |
| ETL-020 One SDK and protocol family | Implemented / Passed |
| ETL-021 SQLite indexed Native Catalog | Implemented / Passed |
| ETL-022 Durable execution logs | Implemented / Passed |
| ETL-023 Deployment and execution management | Implemented / Passed |
| ETL-024 CLI resource policy | Implemented / Passed |
| ETL-025 Online Schema Change barrier | Partial / Partial |
| ETL-026 CDC field presence and key images | Partial / Partial |
| ETL-027 Plain REST and receipt guarantees | Implemented / Partial |
| ETL-028 Row SQL command parameter binding | Implemented / Partial |
| ETL-029 Public managed Registry and native callback ownership | Implemented / Passed |
| ETL-030 Deployment-compiled C# scripts | Implemented / Partial |
| ETL-031 Balanced batch dispatch | Implemented / Passed |
| ETL-032 Import and Export Column | Implemented / Passed |
| ETL-033 XML Source | Implemented / Passed |
| ETL-034 XPath XSLT and XSD tasks | Implemented / Passed |
| ETL-035 SMTP task | Implemented / Blocked |
| ETL-036 Windows WMI tasks | Implemented / Blocked |
| ETL-037 FTP and SFTP resources | Implemented / Blocked |
| ETL-038 Avro IO | Partial / Partial |
| ETL-039 DQS removed by Microsoft | NotApplicable / NotApplicable |
| ETL-040 SSAS Data Mining no longer supported | NotApplicable / NotApplicable |
| ETL-041 SQL Server maintenance Agent and SSAS XMLA | Implemented / Blocked |
| ETL-042 Exact Distinct profiling with shared Compute budget | Implemented / Passed |
| ETL-043 Durable job queue and schedules | Implemented / Partial |
| ETL-044 Proven filter pushdown and streaming JSON array | Implemented / Partial |
| ETL-045 Azure Blob and ADLS Gen2 | Implemented / Blocked |
| ETL-046 S3 and MinIO | Implemented / Blocked |
| ETL-047 ASP.NET Core 10 management and isolated workers | Implemented / Passed |
| ETL-048 React Flow package designer | Implemented / Passed |
| ETL-049 Safe native runtime debugging | Implemented / Passed |
| ETL-050 Full Linux and Windows driver CI | Implemented / Blocked |
| ETL-051 Independent release consumers and capability manifests | Implemented / Partial |
| ETL-052 HTTP response variable mappings | Implemented / Passed |

三轮有界设计复核：

1. Evidence and trigger conditions（Closed）：Track exactly 52 items in the existing goals inventory; 039/040 NotApplicable, other 50 require current evidence.；Keep prior product model and evidence under history; no inherited pass/exemption in ABI 10.；Native driver claims must distinguish COPY, controlled native load, BCP, OCI staged Direct Path and generic ODBC arrays.；A missing external environment is Blocked, never Passed or silently downgraded.
2. Public contracts and ownership（Closed）：Native C17 alone owns Package, Execution, Component, Connector, schema/type semantics, row errors, resource authorization and execution IDs.；Clean-break all public ABI/protocols to 10. No bridge, migration-on-read or alternate evaluator/executor.；Data owns UINT64/Duration/Interval and dictionary/presence semantics; all serialization/expressions/compute/bindings project the same contracts.；Connection Manager owns transaction participants; Journal owns durable decisions and checkpoint epochs; Catalog indexes product/admin state only.；Managed delegates, registry leases and instance handles remain alive until Native disposal; exceptions become Native errors.；ASP.NET Core 10 dispatches one independent Worker per execution; React Flow persists stable graph IDs/layout with package versions; Native validates configurations.；Bounded implementation review: preserve ENUM ordinal zero and SET bitmaps with ordered UINT64 dictionaries; string labels cannot distinguish empty/numeric labels and empty sets. Native SQL scans explicitly project unsigned values and controlled LOAD DATA explicitly assigns numeric values; session variables are cleared.；Bounded implementation review: record absence differs from NULL; Changesets preserve it in Native Batch/Wire and only assign present values. Operations unable to represent partial images reject them. Source normal/error batches share Snapshot storage with persisted output port and physical source position.
3. Crash recovery and delivery scenarios（Closed）：Acquire and capability-check every participant before effects; durably identify participants before prepare; persist commit/rollback decision before phase-two commands.；Recovery follows the durable decision; no source cursor acknowledgement before ordinary/error destinations, transaction outcome and epoch state are jointly confirmed.；Plain REST unknown submission is RecoveryRequired and cannot be automatically replayed; explicit idempotent/receipt modes declare only proven guarantees.；Compatible schema change drains at the epoch barrier, maps stable field identities, rebinds affected components and persists schema version; incompatible changes preserve position/event without confirming cursor.；File append/overwrite uses controlled generations, original checksum and atomic publish; recovery checks publication facts rather than overwriting old artifacts.；Catalog uses SQLite transactions/WAL/indexes for multi-process management; execution correctness remains in Journal, not service memory or Catalog inference.；Durable queued jobs survive restart; concurrent limits and retry bounds are enforced, and unknown commit disables automatic retry.；Concentrated compile/test gates follow coherent implementation. Windows and Linux are required; missing acceptance resources remain Blocked.；Bounded transaction implementation review: prepared-ID disappearance alone is not a COMMIT proof. Each participant stores its binding digest with business effects; recovery checks prepared listings and visible receipts. Journal task/batch/cursor facts stay under the transaction owner until all participant outcomes resolve; completion logging precedes retiring pending facts. Required policy declares participants and needs an authorized state directory; unsupported resources are rejected before business effects.

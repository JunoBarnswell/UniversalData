# 当前实施与验收进度

<!-- Generated from metadata/product.json. -->

SDK 2.0.0 / ABI 10；2.0 implementation in progress: Native identity/Catalog/bulk foundation, unified type domains, CDC field presence and record error contracts; transaction/schema/product closure and acceptance remain pending。

门禁通过 0/11；Linux 与 Windows 均必验；历史 ABI 的结果不计入当前版本。

| 整改 | 当前状态 |
|---|---|
| ETL-001 Complete product dependency closure | Planned / NotRun |
| ETL-002 SQL parameter arrays and truthful bulk capabilities | Planned / NotRun |
| ETL-003 Native execution identity | Implemented / NotRun |
| ETL-004 Source record error ports | Partial / NotRun |
| ETL-005 Expression transform row errors | Partial / NotRun |
| ETL-006 Destination isolation and error barrier | Planned / NotRun |
| ETL-007 Complete unsigned integer domain | Partial / NotRun |
| ETL-008 Duration and calendar Interval | Partial / NotRun |
| ETL-009 JSON and dictionary fidelity | Planned / NotRun |
| ETL-010 CDC TRUNCATE semantics | Planned / NotRun |
| ETL-011 Recoverable transaction participants | Planned / NotRun |
| ETL-012 Durable coordinator decisions | Planned / NotRun |
| ETL-013 PostgreSQL COPY loading | Partial / NotRun |
| ETL-014 MySQL and MariaDB native loading | Partial / NotRun |
| ETL-015 SQL Server BCP loading | Planned / NotRun |
| ETL-016 OCI staged Direct Path loading | Planned / NotRun |
| ETL-017 Enterprise file layouts and publication | Planned / NotRun |
| ETL-018 Incremental overlap target capability proof | Planned / NotRun |
| ETL-019 Unified file and database discovery | Planned / NotRun |
| ETL-020 One SDK and protocol family | Implemented / NotRun |
| ETL-021 SQLite indexed Native Catalog | Implemented / NotRun |
| ETL-022 Durable execution logs | Planned / NotRun |
| ETL-023 Deployment and execution management | Planned / NotRun |
| ETL-024 CLI resource policy | Planned / NotRun |
| ETL-025 Online Schema Change barrier | Partial / NotRun |
| ETL-026 CDC field presence and key images | Partial / NotRun |
| ETL-027 Plain REST and receipt guarantees | Planned / NotRun |
| ETL-028 Row SQL command parameter binding | Planned / NotRun |
| ETL-029 Public managed Registry and native callback ownership | Planned / NotRun |
| ETL-030 Deployment-compiled C# scripts | Planned / NotRun |
| ETL-031 Balanced batch dispatch | Planned / NotRun |
| ETL-032 Import and Export Column | Planned / NotRun |
| ETL-033 XML Source | Planned / NotRun |
| ETL-034 XPath XSLT and XSD tasks | Planned / NotRun |
| ETL-035 SMTP task | Planned / NotRun |
| ETL-036 Windows WMI tasks | Planned / NotRun |
| ETL-037 FTP and SFTP resources | Planned / NotRun |
| ETL-038 Avro IO | Planned / NotRun |
| ETL-039 DQS removed by Microsoft | NotApplicable / NotApplicable |
| ETL-040 SSAS Data Mining no longer supported | NotApplicable / NotApplicable |
| ETL-041 SQL Server maintenance Agent and SSAS XMLA | Planned / NotRun |
| ETL-042 Exact Distinct profiling with shared Compute budget | Implemented / NotRun |
| ETL-043 Durable job queue and schedules | Planned / NotRun |
| ETL-044 Proven filter pushdown and streaming JSON array | Planned / NotRun |
| ETL-045 Azure Blob and ADLS Gen2 | Planned / NotRun |
| ETL-046 S3 and MinIO | Planned / NotRun |
| ETL-047 ASP.NET Core 10 management and isolated workers | Planned / NotRun |
| ETL-048 React Flow package designer | Planned / NotRun |
| ETL-049 Safe native runtime debugging | Planned / NotRun |
| ETL-050 Full Linux and Windows driver CI | Planned / NotRun |
| ETL-051 Independent release consumers and capability manifests | Planned / NotRun |
| ETL-052 HTTP response variable mappings | Planned / NotRun |

三轮有界设计复核：

1. Evidence and trigger conditions（Closed）：Track exactly 52 items in the existing goals inventory; 039/040 NotApplicable, other 50 require current evidence.；Keep prior product model and evidence under history; no inherited pass/exemption in ABI 10.；Native driver claims must distinguish COPY, controlled native load, BCP, OCI staged Direct Path and generic ODBC arrays.；A missing external environment is Blocked, never Passed or silently downgraded.
2. Public contracts and ownership（Closed）：Native C17 alone owns Package, Execution, Component, Connector, schema/type semantics, row errors, resource authorization and execution IDs.；Clean-break all public ABI/protocols to 10. No bridge, migration-on-read or alternate evaluator/executor.；Data owns UINT64/Duration/Interval and dictionary/presence semantics; all serialization/expressions/compute/bindings project the same contracts.；Connection Manager owns transaction participants; Journal owns durable decisions and checkpoint epochs; Catalog indexes product/admin state only.；Managed delegates, registry leases and instance handles remain alive until Native disposal; exceptions become Native errors.；ASP.NET Core 10 dispatches one independent Worker per execution; React Flow persists stable graph IDs/layout with package versions; Native validates configurations.；Bounded implementation review: preserve ENUM ordinal zero and SET bitmaps with ordered UINT64 dictionaries; string labels cannot distinguish empty/numeric labels and empty sets. Native SQL scans explicitly project unsigned values and controlled LOAD DATA explicitly assigns numeric values; session variables are cleared.；Bounded implementation review: record absence differs from NULL; Changesets preserve it in Native Batch/Wire and only assign present values. Operations unable to represent partial images reject them. Source normal/error batches share Snapshot storage with persisted output port and physical source position.
3. Crash recovery and delivery scenarios（Closed）：Acquire and capability-check every participant before effects; durably identify participants before prepare; persist commit/rollback decision before phase-two commands.；Recovery follows the durable decision; no source cursor acknowledgement before ordinary/error destinations, transaction outcome and epoch state are jointly confirmed.；Plain REST unknown submission is RecoveryRequired and cannot be automatically replayed; explicit idempotent/receipt modes declare only proven guarantees.；Compatible schema change drains at the epoch barrier, maps stable field identities, rebinds affected components and persists schema version; incompatible changes preserve position/event without confirming cursor.；File append/overwrite uses controlled generations, original checksum and atomic publish; recovery checks publication facts rather than overwriting old artifacts.；Catalog uses SQLite transactions/WAL/indexes for multi-process management; execution correctness remains in Journal, not service memory or Catalog inference.；Durable queued jobs survive restart; concurrent limits and retry bounds are enforced, and unknown commit disables automatic retry.；Concentrated compile/test gates follow coherent implementation. Windows and Linux are required; missing acceptance resources remain Blocked.

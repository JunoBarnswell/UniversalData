# 当前实施与验收进度

<!-- Generated from metadata/product.json. -->

SDK 9.0.0 / ABI 15；SDK 9 Native SQL, shared sessions and continuous ETL candidate。

门禁通过 0/11；Linux 与 Windows 均必验；历史 ABI 的结果不计入当前版本。

| 整改 | 当前状态 |
|---|---|
| ETL-001 Complete product dependency closure | Implemented / NotRun |
| ETL-002 SQL parameter arrays and truthful bulk capabilities | Implemented / NotRun |
| ETL-003 Native execution identity | Implemented / NotRun |
| ETL-004 Source record error ports | Implemented / NotRun |
| ETL-005 Expression transform row errors | Implemented / NotRun |
| ETL-006 Destination isolation and error barrier | Implemented / NotRun |
| ETL-007 Complete unsigned integer domain | Implemented / NotRun |
| ETL-008 Duration and calendar Interval | Implemented / NotRun |
| ETL-009 JSON and dictionary fidelity | Implemented / NotRun |
| ETL-010 CDC TRUNCATE semantics | Implemented / NotRun |
| ETL-011 Recoverable transaction participants | Partial / NotRun |
| ETL-012 Durable coordinator decisions | Implemented / NotRun |
| ETL-013 PostgreSQL COPY loading | Implemented / NotRun |
| ETL-014 MySQL and MariaDB native loading | Implemented / NotRun |
| ETL-015 SQL Server BCP loading | Implemented / NotRun |
| ETL-016 OCI staged Direct Path loading | Implemented / NotRun |
| ETL-017 Enterprise file layouts and publication | Implemented / NotRun |
| ETL-018 Incremental overlap target capability proof | Implemented / NotRun |
| ETL-019 Unified file and database discovery | Implemented / NotRun |
| ETL-020 One SDK and protocol family | Implemented / NotRun |
| ETL-021 SQLite indexed Native Catalog | Implemented / NotRun |
| ETL-022 Durable execution logs | Implemented / NotRun |
| ETL-023 Deployment and execution management | Implemented / NotRun |
| ETL-024 CLI resource policy | Implemented / NotRun |
| ETL-025 Online Schema Change barrier | Partial / NotRun |
| ETL-026 CDC field presence and key images | Partial / NotRun |
| ETL-027 Plain REST and receipt guarantees | Implemented / NotRun |
| ETL-028 Row SQL command parameter binding | Implemented / NotRun |
| ETL-029 Public managed Registry and native callback ownership | Implemented / NotRun |
| ETL-030 Deployment-compiled C# scripts | Implemented / NotRun |
| ETL-031 Balanced batch dispatch | Implemented / NotRun |
| ETL-032 Import and Export Column | Implemented / NotRun |
| ETL-033 XML Source | Implemented / NotRun |
| ETL-034 XPath XSLT and XSD tasks | Implemented / NotRun |
| ETL-035 SMTP task | Implemented / NotRun |
| ETL-036 Windows WMI tasks | Implemented / NotRun |
| ETL-037 FTP and SFTP resources | Implemented / NotRun |
| ETL-038 Avro IO | Partial / NotRun |
| ETL-039 DQS removed by Microsoft | NotApplicable / NotApplicable |
| ETL-040 SSAS Data Mining no longer supported | NotApplicable / NotApplicable |
| ETL-041 SQL Server maintenance Agent and SSAS XMLA | Implemented / NotRun |
| ETL-042 Exact Distinct profiling with shared Compute budget | Implemented / NotRun |
| ETL-043 Durable job queue and schedules | Implemented / NotRun |
| ETL-044 Proven filter pushdown and streaming JSON array | Implemented / NotRun |
| ETL-045 Azure Blob and ADLS Gen2 | Implemented / NotRun |
| ETL-046 S3 and MinIO | Implemented / NotRun |
| ETL-047 ASP.NET Core 10 management and isolated workers | Implemented / NotRun |
| ETL-048 React Flow package designer | Implemented / NotRun |
| ETL-049 Safe native runtime debugging | Implemented / NotRun |
| ETL-050 Full Linux and Windows driver CI | Implemented / NotRun |
| ETL-051 Independent release consumers and capability manifests | Implemented / NotRun |
| ETL-052 HTTP response variable mappings | Implemented / NotRun |

三轮有界设计复核：

1. Current source evidence and triggers（Implementation dependencies closed）：Management has no authentication and RunDispatcher persists request parameter JSON; all product execution/inspection must cross one scoped Worker entry；Native already owns Catalog, Journal, transactions, epochs and bulk transports; these remain the owners；Compute supports richer reports than profile.statistics exposes; Avro named references and reader resolution are absent in the baseline；PR #7 continuation: verified main remains 203c9c9f. Display-name rebinding cannot prove rename versus drop/re-add; SQL Server ODBC has an official XA request ABI independent of SQLGetXaSwitch exports; RunDispatcher process-local dictionaries cannot arbitrate multiple managers.
2. Public contracts and ownership（One Native contract）：Authentication produces tenant/subject/resource grants; the same scope reaches Native Connection Manager authorization；Catalog stores opaque immutable parameter references; only an authorized Worker resolves encrypted values；Managed descriptors project Native dynamic ports, plans, errors, replay-safe flags, controls and confirmations; Native retains callback owners and sealed batches；Continuation contracts: physical provider identity is distinct from Package logical identity and lineage, with resource scope independent of Package. Native Schema/JSON/Wire/Arrow own the projection. Native Catalog owns CAS, durable fencing generations and pool ceilings; C# and JNI only project these. Microsoft XACALLPARAM goes directly through the current ODBC participant API and Native Journal coordinator.
3. Crash recovery and delivery（Fail closed at uncertain effects）：A Worker cannot start business execution before the service records its Native identity; unknown outcomes require recovery；Source row counters join the epoch manifest so automatic Row Identity survives restart；Control events are notified at Native aligned boundaries without managed forwarding; state snapshots use Native Wire/Journal；Untrusted modules require Linux OS namespace/cgroup isolation; missing infrastructure refuses execution; Windows sandbox acceptance remains blocked；Remote CI billing limits and unavailable independent resources never become Passed；Continuation crash review: write the SQL Server receipt inside the XA branch before business DML; XA recovery plus digest-matching receipts prove outcomes, and prepared branches survive close. A lost Worker lease fences further Catalog writes and cancels its process; an expired Running record becomes RecoveryRequired without automatic side-effect replay. Wire layout revision 2 and checkpoint revision 3 explicitly refuse old recovery payloads. Cross-machine agents and structural state rebinding are still code gaps, not environment blocks.；Two-manager kill acceptance found private Worker execution/event records can lead the management index. Worker protocol revision 2 now transfers bounded canonical Native Catalog pages, continues from durable Native revisions/sequences, and rejects legacy requests; the host cannot relabel an unresolved resume as an ordinary preflight failure. The strengthened test verifies the resumed Native terminal record, not only the product status.

# 当前实施与验收进度

<!-- Generated from metadata/product.json. -->

SDK 2.1.0 / ABI 11；Native contract implementation committed for concentrated Linux acceptance; complete SSIS parity remains Partial.。

门禁通过 0/11；Linux 与 Windows 均必验；历史 ABI 的结果不计入当前版本。

| 整改 | 当前状态 |
|---|---|
| ETL-001 Complete product dependency closure | Implemented / Partial |
| ETL-002 SQL parameter arrays and truthful bulk capabilities | Implemented / Partial |
| ETL-003 Native execution identity | Implemented / Partial |
| ETL-004 Source record error ports | Implemented / Partial |
| ETL-005 Expression transform row errors | Implemented / Partial |
| ETL-006 Destination isolation and error barrier | Implemented / Partial |
| ETL-007 Complete unsigned integer domain | Implemented / Partial |
| ETL-008 Duration and calendar Interval | Implemented / Partial |
| ETL-009 JSON and dictionary fidelity | Implemented / Partial |
| ETL-010 CDC TRUNCATE semantics | Implemented / Partial |
| ETL-011 Recoverable transaction participants | Partial / Partial |
| ETL-012 Durable coordinator decisions | Implemented / Partial |
| ETL-013 PostgreSQL COPY loading | Implemented / Partial |
| ETL-014 MySQL and MariaDB native loading | Implemented / Partial |
| ETL-015 SQL Server BCP loading | Implemented / Partial |
| ETL-016 OCI staged Direct Path loading | Implemented / Partial |
| ETL-017 Enterprise file layouts and publication | Implemented / Partial |
| ETL-018 Incremental overlap target capability proof | Implemented / Partial |
| ETL-019 Unified file and database discovery | Implemented / Partial |
| ETL-020 One SDK and protocol family | Implemented / Partial |
| ETL-021 SQLite indexed Native Catalog | Implemented / Partial |
| ETL-022 Durable execution logs | Implemented / Partial |
| ETL-023 Deployment and execution management | Implemented / Partial |
| ETL-024 CLI resource policy | Implemented / Partial |
| ETL-025 Online Schema Change barrier | Partial / Partial |
| ETL-026 CDC field presence and key images | Partial / Partial |
| ETL-027 Plain REST and receipt guarantees | Implemented / Partial |
| ETL-028 Row SQL command parameter binding | Implemented / Partial |
| ETL-029 Public managed Registry and native callback ownership | Implemented / Partial |
| ETL-030 Deployment-compiled C# scripts | Implemented / Partial |
| ETL-031 Balanced batch dispatch | Implemented / Partial |
| ETL-032 Import and Export Column | Implemented / Partial |
| ETL-033 XML Source | Implemented / Partial |
| ETL-034 XPath XSLT and XSD tasks | Implemented / Partial |
| ETL-035 SMTP task | Implemented / Partial |
| ETL-036 Windows WMI tasks | Implemented / Partial |
| ETL-037 FTP and SFTP resources | Implemented / Partial |
| ETL-038 Avro IO | Partial / Partial |
| ETL-039 DQS removed by Microsoft | NotApplicable / NotApplicable |
| ETL-040 SSAS Data Mining no longer supported | NotApplicable / NotApplicable |
| ETL-041 SQL Server maintenance Agent and SSAS XMLA | Implemented / Partial |
| ETL-042 Exact Distinct profiling with shared Compute budget | Implemented / Partial |
| ETL-043 Durable job queue and schedules | Implemented / Partial |
| ETL-044 Proven filter pushdown and streaming JSON array | Implemented / Partial |
| ETL-045 Azure Blob and ADLS Gen2 | Implemented / Partial |
| ETL-046 S3 and MinIO | Implemented / Partial |
| ETL-047 ASP.NET Core 10 management and isolated workers | Implemented / Partial |
| ETL-048 React Flow package designer | Implemented / Partial |
| ETL-049 Safe native runtime debugging | Implemented / Partial |
| ETL-050 Full Linux and Windows driver CI | Implemented / Partial |
| ETL-051 Independent release consumers and capability manifests | Implemented / Partial |
| ETL-052 HTTP response variable mappings | Implemented / Partial |

三轮有界设计复核：

1. Current source evidence and triggers（Implementation dependencies closed）：Management has no authentication and RunDispatcher persists request parameter JSON; all product execution/inspection must cross one scoped Worker entry；Native already owns Catalog, Journal, transactions, epochs and bulk transports; these remain the owners；Compute supports richer reports than profile.statistics exposes; Avro named references and reader resolution are absent in the baseline；PR #7 continuation: verified main remains 203c9c9f. Display-name rebinding cannot prove rename versus drop/re-add; SQL Server ODBC has an official XA request ABI independent of SQLGetXaSwitch exports; RunDispatcher process-local dictionaries cannot arbitrate multiple managers.
2. Public contracts and ownership（One Native contract）：Authentication produces tenant/subject/resource grants; the same scope reaches Native Connection Manager authorization；Catalog stores opaque immutable parameter references; only an authorized Worker resolves encrypted values；Managed descriptors project Native dynamic ports, plans, errors, replay-safe flags, controls and confirmations; Native retains callback owners and sealed batches；Continuation contracts: physical provider identity is distinct from Package logical identity and lineage, with resource scope independent of Package. Native Schema/JSON/Wire/Arrow own the projection. Native Catalog owns CAS, durable fencing generations and pool ceilings; C# and JNI only project these. Microsoft XACALLPARAM goes directly through the current ODBC participant API and Native Journal coordinator.
3. Crash recovery and delivery（Fail closed at uncertain effects）：A Worker cannot start business execution before the service records its Native identity; unknown outcomes require recovery；Source row counters join the epoch manifest so automatic Row Identity survives restart；Control events are notified at Native aligned boundaries without managed forwarding; state snapshots use Native Wire/Journal；Untrusted modules require Linux OS namespace/cgroup isolation; missing infrastructure refuses execution; Windows sandbox acceptance remains blocked；Remote CI billing limits and unavailable independent resources never become Passed；Continuation crash review: write the SQL Server receipt inside the XA branch before business DML; XA recovery plus digest-matching receipts prove outcomes, and prepared branches survive close. A lost Worker lease fences further Catalog writes and cancels its process; an expired Running record becomes RecoveryRequired without automatic side-effect replay. Wire layout revision 2 and checkpoint revision 3 explicitly refuse old recovery payloads. Cross-machine agents and structural state rebinding are still code gaps, not environment blocks.；Two-manager kill acceptance found private Worker execution/event records can lead the management index. Worker protocol revision 2 now transfers bounded canonical Native Catalog pages, continues from durable Native revisions/sequences, and rejects legacy requests; the host cannot relabel an unresolved resume as an ordinary preflight failure. The strengthened test verifies the resumed Native terminal record, not only the product status.

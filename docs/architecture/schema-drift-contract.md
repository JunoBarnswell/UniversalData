# Canonical Schema Discovery / Drift

Connector 只提供真实 metadata/discover；SQL 类型映射与 Snapshot、Fingerprint、Diff、Compatibility、Mapping 均由 Data 持有。Fingerprint 编码规范字段属性和键约束，不使用 JSON 排版。外部缺稳定物理列 ID 时用明确来源命名空间建立初始 ID；不能把同 ordinal 或相似名称当成重命名证据。

Strict 要求已声明 Schema 一致；Compatible 仅放行明确无损变化（额外未投影列、可由 ID 重排等），不隐式缩窄或改变 NULL/Decimal 语义；ExplicitMapping 要求完整映射并使用唯一 Data cast。Added/Removed/Renamed/Reordered/Type/Nullability/Precision/Key 的差异可序列化。

源 preflight 完成实际发现与策略检查后才允许目标副作用。目标已有 schema 的变化在写入之前同样检查。Resume 中 schema fingerprint 与原政策必须一致。发现经过 Lease 的 Discover 授权、Deadline/Cancel 与资源释放；缓存仅属于本次 Execution，不能隐式复用过期元数据。

C/.NET/Java 消费同一个 Native snapshot/diff JSON，不自己解析 SQL 类型或比较字段。

规范政策 JSON 为 `{"policy":"strict|compatible|explicitMapping","mapping":[{"expected":"ID/name","actual":"ID/name"}]}`。SQL Source 有 table 时通过真实 discover 进行 preflight；任意表达式查询仍使用明确的 typed result schema，由 Reader 校验列名和类型。目标统一发现存储 Schema，将目标列身份绑定到输入映射，使用同一 Data compare/cast，默认 Compatible；跨类型或重命名必须完整 ExplicitMapping。

目标支持完整命名列映射；未供应的列由后端约束/default 决定，写失败保留原始错误。SQLite NUMERIC 的精确精度、PostgreSQL 无界 numeric/非 UTC timestamp 等没有可靠 canonical domain 时返回 Unsupported。不会推测 SQL 类型。Nullable/Decimal/Key 政策与整个实际 Schema 的指纹进入 Journal；恢复期间变化拒绝，包含新增的未投影列。

Diff 使用嵌套列的原生 identity 报告子结构变化。Schema 报告有固定字节上限；失败的 preflight 也进入原生诊断，C/.NET/Java 查询结果完全相同。

## Physical and logical field identity

`ud_field.physical_identity` names a proven provider object/column incarnation, independently of the Package logical `identity` and sorted origin `lineage`. Discovery takes separate logical and tenant/resource namespaces. PostgreSQL supplies database/relation OIDs and attribute numbers; column rename retains physical identity while drop/re-add receives a different identity. A first discovery may bind an unbound logical projection by name; once physical identity is present, rebinding never falls back to the display name. Providers without physical proof leave it unspecified.

Schema comparison and mapping use the Native recursive projection for Struct fields: compatible identity-preserving rename/reorder, ignored unprojected additions, nullability checks and exact numeric widening share the same plan and cast kernels. Missing projected fields, lost/replaced known physical identities and lossy conversions are rejected. JSON, Arrow metadata (`ud.physical_id`) and Wire carry the same identity. Wire layout revision 2 has an unambiguous marker; checkpoint revision 3 rejects former snapshots and preserves their files. Native plugins publish complete Field/Value layout sizes and must be rebuilt.

This closes identity/projection foundations. Structural running-plan/state migration remains incomplete; the pipeline currently retains its bound output shape. MySQL and SQL Server provider physical discovery also require further development. New current-source unit/real-DDL tests are registered; prior-source tests do not certify this change.

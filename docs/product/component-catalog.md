# 当前组件清单与 SSIS 映射

<!-- Generated from metadata/product.json. -->

SDK 10.0.0，唯一 ABI/协议 15。Linux 与 Windows 均须验收。下列目录记录审查源码的组件入口和能力边界，不等于当前运行验收；50 项适用整改状态见 metadata/product.json 的 goals。

| SSIS 职责 | 实际入口/ID | 实现与边界 |
|---|---|---|
| Package / Project / Environment | `ud_package`、`ud_execution`、Catalog deployment | 不可变定义；一个根 Execution；子包使用独立 Activation |
| Sequence / For / Foreach | `sequence`、`for`、`foreach` | 原生容器、作用域、显式上限 |
| Precedence / Event Handlers | success/failure/completion + AST + AND/OR；before/after/failure | 共享 Executor 的有界任务调度；deadline、有序事件；错误处理不清除原始失败 |
| Data Flow Task / Execute Package | `dataFlow`、`executePackage` | 独立 Control Flow / Data Flow，共用根资源预算；异步边界形成可并行 Execution Tree；每组件独立 Activation |
| Execute SQL / File System / Execute Process | `sql.execute`、`file.system`、`process.execute` | 授权先于 I/O；Windows/POSIX 原生进程管理 |
| Data Profiling / HTTP / Native Custom Task | `data.profile`、`profile.statistics`、`http.request`、第三方 Task descriptors | HTTP 依赖可选 libcurl，关闭时 Unsupported；插件必须当前完整 ABI |
| Flat File / JSON / Database Source | `file.csv.source`、`file.ndjson.source`、`sql.source` | 唯一 typed schema；SQL 恢复需要持久 snapshot，缺少即拒绝 SQL Server and PostgreSQL use typed predicates for proven keyed Bool/Int64 scans and can omit unused primitive columns from table reads. |
| Flat File / JSON / Database Destination | `file.csv.destination`、`file.ndjson.destination`、`sql.destination` | 文件 staging/hash/publish；SQL 事务+receipt，不把输入消费视为提交 |
| Data Conversion / Derived Column | `cast`、`derive` | Data 转换和唯一 Expression AST |
| Copy Column / Character Map | `copyColumn`、`characterMap` | 复制列保留 lineage；trim/upper_ascii/lower_ascii 保留非 ASCII UTF8 |
| Conditional Split / Multicast / Union All | `split`、`multicast`、`unionAll`、`pass` | 多端口、有界队列、sealed 引用共享、schema 检查 |
| Sort / Aggregate / Join | `sort`、`aggregate`、`join` | Stable external sort concatenates ordered fixed-width key ranges and merges overlapping runs within Buffer Manager limits. Aggregate and join use the declared key comparison semantics. |
| Lookup | cacheBuild, cacheLookup, dbLookup | Full probes a sealed execution reference. Partial caches database observations by probe key. None issues one provider lookup per non-null probe. Match, record error and configured no-match outputs use ports 0, 1 and 2. Epoch-scoped database references use a read snapshot. |
| Slowly Changing Dimension | scd, scd.destination | The inferredMember Boolean identifies a current placeholder. Materialization updates mapped attributes and clears the flag in one preimage-checked update, retaining surrogate, validity and version. |
| Window | `window` | Row Number/Running Sum，分区及排序；BI Dataset/Query planner/并行分区待实现 |
| Row Count / Audit | `rowCount`、`audit` | Activation 变量及根有序事件；并发任务不得写共享祖先变量 |
| Error Output / Data Quality | `quality` 普通端口 0、错误端口 1 | Foreign Key 双输入、完整规则版本/政策、多 Violation、正规错误端口；分类与实际提交/确认分开。 |
| Connection Manager | file/process/http definitions；SQLite/PostgreSQL physical driver | file/process/http definitions；SQLite/PostgreSQL typed driver；SQLite 纳入 Windows 验收，PostgreSQL 数据库行为未验收 |
| Expression Function | `abs`、`length`、`trim`、`upper_ascii`、`lower_ascii`、`concat`、`coalesce`、`substring` | 8 个 native pure descriptors；length/substring 按 UTF8 code point；coalesce 参数显式求值 |
| Buffer Manager | `ud_buffer_manager` / `ud_operator` | 按真实 allocation capacity 去重计量；驻留只 retain；spill 使用唯一 wire；显式 View 和 working quota；失败文件由管理器持有并重试清理 |
| Transaction / Checkpoint / Catalog | Journal 4、目标 receipt、immutable artifacts/history | 当前版本索引式分段 Journal、原子快照/manifest、目标 receipt、不可变 artifacts/history；不提供跨任意目标的分布式事务 |
| Event-time streaming / Studio observation | eventTime; eventWindow; eventDeduplicate; eventJoin; Native Catalog execution feed | Native Watermark and bounded shared working subquotas. Final tumbling/sliding/session aggregates with stable RowIds. Keyed tumbling and sliding windows with count, sum or avg can emit inserts, updates with before images, and deletes. The window state retains the revision and published value through checkpoint recovery. eventDeduplicate keeps the first row per key until its event-time TTL reaches the Watermark. eventJoin emits inner matches within the configured inclusive time difference. Deduplicate and join drop rows at or behind the common Watermark. Session remains final-only. General trigger policy, full CDC retractions and distributed state/rescaling are not established. First-N probes still require explicit Debug. |
| Change Data Capture | CDC source, sql.destination (changeset) | microBatchRows and microBatchMs group complete source transactions. A sealed epoch stores original watermarks and the last log position. SQL Server sends bounded ordered statement batches. PostgreSQL uses bounded libpq pipelines and checks individual affected-row counts. Grouping is source-specific, not a general Package microbatch/time trigger. |
| 开放湖表 | `iceberg.source`、`iceberg.destination` | Iceberg v2 REST Catalog / Parquet：固定快照读取，append、按键 upsert、CDC changeset 及快照回执核对。可按 Catalog 已有分区规格写入；源码夹具覆盖 bucket 演进与跨规格 upsert。Native 新建表仍为无分区；仓库使用受权共享文件路径，不宣称远端对象存储凭据、表维护或通用独立 SQL DELETE 已可用。真实 PRODUCT 组合验证另记。 |
| 消息发布、订阅与消费 | mq.source、mq.destination | ActiveMQ AMQP 1.0、RabbitMQ AMQP 0-9-1、RocketMQ 5 gRPC、Kafka。使用实时发布与确认结果，真实 broker 及完整跨主机验收另行记录。 |

未完成扩展：ETL-011（合格 SQL Server 事务参与方）、ETL-025/026（在线身份/在场证明）、ETL-038（Avro broader named references/unions）与当前流处理/湖表边界单独跟踪；历史运行不计入 SDK 10 当前验收。

Arrow C Data Interface 与原生内存数据模型不等同于 Parquet driver。Microsoft .dtsx/COM/SSISDB/MSDTC 专属兼容不承诺；没有占位注册或旧 generic operation 路径。

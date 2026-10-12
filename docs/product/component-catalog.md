# 当前组件清单与 SSIS 映射

版本族 4。当前范围为统一架构和原生核心组件；协议及高级扩展不作为本轮核心验收前置条件。设计、实现和执行证据分别记录，最终平台结果见 `../acceptance/ssis-status.json`。

| SSIS 职责 | 实际入口/ID | 实现与边界 |
|---|---|---|
| Package / Project / Environment | `ud_package`、`ud_execution`、Catalog deployment | 不可变定义；一个根 Execution；子包使用独立 Activation |
| Sequence / For / Foreach | `sequence`、`for`、`foreach` | 原生容器、作用域、显式上限 |
| Precedence / Event Handlers | success/failure/completion + AST + AND/OR；before/after/failure | 有限并发、任务 deadline、顺序事件；失败处理不清除原始失败 |
| Data Flow Task / Execute Package | `dataFlow`、`executePackage` | 同一资源预算、连接、Journal、Registry |
| Execute SQL / File System / Execute Process | `sql.execute`、`file.system`、`process.execute` | 授权先于 I/O；Windows/POSIX 原生进程管理 |
| Data Profiling / HTTP / Native Custom Task | `data.profile`、`profile.statistics`、`http.request`、第三方 Task descriptors | HTTP 依赖可选 libcurl，关闭时 Unsupported；插件必须当前完整 ABI |
| Flat File / JSON / Database Source | `file.csv.source`、`file.ndjson.source`、`sql.source` | 唯一 typed schema；SQL 恢复需要持久 snapshot，缺少即拒绝 |
| Flat File / JSON / Database Destination | `file.csv.destination`、`file.ndjson.destination`、`sql.destination` | 文件 staging/hash/publish；SQL 事务+receipt，不把输入消费视为提交 |
| Data Conversion / Derived Column | `cast`、`derive` | Data 转换和唯一 Expression AST |
| Copy Column / Character Map | `copyColumn`、`characterMap` | 复制列保留 lineage；trim/upper_ascii/lower_ascii 保留非 ASCII UTF8 |
| Conditional Split / Multicast / Union All | `split`、`multicast`、`unionAll`、`pass` | 多端口、有界队列、sealed 引用共享、schema 检查 |
| Sort / Aggregate / Join | `sort`、`aggregate`、`join` | 共用 Compute operator 和 Buffer Manager；Sort 稳定外存归并，Aggregate 有界分组，Join 有界 block nested loops |
| Lookup | `lookup` | 确定性首个匹配、inner lookup；无匹配不输出；独立 Cache Transform 尚未实现 |
| Window | `window` | Row Number/Running Sum，分区及排序；BI Dataset/Query planner/并行分区待实现 |
| Row Count / Audit | `rowCount`、`audit` | Activation 变量及根有序事件；并发任务不得写共享祖先变量 |
| Error Output / Data Quality | `quality` 普通端口 0、错误端口 1 | NotNull/Unique/PrimaryKey/Range/Regex/RowCount；错误行携带 rule、source row、code；目标负责持久化 |
| Connection Manager | file/process/http definitions；SQLite/PostgreSQL physical driver | SQLite 本轮实际验收；PostgreSQL 源码接入当前契约但本轮无数据库验收 |
| Expression Function | `abs`、`length`、`trim`、`upper_ascii`、`lower_ascii`、`concat`、`coalesce`、`substring` | 8 个 native pure descriptors；length/substring 按 UTF8 code point；coalesce 参数显式求值 |
| Buffer Manager | `ud_buffer_manager` / `ud_operator` | 逻辑 resident/spill 配额、wire/hash、writer fence、失败/取消清理；实际分配由 context 限额控制 |
| Transaction / Checkpoint / Catalog | Journal 4、目标 receipt、immutable artifacts/history | 支持已声明可重放链；跨任意目标分布式事务不提供 |

未实现扩展：Merge/Merge Join、Pivot/Unpivot、完整 Cache Transform、Pipeline ForeignKey reference input、SCD、Fuzzy、Term、CDC、并行分区、BI planner、ODBC/SQL Server、MySQL、HTTP Source/Destination、SFTP、Kafka、RabbitMQ、Cloud、Excel、Parquet。Arrow C Data Interface 和统一内存数据模型已实现，不等同于 Parquet driver。

ODBC/DuckDB 旧 generic operation 原型已删除；今后必须通过当前连接/组件契约重新实现。未实现能力没有占位注册。Microsoft `.dtsx`/COM/SSISDB/MSDTC 的专属兼容不承诺。

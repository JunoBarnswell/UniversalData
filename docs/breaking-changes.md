# Breaking Changes：SDK 1.2.0 / ABI 6

本轮采用 Clean-break。旧 ABI 4 以及此前对象、定义、插件、检查点需要从源数据重新生成并重新编译宿主；没有 ABI 别名、旧 Journal reader 或兼容执行链。

| 边界 | 唯一当前契约 |
|---|---|
| 版本 | metadata/product.json 生成 C、C#、Java、CMake/Maven 坐标与验收报告；ABI/Package/Schema/Plan/Wire/Plugin/Worker/Checkpoint 同为 5 |
| Registry | 仅 Task、Source、Transform、Destination、Function 五类；Function 枚举为 5；删除无生命周期的 Connection、Enumerator、LogProvider |
| Buffer Store | 驻留按真实 allocation capacity 去重；working_limit 必填；ud_buffer_read 返回显式 ud_buffer_view；batch 借用至 view release |
| Block release | 有 reader 时 Busy 且保持句柄；其他结果消费句柄；删除失败的 spill 仍由 manager 拥有，可重试 destroy |
| Queue | 有界并发队列持有 ud_buffer_block；push/pop 转移 block 所有权；删除按当前状态返回 status |
| Pipeline | 源及异步边界形成 Execution Tree，共用根 Executor；组件独立 Activation；Step 禁止作用域写入，Finish 在全图 drain 后执行 |
| Metrics | 删除 completed_tasks/completedTasks；使用 executed_tasks、durable_tasks、recovery_tasks、resumed_tasks；增加树数量、树并发与共享 worker 高水位 |
| Checkpoint | 索引、哈希链分段帧、任务 owner 回收、原子快照/head/tail；旧逐记录 .udj 不读取 |
| 交付 | 以当前 Release ZIP、清单哈希、独立 C/NuGet/Maven/JNI 消费作为证据；历史 ABI 4 产物不代表当前完成 |

旧 Job/DAG/Run、Query 专用模型、generic operation、默认 CSV/NULL/损坏数据降级路径继续下线。核心类型、表达式、Compute 和资源所有权用于 ETL 及未来 BI。未实现扩展保持真实 Planned/Partial，不注册占位组件。

文件 stage/hash/publish、SQLite transaction/receipt 和 CommitUnknown 继续通过同一 Runtime。没有任意数据库目标的全局分布式事务；不承诺 .dtsx、COM、SSISDB 或 MSDTC 兼容。

唯一事实来源与本轮门禁见 [product.json](../metadata/product.json)，设计见 [基础设施收敛](architecture/infrastructure-convergence.md)。

## 四引擎 ABI 6

SDK 1.2.0 同批升级 ABI/Package/Plan/Schema/Wire/Plugin/Worker/Checkpoint 为 6。Task descriptor 支持准备、提交、并发取消及恰好一次 Dispose；Enumerator 是具有 configure/open/next/position/dispose 的正式类别。新增 optional output、阻塞/状态/亲和/恢复能力。驱动表新增事务、分页 metadata 与可取消/限时 I/O 控制；Host API 工厂携带 Activation 控制。旧布局整体 Incompatible，不提供桥接。

Root Pool 常驻，Control 由完成事件推进入度，树由 Path 输入/容量/End 定向唤醒。Native Operator 新增根 executor 与分区数，Sort/Join/Aggregate 复用同一算法和预算。执行指标新增已提交/已持久确认行数与错误计数；C/.NET/Java 同步投影，Explain/Diagnostics 返回原生绑定计划 JSON。

约束 `evaluation` 为 constraint/expression/and/or，`combine` 独立定义入边 AND/OR。Task `retry` 仅接受声明 ReplaySafe 的可重试 Transient；CommitUnknown 永远不重试。事务 `required/supported/notSupported` 明确继承，只使用可证明的单一 Session；禁止 SQL Task 通过原始 BEGIN/COMMIT 绕过事务 owner。容器事务与持久子任务检查点组合当前拒绝。

Foreach 的默认 collection 与显式 core.variable/core.files/core.query/core.schema 使用同一 Registry 生命周期。查询/Schema 缺少可重建的外部快照令牌，恢复绑定阶段拒绝。文件分区按稳定全局行位置定义 range/round-robin，源版本由现有 Journal 文件摘要核验。恢复计划显式序列化树调度，分区算子仍按确定顺序归并；不是多资源分布式 exactly-once。

Cast `errorPolicy=fail/ignore/redirect` 使用同一 Data 标量转换和正规 error Path，包含 ErrorCode、ColumnID、SourceRowID。Redirect 必须连接 error port，并在 Prepare/I/O 前拒绝缺失路径。

本轮仅验证 Linux；Windows 豁免。历史 ABI 4/5 Windows 记录保留作来源，不能当作 ABI 6 验收。

# SDK 9.0.0 / ABI 15

SDK 9 的普通、单库事务和跨库事务操作均由 Native 驱动执行，业务代码继续使用官方 `ISqlSugarClient`。直接访问物理连接、厂商命令对象及直接提交协调器分支返回 `Unsupported`。Worker 私有协议升至 8，应用与 Worker 必须使用相同主版本和私有协议。Native ABI 与 Package 版本维持 15。

# SDK 8.0.0 / ABI 15

C# 通过 `DataClient` 注册命名数据源和业务程序集。 `RunAsync` 在 Worker 调用公开静态方法，数据库操作在 `DataContext.Database(name).UseAsync` 作用域内使用官方 `ISqlSugarClient`。NuGet 锁定 `SqlSugarCore` 5.1.4.216。应用须用 SDK 8 重新编译。原有 `SqlSugarClient.OpenAsync` 和 `SqlCursor<T>` 的调用点迁至官方 SqlSugar 查询或原生 ETL。

大数据搬运迁至 `DataClient.Etl`，复用 Native Package 的批次、资源授权和恢复语义。已构建的 Package 可用 `new EtlClient(data)` 执行、检查和恢复。执行方法直接返回结果，失败从 `UniversalDataException` 或其派生类型读取状态码和诊断。进程中断后，`DataExecutionException.Reference` 保留恢复所需的标识。

SDK 8 要求同一发行版的 Worker 协议修订 6。Native ABI、Package 与持久检查点仍为 15。详情见 [C# API](api.md)。

# SDK 7.0.0 / ABI 15

C# 从 `SqlSugarClient.OpenAsync` 打开会话，从 `EtlClient` 启动、预览并恢复 Package。SQL 读取使用 `SqlCursor<T>`，按行或按原生批次返回 `Result<T>`。执行结束返回 `End`。进程中断后的诊断保留 Worker 或传输原因，可能已写入的数据通过 `CommitUnknown` 进入核对。

v14 Package 使用 `python scripts/migrate_package_v14_to_v15.py input.json output.json` 生成新副本。切换前用 v14 处理待恢复执行。Catalog 以原计划时间为间隔起点迁移旧日程，历史执行仍保留原版本。重新发布 v15 Package 后，将作业绑定到新的发布修订。

普通作业按管理端保留的 CPU 槽位、Native 内存和进程数量派发。Worker 节点在本地再次核对额度和资源授权。显式分片计划继续使用 Linux systemd 或 Windows Job Objects 的进程级限制。

安装 `UniversalData` 7.0.0 时还原锁定版本的 Windows x64 和 Linux x64 运行时包。Worker 随应用构建和发布。SQL Server ODBC 18 和 Oracle Instant Client 等需由宿主安装的客户端仍按各平台供应商要求提供。

# SDK 6.0.0 / ABI 14

C 公共 ABI、插件、Package、Schema、Wire、Worker 和检查点协议同步升级到 14。按 `metadata/product.json` 生成 C、C#、Java 和 Studio 版本。重建宿主、插件和消费者后一起发布。

将 v13 的 Package 定义离线写入新文件，再由 v14 校验和部署：

```powershell
python scripts/migrate_package_v13_to_v14.py source-v13.json copy-v14.json
```

迁移工具只升级 Package 和识别到的 Schema 信封。业务行及其 `version` 字段保留原值。旧检查点、运行中的作业及二进制 Wire 由其 ABI 13 运行环境处理，升级前完成这些作业并保留对应记录。

为 ABI 14 宿主配置独立的数据目录，并在新目录部署迁移后的 Package。保留 ABI 13 的 Catalog 与检查点目录，使用对应版本查看旧运行记录和处理未结作业。

### 历史版本

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

池复用必须声明 `UD_CONNECT_POOL` 并实现 `reset_session`；重置失败立即退役，关闭失败仍由 manager 持有。SQLite 阻止跨租约的 PRAGMA/ATTACH/临时对象状态变更；PostgreSQL DISCARD ALL 后恢复原生 UTC/格式基线。PostgreSQL 模块要求 libpq 17+，命令、单行游标与非阻塞服务端取消共用同一 I/O owner。

Diagnostics 包含任务 precedence 决策、ready 等待/耗时/CPU、树/组件/Path 行数及等待、共享调度量子和连接池结果。任务/管道记录分别限制 1024/256 条并报告丢弃计数；诊断分配失败不会改变已提交业务结果。

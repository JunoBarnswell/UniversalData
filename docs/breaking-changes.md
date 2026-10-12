# Breaking Changes：SDK 1.1.0 / ABI 5

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

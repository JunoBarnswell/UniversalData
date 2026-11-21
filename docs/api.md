# UniversalData C# SDK 9 / Native ABI 15

安装 `UniversalData` 9.0.0 会还原官方 `SqlSugarCore` 5.1.4.216 及同版本运行时包。应用通过 `DataClient` 注册数据源、业务程序集和 Worker 配额。数据源按名称解析，Worker 根据宿主策略检查资源和凭据。SQLite 使用绝对路径。远程资源可在添加数据源时用 `options.Resource` 对应策略中的显式授权。

## 业务调用与数据库事务

`RunAsync` 在 Worker 中调用已注册程序集的公开静态方法，输入和输出使用可序列化的业务数据。回调使用官方 `ISqlSugarClient` 的查询、参数化 ADO 和写入。UniversalData 的 C 驱动持有连接和发送语句，回调结束时归还连接。取得物理连接、厂商命令以及从事务分支独立提交均返回 `Unsupported`。

```csharp
await using var data = DataClient.CreateBuilder(policyFile, "accounting")
    .AddSqlite("orders", "Data Source=" + Path.GetFullPath("orders.db"))
    .AddModule(typeof(OrderJobs).Assembly)
    .Build();

var order = await data.RunAsync(OrderJobs.FindAsync, orderId, cancellationToken);

public static class OrderJobs
{
    public static Task<Order> FindAsync(DataContext ctx, long id, CancellationToken cancellation) =>
        ctx.Database("orders").UseAsync((db, token) =>
        {
            token.ThrowIfCancellationRequested();
            return db.Queryable<Order>().Where(order => order.Id == id).FirstAsync();
        }, cancellation);
}
```

`OpenSessionAsync` 可以在同一 Worker 租约内依次运行多个调用。每次调用取得当时的数据源修订快照。通过 `Sources` 查询、替换或探测数据源。替换前已借出的连接可以完成当前作用域，新的借用要求当前修订。`ParallelAsync` 在一次调用内按 Worker 分支配额并行工作。

`ConfigureWorkers(w => w.MaxSessionsPerWorker(2))` 允许主机策略认可的模块在同一 PID 中运行多个协作式会话。各会话拥有独立上下文和取消，进程级崩溃会同时中断该 PID 上的会话。默认每个进程同时承载一个会话。主机可把健康的受信 Worker 分配给后续会话。`OpenIsolatedSessionAsync` 为需要独立终止的调用分配专用 Worker。

同一节点数据目录下，SDK 和 Management 对每个租户的同一关系数据资源共用 Native 物理会话限额。主机策略 `maxConnectionsPerSource` 默认为 8，使用同一节点目录的宿主需使用相同值。跨宿主为同一数据库显式指定同一资源标识，可使不同连接名计入同一限额。SDK 的 `ConfigureWorkers(w => w.MaxConnectionsPerSource(2))` 限定一次业务调用可同时租用的该源连接数。

单库事务通过 `ctx.Database(name).TransactionAsync` 声明。具备 Native 分布式事务能力的数据库通过 `ctx.Atomic(first, second).RunAsync` 使用同一事务协调器。事务作用域内，所选数据库的 SQL 写入使用协调器持有的连接。遇到 `CommitUnknown` 时保存 `DataExecutionException.Reference`，用 `DataClient.RecoverAsync` 查询并推进 Native 日志中已经持久化的事务决定。

跨进程业务异常以 `DataBusinessException` 返回。它保留 Worker 中的异常类型名称、消息和执行引用。数据库和执行故障通过 `UniversalDataException` 的状态和诊断字段返回。对 MySQL 和 MariaDB，事务写入需要可确认的 InnoDB 目标表及用于验证触发器的直接授权。跨库事务默认使用已预置的回执表。允许预置回执的所有参与者需显式配置 `ProvisionTransactionReceipts` 并具有相应的策略权限。

## 原生 ETL 批次

`FromTable<T>` 从 Native 连接发现物理列，`FromSql<T>` 使用实体列定义和可选的参数批次。 `Map` 在 Native 流中调用已注册的公开静态方法。 `Append` 按批追加，`UpsertBy` 通过声明的键匹配目标。 `PreviewAsync` 从表源读取有界样本并返回原生预览。映射和目标写入在正式执行时运行。任意 SQL 源预览返回 `Unsupported`。 `StartAsync` 提供执行标识、进度和诊断。 `RunAsync` 等待最终指标。

```csharp
var transfer = data.Etl("order-archive")
    .FromTable<Order>("orders")
    .Stream()
    .BatchSize(4096)
    .Map(OrderArchive.Normalize)
    .ToTable("archive", "orders")
    .Append();

var metrics = await transfer.RunAsync(cancellationToken);
Console.WriteLine(metrics.CommittedRows);
```

默认快照可以通过 Native Journal 恢复。 `Stream()` 直接从数据库游标读取，适用于可重新读取的源或由业务处理重跑的任务。直接流式执行的 `ResumeAsync` 返回 `Unsupported`。使用 `EtlRun.CancelAsync` 请求停止执行。取消 `WaitAsync` 的令牌只取消调用方的等待。

`FromChanges<T>` 使用 Native CDC，先读取快照，再续读事务日志。传入明确的 `EtlChangeOptions.Position` 时从该位点续读。MySQL 与 MariaDB 需要指定唯一的复制服务编号 `ServerId`。输入的前后镜像和删除由同一 Native changeset 规则交给 `ToTable(...).UpsertBy(...)`。

```csharp
var occurrence = await data.Etl("orders-live")
    .FromChanges<Order>("orders", "orders", new(Slot: "orders_sub", Publication: "orders_pub"))
    .ToTable("serving", "orders")
    .UpsertBy(row => row.Id)
    .SubmitAsync(management, requestId, cancellationToken);
```

持续作业通过 `SubmitAsync` 发布版本、保存作业并使用 `requestId` 启动。数据源使用已有 Management 凭据引用。应用关闭后由 Management 持有执行。使用作业记录里的 `RunId` 调用 `ExecutionAsync`、`StopExecutionAsync` 或 `ResumeExecutionAsync`。

用 `AddIceberg(name, catalog, storageRoot, warehouse, credentialReference)` 注册 REST Catalog 和文件权限范围。`ToIceberg(target, tableNamespace, table)` 将数据提交为 Iceberg v2 表的新快照。全量数据可调用 `Append()`，声明主键后可用 `UpsertBy(...)` 接收 CDC 的插入、更新和删除。`FromIceberg<T>(source, tableNamespace, table, snapshotId)` 按指定快照读取映射的列，省略 `snapshotId` 时读取当前快照。

读取开始时固定快照，恢复后继续读取同一快照。提交恢复使用 Catalog 快照中的执行标识和数据摘要。当 Catalog 无法确认已经发出的提交时，作业保留待恢复状态。当前写入支持本地文件仓库和无分区表。

`eventWindow` 的 `emit: "changes"` 为带键的滚动和滑动窗口输出插入、更新和删除。输出包含 `_window_start`、`_window_end`、`_revision` 和 `before`。`count`、`sum`、`avg` 支持撤回和迟到修订，作业检查点保留窗口值与修订。使用 `beforeField` 指定输入的前镜像字段。

实体列使用官方 `SugarTable` 和 `SugarColumn`。对十进制金额声明实际精度和刻度，例如 `[SugarColumn(Length = 18, DecimalDigits = 2)]`。Native Batch 保留 SQL NULL 和数值精度。SQLite 使用 `TEXT` 存放需要完整保真的十进制值，转换时使用固定区域格式。

`examples/dotnet-data` 的 `--sql-etl` 示例读取并规范化 `example_orders`，然后批量写入目标的同名表。源和目标 JSON 文件指定 `dbType` 和 `connectionString`，还可指定 `resource`、`credentialReference` 和 `nativeConnectionString`。宿主策略授权这些资源，并允许执行示例程序集和相应的 Native 插件。策略的 `trustedAssemblyHashes` 可包含经主机核对的示例程序集 SHA-256。

```powershell
dotnet run --project examples/dotnet-data -- --sql-etl policy.json source.json target.json result.json
```

## Package API

使用 `new EtlClient(data)` 启动、预览、检查和恢复已构建的 Package。托管组件通过 `ManagedBatch.ToEntities<T>()` 读取当前批，使用 `ManagedComponentContext.CreateBatch(rows, output)` 生成输出。需要保留行血缘的转换在发出之前调用 `CopyRowIdentity`。

Package、Execution、Schema 的公共契约和线程所有权以 `include/ud` 为准。组件回调用户数据应存活至 Execution 销毁。 `UD_END` 表示游标已读完。详见 [公共 C ABI](architecture/public-c-abi.md) 和 [产品模型](product/product-model.md)。

# UniversalData C# SDK 8 / Native ABI 15

安装 `UniversalData` 8.0.0 会还原官方 `SqlSugarCore` 5.1.4.216 及同版本的 Windows x64 和 Linux x64 运行时包。应用通过 `DataClient` 注册数据源、业务程序集和 Worker 配额。数据源按名称解析，Worker 根据宿主策略检查资源和凭据。SQLite 使用绝对路径。远程资源可在添加数据源时用 `options.Resource` 对应策略中的显式授权。

## 业务调用与数据库事务

`RunAsync` 在独立 Worker 中调用已注册程序集的公开静态方法，输入和输出使用可序列化的业务数据。数据库连接在 `UseAsync` 回调结束时释放。回调使用官方 `ISqlSugarClient` 的查询、参数化 ADO 和普通写入。

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

`OpenSessionAsync` 可以在同一 Worker 租约内依次运行多个调用。每次调用取得当时的数据源修订快照。通过 `Sources` 查询、替换或探测数据源。 `ParallelAsync` 在一次调用内按 Worker 分支配额并行工作。

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

实体列使用官方 `SugarTable` 和 `SugarColumn`。对十进制金额声明实际精度和刻度，例如 `[SugarColumn(Length = 18, DecimalDigits = 2)]`。Native Batch 保留 SQL NULL 和数值精度。SQLite 使用 `TEXT` 存放需要完整保真的十进制值，转换时使用固定区域格式。

`examples/dotnet-data` 的 `--sql-etl` 示例读取并规范化 `example_orders`，然后批量写入目标的同名表。源和目标 JSON 文件指定 `dbType` 和 `connectionString`，还可指定 `resource`、`credentialReference` 和 `nativeConnectionString`。宿主策略授权这些资源，并允许执行示例程序集和相应的 Native 插件。策略的 `trustedAssemblyHashes` 可包含经主机核对的示例程序集 SHA-256。

```powershell
dotnet run --project examples/dotnet-data -- --sql-etl policy.json source.json target.json result.json
```

## Package API

使用 `new EtlClient(data)` 启动、预览、检查和恢复已构建的 Package。托管组件通过 `ManagedBatch.ToEntities<T>()` 读取当前批，使用 `ManagedComponentContext.CreateBatch(rows, output)` 生成输出。需要保留行血缘的转换在发出之前调用 `CopyRowIdentity`。

Package、Execution、Schema 的公共契约和线程所有权以 `include/ud` 为准。组件回调用户数据应存活至 Execution 销毁。 `UD_END` 表示游标已读完。详见 [公共 C ABI](architecture/public-c-abi.md) 和 [产品模型](product/product-model.md)。

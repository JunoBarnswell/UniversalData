# UniversalData SDK 6 / ABI 14

数据库访问由 C 负责。独立 .NET 调用方通过同一个 NuGet 包使用 SqlSugar 风格接口，需要安装所选数据库的 UniversalData 原生插件以及它依赖的数据库客户端。已安装的同 RID 插件可随应用发布。宿主内从已有的 `Execution` 打开逻辑连接，授权与凭据通过现有回调解析。

```csharp
using SqlSugar;

using var source = new SqlSugarClient(new ConnectionConfig
{
    DbType = DbType.Sqlite,
    ConnectionString = "Data Source=orders.db",
    IsAutoCloseConnection = true
});

foreach (var batch in source.Queryable<Order>()
    .Where(order => order.Id > 100)
    .OrderBy(order => order.Id)
    .ToBatches(512))
{
    Process(batch);
}
```

`Queryable<T>` 的条件、排序和分页在数据库中执行。用 `ToList()` 明确接收完整结果，用 `ToStream()` 或 `ToBatches()` 逐步拉取。释放枚举器会关闭 C 游标。`Ado` 接受参数化查询并提供命令、标量、读取器和本地事务。数据类型映射使用 `SugarTable`、`SugarColumn` 和实际游标列信息，SQL NULL 保持为空值。

`decimal` 默认按精度 38、28 位小数传递。可以用 `[SugarColumn(Length = 38, DecimalDigits = 0)]` 映射整数列。SQLite 的 `decimal` 目标列使用 `TEXT` 保留完整精度。数值超出可精确表示的范围时报告列名。

Oracle 的原始查询返回固定刻度的 `NUMBER` 时，读取器保留驱动提供的精度和刻度。驱动报告刻度可变时，读取器保留十进制文本，调用方可按业务类型转换。Oracle 的 `Ado.BeginTran()` 默认使用 `ReadCommitted`，也可通过重载显式选择隔离级别。

`Insertable`、`Updateable` 和 `Deleteable` 接收单个对象或一批对象。`Fastest<T>()` 使用相同的原生写入入口。`PageSize` 指定物理执行的分块上限，整个 SDK 批量调用仍属于一个事务。按键更新和删除使用实体声明的主键。显式事务中的批量失败要求回滚。驱动的 `CommitUnknown` 需要先核实数据库状态。

`examples/dotnet-data` 的 `--sql-etl` 示例从源读取 `example_orders`，去除 `Name` 两端的空白，再批量写入目标的同名表。为源和目标分别准备 JSON 配置文件，每个文件包含 `dbType`、`connectionString` 和 `pluginPath`。在已加载对应 ABI 14 原生库的环境中运行下列命令。

```powershell
dotnet run --project examples/dotnet-data -- --sql-etl source.json target.json result.json
```

托管数据流组件可用 `ManagedBatch.ToEntities<T>()` 读取当前批，用 `ManagedComponentContext.CreateBatch<T>(rows, output)` 生成已绑定输出端口的同结构 Batch。需要维持行血缘的转换在发出之前调用 `CopyRowIdentity`。SQL Destination 和 SQL Command 通过 C 执行写入。

## Package API

Package、Execution、Schema 的公共契约及线程所有权以 `include/ud` 为准。组件回调用户数据应存活到 Execution 销毁。`UD_END` 表示已到达游标末尾。详见 [公共 C ABI](architecture/public-c-abi.md) 与 [产品模型](product/product-model.md)。

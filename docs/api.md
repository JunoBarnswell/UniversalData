# UniversalData C# SDK 10 / Native ABI 15

安装 `UniversalData` 10.0.0 会还原对应平台的 Runtime、SqlSugarCore 5.1.4.216 和 YamlDotNet 16.3.0。C# 通过命名数据源、强类型输入和行类型声明作业。Worker 接收 Native v15 Package，由 C 内核访问外部系统并执行数据流、控制流和事务。

```csharp
var syncOrders = DataJob.Define<SyncOrders>("orders.sync")
    .FromHttp<Order>("sales", "/orders", step: "read-orders")
    .Where((order, input) => order.IsPaid && order.Id >= input.FromId, step: "paid")
    .ToTable("finance", "paid_orders")
    .UpsertBy(order => order.Id);

await using IDataClient data = DataClient.Create(args);
var result = await data.RunAsync(syncOrders, new SyncOrders(FromId: 1000), ct);
Console.WriteLine(result.CommittedRows);

public sealed record SyncOrders(long FromId);
public sealed record Order(long Id, bool IsPaid,
    [property: SugarColumn(Length = 18, DecimalDigits = 2)] decimal Amount, string? Note);
```

`Where`、`Select` 和键选择使用 C# 表达式树。SDK 在开发端将字段、参数、运算和支持的函数编译为 Native 规则。无法翻译的表达式会在提交前报告作业、步骤及对应方法或表达式。金额的精度、可空性和列名沿用 `SugarColumn`，表名沿用 `SugarTable`。SQLite 需要精确保留十进制值时，使用 `Select` 将金额转换成固定区域性的字符串，并写入 `TEXT` 目标列。

## 查询、管道与事务

`Return()` 声明类型化行流，`RunAsync` 返回 `IAsyncEnumerable<T>`。Worker 每次发送一个有界 Native 批次，SDK 按需投影为业务类型。使用 `await foreach` 读取。可重读的大型查询可在 `FromSql` 或 `FromTable` 上启用 `stream: true`。事务内查询和需要回执的目标使用默认快照读取。

```csharp
var find = DataJob.Define<SyncOrders>("orders.find");
var read = find.FromSql<Order>("finance",
    "SELECT Id, IsPaid, Amount, Note FROM paid_orders WHERE Id >= ?1",
    find.BindParameters(input => input.FromId), stream: true).Return();

await foreach (var order in data.RunAsync(read, new SyncOrders(1000), ct))
    Console.WriteLine(order.Id);
```

SQL 占位符使用目标驱动的语法。`BindParameters` 将作业输入绑定到 Native SQL 参数批次。控制步骤按声明顺序运行。`Parallel` 声明可并行的控制分支，由 Native 根据作业配额运行。命名步骤可按前序成功、失败或结束触发。`Split`、`Fork`、`UnionAll` 和 `Join` 在同一管道中分流、复制、汇合和按键关联。

`Transaction("finance")` 声明单库事务。多库事务通过 `Transaction("orders", "finance")` 声明参与者，由 Native 协调器检查驱动能力并持有参与者连接。SQLite 单库事务使用 `serializable`，其他数据库使用 `readCommitted`。配置 XA 的数据库应具备 Native 文档所列的事务和回执权限。执行状态为 `CommitUnknown` 时保存 `DataExecutionException.Reference`，使用原作业、输入和引用调用 `DataClient.ResumeAsync`。恢复按 Native Journal 的已持久化决定推进。

`StartAsync` 返回 `EtlRun`，可读取进度、诊断和执行指标。`CancelAsync` 停止执行。取消 `WaitAsync` 的令牌会结束调用方等待。直接流式读取使用当前数据库游标，带持久边界的作业按检查点恢复。

`FromMessages` 和 `ToMessages` 使用 Native Kafka、RabbitMQ、RocketMQ 5、ActiveMQ Classic 和 ActiveMQ Artemis。`FromMessages<TRow>` 读取 `TRow` 声明的消息体字段，对应原生 `mq.source` 的 `includeMetadata: false`。通用消息作业至少交付一次，目标提交后才确认源消息。Kafka 到 Kafka 可声明 `KafkaTransaction(checkpointTopic)`，将输出、位点和检查点一起提交。HTTP 使用 `FromHttp`、`Request` 和 `ToHttp`。`FromHttp` 读取 JSON 数组或带 `rows` 和 `nextCursor` 的分页对象，作业也可指定分页字段名。

消息地址的读写权限按资源和地址配置，例如 `broker-resource/queue/orders` 或 `broker-resource/topic/orders`。消费组提交位点需要 `broker-resource/group/<group>` 的写权限。无消费组的队列使用队列的写权限确认消息。

预览包含消息源的路径时，Native 绑定端口和字段结构，返回 `sampled: false` 与零行样本。代理连接、订阅、消费和确认均发生在正式执行阶段。

## 配置、发布与资源配额

默认配置文件为 `universaldata.yaml`，也可使用 `universaldata.yml` 或通过 `--config` 指定路径。无参数 C# 客户端可以用 `DataClient.CreateBuilder(policyFile, tenant)` 显式注册数据源。

```yaml
hostPolicy:
  path: host-policy.json
  tenant: finance
sources:
  sales:
    kind: http
    baseUri: https://sales.example.com
  finance:
    kind: sqlite
    resource: /srv/universaldata/finance.db
    credentialReference: finance-db
assemblies:
  - ./publish/FinanceJobs.dll
```

C# 作业可声明为开发程序集中的公开静态字段或属性，也可在 YAML 的 `jobs` 中直接声明 v15 Package 的命名步骤、连接、输入和规则。C# 作业可在 `jobs` 的同名条目配置默认参数、执行选项和日程。用 `ud_cli export` 查看可部署的 Native 定义。同名 C# 与 YAML 业务定义会导致启动前报错。节点通过已授权的本机插件和宿主凭据执行导出的规则包。

```powershell
ud_cli export orders.sync --config erp.yaml
ud_cli run orders.sync --config erp.yaml --params-file today.yaml
ud_cli publish orders.sync --config erp.yaml --run --request-id orders-2026-11-21 --expected-revision 0
```

参数依次覆盖作业默认值、参数文件、环境变量、显式命令行参数和本次 C# 输入。环境变量可用 `UD_PARAMETERS_ORDERSSYNC` 提供 JSON 对象，命令行可用 `--set FromId=1000`。发布使用宿主凭据引用。已认证客户端通过一次 `POST /api/publications` 提交不可变 Package、预期修订、调度、参数和可选首跑。重试时复用请求号。确认状态时使用回执中的执行编号和事件订阅。

作业可通过 `JobExecutionOptions` 或 YAML 的 `execution` 配置 `batchRows`、`edgeCapacity`、`parallelism`、`memoryBytes`、`timeoutMilliseconds`、`maxConnectionsPerSource` 和 `messagePrefetch`。Native 运行进度报告实际选项、批次数和队列高水位。宿主策略决定授权和资源上限。`ConfigureWorkers` 管理本地会话配额，`MaxSessionsPerWorker` 控制同一 Worker 进程可并发承载几个单槽作业。

## CDC、Iceberg 与已构建 Package

`DataClient.Etl` 提供表发现、Native 预览、CDC 和 Iceberg 的链式定义。`Select` 编译列投影为 Native 表达式。`FromChanges<T>` 先读取快照，再读取事务日志，指定 `EtlChangeOptions.Position` 可以从所给位点续读。MySQL 与 MariaDB 需要唯一的 `ServerId`。Native changeset 保留前后镜像和删除。

```csharp
var occurrence = await data.Etl("orders-live")
    .FromChanges<Order>("orders", "orders", new(Slot: "orders_sub", Publication: "orders_pub"))
    .ToTable("serving", "orders")
    .UpsertBy(row => row.Id)
    .SubmitAsync(management, requestId, ct);
```

`SubmitAsync` 使用同一个原子发布接口。Native Catalog 持有调度和首跑状态，应用结束后 Worker 仍可按计划执行。可以通过 `ExecutionAsync`、`StopExecutionAsync` 或 `ResumeExecutionAsync` 操作返回的 `RunId`。

`AddIceberg` 注册 REST Catalog 和文件范围。`FromIceberg<T>` 可按指定快照读取。`ToIceberg` 将数据提交为 Iceberg v2 新快照，支持按键应用 changeset。恢复时按执行标识与数据摘要核对已提交快照。`eventWindow` 支持输出带键的窗口插入、更新和删除，检查点保留窗口状态及修订。

使用 `EtlClient` 执行、检查、预览和恢复现有 Native Package，使用 `TestMessageConnectionAsync` 验证已登记的消息连接。Package 的生命周期和线程所有权见 [公共 C ABI](architecture/public-c-abi.md)。

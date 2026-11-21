# UniversalData 8.0 C# 链式 SDK 验收

2026-11-20，Linux PRODUCT Release。C# 消费程序集使用官方 SqlSugarCore 5.1.4.216。链式 ETL 通过 Worker 进程执行 Native Package。

## ETL 数据规模

SQLite 测试经 `DataClient.Etl()` 读取、转换并追加 10,000,000 行。Native 报告读入和提交各 10,000,000 行，独立会话按键回读同样的行数并验证有序校验和 `82da80c54b8880a5`。每批最多 4,096 行，运行 100.156 秒，约 99,844 行/秒。客户端累计托管分配 3,547,328 字节，采样峰值 RSS 为 118.3 MiB，Worker 为 130.1 MiB，Native 队列峰值为 2 批。SQLite 将精确小数列存为不依赖区域设置的文本。

MariaDB 11.8 实库通过 1,003 行链式映射与按键写入。表源预览验证目标零写入，提交后用官方 SqlSugar 逐行逐字段独立回读。另在 SQLite 上验证取消等待后可以继续取得 256 行提交结果，显式取消执行会返回 Native 取消结果和原 Journal 标识。

## Native 回归

- PostgreSQL 17 使用验证 TLS 的快照和 WAL，SQL Server 使用验证 TLS 的 ODBC 18 和原生 CDC，MySQL 8.4 和 MariaDB 11.8 使用 ROW/FULL binlog，Oracle 使用验证 TCPS、归档和全列补充日志。五组均核对初始快照、增删改、变更前镜像、断点续读和授权撤销。
- PostgreSQL CDC 写入 SQLite 后在目标提交点强制终止进程，从原执行恢复并独立回读 100 行。C# 托管源、转换和目标另通过 Native epoch 进程终止、同执行编号恢复和稳定行标识核对。
- Native XA 在 MySQL、MariaDB、PostgreSQL、SQL Server、Oracle 的 15 种双参与者组合及 PostgreSQL/MariaDB/SQL Server 三参与者组合实测通过。每组覆盖 prepare 后退出、提交决定后的恢复以及轮换首个已提交库，均由独立连接回读。
- 使用真实 Microsoft DTExec 17.0.1000.7 的 Windows 基线重跑 Native，50,000 行的 100,000 项类型化观测全部一致。 [Microsoft 捕获任务](https://github.com/JunoBarnswell/UniversalData/actions/runs/44689502092)
- MySQL 的 50,000 行 Native CSV 流程通过两种协调器提交点强制退出和同编号恢复。每组独立核对 49,950 条业务记录与 50 条错误记录，逐行签名一致。

## C# Worker 和安装

会话测试通过两个执行进程同时进入 SQL/HTTP 分支、长会话复用进程并刷新源修订、旧上下文失效、业务异常和取消、超时强制退出及名额回收。SQLite 原子事务校验在业务回调执行前返回不支持。官方 SqlSugar 的 SQLite 用例另覆盖复合键、NULL、投影分页、Native 批追加/更新和读取器失效。

SDK 的 15 种双库和一组三库事务在实库全部通过，逐组验证业务提交、确认回执、异常回滚、prepare 前强制退出及原编号恢复。在另一组 PostgreSQL/SQL Server 混合事务中，Native Journal 的提交决定同步到磁盘后强制退出 Worker。恢复前独立 PostgreSQL 查询仍读到旧数据。随后用同执行引用恢复，两库独立回读均确认写入，持久化的业务调用记录仍只有一次。

慢会话测试使用四个进程名额和两个持续发送快请求的会话，实测每个 Worker 的 cgroup 限额为 1 CPU、512 MiB。普通基线，以及加入 HTTP 阻塞、数据库锁等待、CPU 密集 Worker 后，每轮分别完成 1,000 个快请求，均无超时。

| 场景 | SDK 提交至返回 P95 | 基线倍数 | 计划发压至返回 P95 |
| --- | ---: | ---: | ---: |
| 基线 | 3.099 ms | 1.000 | 359.157 ms |
| 慢 HTTP | 2.760 ms | 0.891 | 431.481 ms |
| 慢 SQL | 1.919 ms | 0.619 | 3.762 ms |
| CPU 密集 | 2.229 ms | 0.719 | 372.200 ms |

SDK 提交至返回包含调用过程中的 Worker 排队、连接等待和实际查询。发压进程在提交前出现了数百毫秒调度延迟。初次按计划发压时刻计时，慢 HTTP 和慢 SQL 两组超过 1.10 倍门槛，上表保留同一口径的复测数据。

2026-11-21 的 Linux 复核定位到独占 Worker 收帧后约 881 ms 的派发停顿。父进程监视移出请求线程池。节点在启动前分配并核对逻辑 CPU，两个快 Worker 和慢 Worker 分别使用 CPU 0、1、2。连接租用复用已找到的驱动路径，并按当前源修订校验借用。

复测以约 166.7 请求/秒运行。基线和各慢场景正序、倒序各采集 1,000 个快请求，逐请求合并计算 P95，保留每个窗口的结果。连续三轮中，独占与共享均通过两项 1.10 倍门槛。独占 Worker 的三轮最大比值如下。

| 慢场景 | 计划发出至返回 P95 | SDK 提交至返回 P95 |
| --- | ---: | ---: |
| HTTP 阻塞 | 1.0026 | 1.0187 |
| SQL 锁等待 | 0.9766 | 0.9892 |
| CPU 密集 | 1.0988 | 1.0045 |

共享 Worker 的最高比值为 0.6691 和 0.7513。独占慢 CPU 的单个约 6 秒窗口最高为 1.1025 倍，同场景合并 2,000 个请求后的最高值为 1.0988 倍。 [验收代码](../../../../tests/sdk_fluent/LatencyCase.cs)

Linux x64 从本地包源和空 NuGet 包/HTTP 缓存离线安装，使用 .NET 8.0.31，按还原锁文件编译为零警告、零错误。真实 Worker 完成官方 SqlSugar SQLite 写入与回读及 HTTP 200 请求。使用已发行的 v7 Worker 启动 v8 SDK 返回 `Incompatible`。

Linux PRODUCT 的 93 项便携回归中 90 项通过。两个跳过项提供独立 PostgreSQL 后通过，包括两次 Management 接管及 Catalog 迁移。该次便携运行缺少 Kafka 弹性测试拓扑。浏览器验证通过 SQL 节点重载、200/500/1,000 节点画布、断点、Worker 退出和执行恢复。 [Kafka 千万条数据及扩缩验收](../worker-cluster-ha/README.md)

[SDK 使用方式](../../../api.md) · [千万行测试入口](../../../../tests/managed_sql/README.md)

## UniversalData 11 性能复核

本次使用 Linux PRODUCT Release 和真实 cgroup Worker 配额。每组慢负载与基线分别采集 2,000 个请求，同时检查 SDK 提交至返回和计划发出至返回的 P95。采样保留原有 1.10 倍门槛。事件驱动的终态等待和控制帧合并后，独占 Worker 的慢 HTTP 场景三轮均通过。独占 Worker 最差的计划发出至返回比值为 1.204，共享 Worker 为 3.180，因此整体门槛仍未通过。记录同时包含计划发压、等待上一条请求和可提交后的调度延时。 [实测记录](worker-performance-v11.json)

SQLite ETL 每批 4,096 行，写入和独立回读各 10,000,000 行，校验值为 `82da80c54b8880a5`。本轮吞吐为 98,499 行/秒，Worker 峰值 RSS 为 119,463,936 字节，客户端峰值 RSS 为 130,318,336 字节。 [实测记录](worker-performance-v11.json)

RabbitMQ 复核用三个实际 Worker 清空 900,000 条积压。每个 Worker 的峰值在途数为 16,384 条。每条模拟业务等待至少五秒，由 Native 在目标提交后确认源消息。独立回读验证 900,000 个唯一 ID，剩余队列深度为零。 [复现步骤](../../../../tests/message_brokers.md#rabbitmq-突发积压-demo) [实测记录](mq-burst-v11.json)

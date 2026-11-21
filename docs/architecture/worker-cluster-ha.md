# Worker 自主管理与集群接管

Management 在单机模式用本机 Worker 处理执行与检查。集群中的执行交给已注册的 Agent，Management 保留本机进程用于检查和 Kafka 位点采样。两种入口使用同一个 Hosting 监督器。普通 Worker 在初始化 CLR 与 Native 后通过继承的管道报告 PID，每个进程领用一份任务。监督器通过同一条继承管道发送开始、停止和取消命令。隔离任务和专用 CPU、内存预算按任务创建进程。Windows 使用带关闭终止标志的 Job Object，Linux 的专用配额和隔离任务使用 systemd 用户管理器与 cgroup v2。

SDK 在 Linux 启用资源隔离时，Worker 从同一个节点预算预约逻辑 CPU。启动器在 CLR 初始化前设置并核对 CPU 亲和性和 cgroup 限额。独占 Worker 分配一个逻辑 CPU，共享 Worker 按所预约的 CPU 槽数分配。

默认预热一个普通进程。进程在启动、执行和排空各阶段计入配额，确认退出后归还配额。空闲进程被异常终止后以 1、2、4 秒递增退避，最长 30 秒。更新监督器配置时用文件替换写入，管理器验证新配置再切换代号。空闲额度允许时先等新进程就绪，容量吃紧时先释放空闲旧进程。守护者离开或许可耗尽后，Worker 请求原生停止，在清理期限内仍存活时终止进程。

## Catalog 与接管

单机模式使用 SQLite。集群使用 PostgreSQL 17 的可写服务地址。Catalog 共享文档校验、摘要、修订、条件更新、作业触发和租约逻辑。PostgreSQL 事务先锁定 Catalog 时钟，再校验数据库时间上的 Management 任期与执行租约。同步提交确认后才把开始许可传给 Agent。运行记录、原生执行事实和日程游标沿用 Catalog 的版本事务。队列从当前状态索引读取有限页，并在租户之间轮转。

参考部署使用一台 Windows Management、一台 Linux Management、两台以上 Worker，以及通过 Patroni 和多数仲裁管理的三个 PostgreSQL 17 实例。参考 [Patroni 配置](../../examples/worker-cluster/patroni-config.yml)启用严格同步模式和数据库旧主隔离，在 Management HTTPS 前使用按 `/health/ready` 探测的稳定入口。连接串文件指向可写端点，生产环境配置服务账号、TLS 校验和库级权限。两个 Management 实例共用一份 32 字节 Base64 加密密钥，并各自持有唯一的管理节点编号。管理服务就绪要求取得 12 秒任期并能读取集群身份。任期每 3 秒续约，续约结果无法确认时撤销本机就绪状态。

Agent 配置所有候选 Management 地址与各证书的 SHA-256 指纹。证书仍需通过系统信任链和主机名校验。Agent 每 5 秒发送心跳，10 秒后节点停止承接新任务，20 秒后判为离线。执行许可根据 Agent 发出续租询问时的单调时钟计算，最长 20 秒。连接中断时，Agent 在许可内保留原 Worker 和已持久化的报告。新主通过 Agent 启动身份、任务代号、PID 与已接收的报告序号接续，先重新校验原执行者权限。已授权且外部提交无法核实的任务进入 `RecoveryRequired`。能用 Kafka 截点恢复的任务走 Broker 校验，其他任务从仍可读取的原日记恢复。

## Kafka 分区

在执行或作业的 `elastic` 属性设置进程边界。支持一个直接 Data Flow 的单源 Kafka 事务作业。已发布包修订和受保护参数引用固定在同一执行编号下。原生 Kafka 连接器读取分区和起始位点，采样使用连接、Broker 地址、源 Topic 和消费组的授权。每个分区对应一个 Native 消费通道和一个事务检查点分区。检查点 Topic 的分区数量应覆盖 `checkpointPartition + Source 分区编号`。原生绑定拒绝全局阻塞算子，并校验有状态算子的检查点和恢复能力。

默认每 5 秒采样，目标为每进程 10,000 条积压，连续 3 次超出阈值才增加一个进程。积压低于缩容界限 120 秒后减少一个进程，任意两次调整相隔至少 30 秒。Catalog 先保存新分组，再请求受影响的旧分组停止读取并完成事务边界。新 Worker 使用原事务标识隔离旧生产者，确认 Broker 检查点和已提交位点后继续。分区缺失、位点矛盾和采样失败时可以在执行详情查看原因。

## 迁移

先停止接单，完成排空并保留 SQLite、受保护参数、Cookie 密钥和节点日记的备份。对每个 Management 集群使用独立的专用数据库。使用与当前源码同版的 CLI 依次运行以下命令。

```text
ud_cli catalog migrate check  POLICY_FILE POSTGRESQL_CONNECTION_FILE ENCRYPTION_KEY_FILE
ud_cli catalog migrate import POLICY_FILE POSTGRESQL_CONNECTION_FILE ENCRYPTION_KEY_FILE
ud_cli catalog migrate verify POLICY_FILE POSTGRESQL_CONNECTION_FILE ENCRYPTION_KEY_FILE
```

核对三个命令的租户文档数量和快照摘要。重复导入同一快照应得到同一结果。迁移保留文档修订、记录的日记归属与摘要，原节点日记文件继续由该 Agent 管理。迁移命令加密保存 Cookie 密钥、身份版本和参数引用对应的材料。第一笔集群写入前可使用备份回到 SQLite。开始写入后使用 PostgreSQL 的备份和故障恢复流程。

## 验收资源

| 测试 | 资源 |
|---|---|
| `management_worker_autoscale` | Windows 或 Linux 的 Host 构建，两个 Worker 并行写出一千万行 CSV，核对行数和 SHA-256 |
| `management_sentinel_ha` | `UD_TEST_SENTINEL_POSTGRESQL_CONNECTION_FILE` 指向空的专用 PostgreSQL 17 数据库。测试在 Linux 上启动两个 Management 进程和两个 mTLS Agent，检查任务接续和 Cookie |
| `management_catalog_migration` | `UD_TEST_CATALOG_POSTGRESQL_CONNECTION_FILE` 指向另一空的专用 PostgreSQL 17 数据库 |
| `management_kafka_elastic` | `UD_TEST_CLUSTER_KAFKA_ELASTIC_CONFIG` 提供 `managementUrl`、`identity`、`broker`、`package`、`sourceTopic`、`targetTopic` 和 `consumerGroup`，执行环境需要 Python `confluent-kafka`。输入和输出 Topic 专供这一次测试使用，默认验证一千万条 |

Windows 和 Linux 的双向接管、单向网络故障、数据库多数派丢失，以及事务交接期间的进程故障，需要在实际部署拓扑记录。应用接单恢复目标为 20 秒，数据库主切换目标为 60 秒。

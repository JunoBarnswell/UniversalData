# 统一事务、检查点与恢复

C17 Execution、Connection Manager 和现有 Journal 是唯一执行与恢复所有者。Catalog 保存管理记录，不推断事务事实。旧协议与旧恢复状态明确拒绝，旧文件和数据库回执代次保留。

`transaction: {mode: "required", participants: ["business", "errors"], scope: "task", provisionReceipts: true}` 声明完整参与者。Manager 在业务副作用前检查所有连接的真实参与能力；普通本地事务能力不等于可恢复参与者能力。缺少 Native Journal 或任一参与者不支持时拒绝开始。

Task 范围整体提交。Data Flow 可显式选择 `scope: "epoch"`：每个屏障准备全部分支，先持久化共同决定，再解析分支结果，发布该 Epoch 的暂存事实，然后允许源确认，并开始下一分支。Task 范围中的 CDC 在绑定时拒绝，避免等待源确认与整体提交相互阻塞。

CDC 源可配置 `microBatchRows`、`microBatchMs`。两项省略时每笔源事务发出一个屏障。配置后以第一个读到的完整提交开始计时，达到行数或时间阈值后在完整源事务边界封存。封存记录保留批内源事务的顺序、各提交后的水位以及最后一个日志位置。`maxTransactions` 仍按源事务计数。

SQL 目标按输入批检查初始前像并记录收据。需要整个 epoch 在目标上原子提交时，由 Data Flow 的 `transaction.scope = "epoch"` 协调。源在目标确认和 checkpoint 持久化后推进日志位置。提交后中断时，恢复流程重放已封存数据，核对已有收据，并确认最后一个完整事务的位置。

PostgreSQL 使用 PREPARE TRANSACTION 并检查 max_prepared_transactions。MySQL/MariaDB 使用 XA START/END/PREPARE/COMMIT/ROLLBACK。Oracle 通过 OCI 使用 DBMS_XA 完成分支生命周期，将 64 个十六进制字符组成的 Native 分支 ID 编码为 32 字节 XID。Native 分支 ID、配置指纹和同事务的分支回执证明提交。缺少可验证的分支回执时，提交结果保持不确定。SQL Server/ODBC 需要通过驱动的 XA 能力探测才能成为参与者。

Task Done、批次 Done、游标、枚举器和 Epoch 在未决事务内先存为有序暂存事实。共同提交后才发布；回滚仅退役暂存事实。持久 COMMIT 决定后禁止改为回滚。恢复没有决定的分支时先持久化回滚决定；存在决定时按决定逐个恢复，即使一个资源不可用，也尝试其他参与者。任何无法证明的提交进入 RecoveryRequired，停止源确认和自动重放。

Journal 按固定上限滚动日志段，累计的日志大小达到前一快照的规模后再压缩。恢复先在原 Journal 的单写者锁内核验并持久确认日志链及尾锚点，随后才能按决议解析分支。原日志缺失、损坏或不匹配时保持 `RecoveryRequired` 并保留原始原因。运行使用可持久挂载的原卷。跨节点恢复能力取决于能否取回同一权威 Journal。

文件写入先同步暂存文件，再记录 PREPARED。新发布和恢复都在目标的 generation 锁内确认文件和目录持久性。恢复看到相同哈希时仍需证明持久化。证明失败返回 `CommitUnknown`，完成事实与输入确认等待后续恢复。Windows 使用受支持的本机 NTFS 发布和目录同步语义。

SQL 目标的记录错误在真实事务保护下使用 savepoint 隔离。先尝试批量写入，记录错误回滚该尝试后使用原生参数绑定逐行处理。正常目标和错误目标均须属于声明参与者。转换、截断和业务约束可重定向；连接中断、取消、超时、语句/Schema 执行故障以及提交不明保持失败。原始输入只有明确启用才保留。

REST 统一声明 `delivery: "response" | "idempotent" | "receipt"`。普通响应模式只发送一次；未决发送意图不能自动重放。幂等模式使用 Native 请求身份；回执模式验证 digest 和源位置，并可查询远端回执。增量重叠根据已绑定的 KEYED_REPLACE 能力与键 lineage 判断，不依赖 SQL 组件名称。

## 交付语义

- 单 SQL 目标在同一事务提交目标行与回执。SQLite 业务连接启用并读回 WAL、FULL 同步与外键约束。业务键、金额精度、FK、CHECK 和跨行不变量由写入配置与目标数据库负责。显式隔离级别由 Native 核对，能力不足时拒绝绑定。多 SQL 的共同决议允许各目标在不同时间变得可见。
- Kafka 数据流声明 `delivery.mode=kafkaTransaction` 后，合格的同集群链路在同一 Broker 事务提交输出、状态和消费位点。消费端用 `read_committed`，部署按该模式准备压缩检查点 Topic 及复制策略。
- `delivery.mode=eventReceipt` 支持从一个 RabbitMQ 队列直接写入一个具备事务回执能力的 SQL 目标。消息必须提供稳定且在队列内唯一的 ID。Native 基于队列及其逻辑 Broker 资源标识生成事件键，将完整规范化输入和绑定配置纳入摘要。重投可以改变批次和执行 ID。SQL 回执与业务行同事务写入，同键同摘要返回已确认结果，同键异摘要停止且保留源消息。源确认发生在目标提交和 Native 检查点确认之后。
- `eventReceipt` 切换前先排空或对账原队列。部署为 Broker 资源使用稳定且能区分 vhost 的标识，保留目标路由身份与回执。一个事件回执对应这个目标库中的副作用。数据库触发器发起的跨资源调用属于独立交付。
- 其他 MQ、HTTP 和文件组合按各自声明的确认、幂等或回执能力执行。业务行与发送意图可在同一个 SQL 事务中写入业务表和 outbox，再由独立消费者按稳定事件 ID 交付。整个异构链路可能在提交和确认之间重投。

PostgreSQL COPY 与受控 MySQL/MariaDB LOCAL INFILE 在同一业务写入/回执路径内。SQL Server BCP 与 OCI Direct Path 只装载受控暂存区；正式业务写入和回执仍由原事务拥有，暂存区生命周期延续至事务结果确定。Direct Path 的独立提交不能被描述为可随业务事务回滚。

以上是当前源码契约，尚未进行 ABI 10 编译或行为验收。注册的 transaction_fact_publication 和真实数据库 participant_recovery 测试覆盖暂存事实发布，以及 prepare 后退出、部分提交后退出的恢复边界；它们当前不是通过证据。SQL Source 错误隔离、在线 Schema 重绑定、完整 Epoch/指标恢复与 Windows 驱动验收仍按 metadata/product.json 追踪。

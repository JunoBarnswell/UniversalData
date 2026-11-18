# 四引擎目标架构

唯一链路：不可变定义 → 纯语义绑定 → Control Plan / Physical Pipeline Plan → Root Execution / Activation → Shared Scheduler / Compute / Buffer / Connection Lease / Journal → Events / Metrics / Catalog → C ABI / PInvoke / JNI。Catalog 与语言层不执行业务算法。

## 所有权与调度

Root Execution 拥有 Registry 租用、常驻 Worker Pool、根 Context 配额、Buffer Manager、Connection Manager、Journal 和诊断。任务、树和分区提交同一个 Work Group 契约。根调用线程参与执行，额外 worker 最多预算减一；嵌套 wait 在原生调用栈上推进自己的子工作，不创建线程，也不抢占无关祖先任务。所有回调退出后 group 才可销毁。

Control Binder 形成索引化前后继边、入度和规范化约束模式。前驱完成仅更新其出边，入度为零时评估约束并入 ready queue；Skipped 沿相同依赖图传播。不存在 task_wave。并行作用域禁止祖先写；绑定与执行使用相同保守并行策略。事务作用域独占声明的单一物理 Session，不能把不支持的资源组合描述成 ACID。

Pipeline Planner 形成真实树、树内拓扑、跨树 Path、Buffer/队列容量、输入/容量/End 等待原因及 ordering/state/concurrency 计划。树同一时刻仅有一个控制者。push 唤醒消费者，pop 唤醒生产者，End 唤醒后继；运行中事件置 pending wake，防止回调返回 Busy 时丢唤醒。禁止任意进度导致全体等待树重扫。

Compute 持有唯一 Sort/Join/Aggregate/Lookup/Window 算法。分区调度调用同一根 pool；结果合并遵循 canonical compare/hash/NULL/decimal 和显式排序规则。全局排序稳定归并，不因 worker 完成顺序改变相等键顺序。不可恢复 source/driver 明确拒绝恢复计划。

## Buffer、连接、恢复

Data 是 Schema、Value 和存储清单的唯一所有者。Buffer Manager 按真实分配去重计量封存批次。Path 槽位覆盖发布、排队和消费，消费方释放视图和 block 后归还槽位。短时交换优先使用现有 working 配额的四分之一，余量供算子状态、读取视图和编解码使用。长时算子状态从 resident 配额驻留。驻留不足的 block 使用同一个 Wire 格式和 spill 配额。运行诊断中的路径 `spillBytes` 记录交换写量，执行级 `totalSpill` 汇总交换和算子状态等全部写量。

Grace Hash Join 逐分区构建索引和输出。索引行定位到原分区页和行号。建表时解码的右页由当前分区的 Working 子域保留，探测及右侧未匹配行复用这些页。子域上限为 Working 的四分之一。额度不足时释放保留视图，由状态 reader 继续逐页访问。`hashStateIo` 记录左右分区页的写入量，以及左侧扫描、右侧建索引、探测和收尾的读取量。Buffer Manager 临时文件压紧值的缺失和空标志，公共 Wire 与持久检查点保留原布局。

## 消息源、目标与执行成员

`mq.source` 和 `mq.destination` 通过 Connection Manager 调原生协议驱动。ActiveMQ Classic 与 Artemis 使用 AMQP 1.0，RabbitMQ 使用 AMQP 0-9-1，RocketMQ 5 使用官方 v2 gRPC，Kafka 使用 librdkafka。包中配置连接引用、`targetType`、`address` 和目标所需的 `routingKey`。消费端可按协议选择 `group`、`subscription`、`filter`、`initialPosition`。Kafka 单消费者可用 `endOffsets` 声明逐分区的排他结束位置。所有生产队列、主题、绑定及消费组由宿主预置。

消息体可用 `binary`、`json` 或 Kafka `avro`。源的 `_message` 结构保留地址、标识、键、Header、分区、位点、时间及重投标志。目标可发布键和 Header。Avro 通过授权 Registry 连接访问已注册的 `schemaSubject` 和 `schemaVersion`，以已注册 reader schema 读取，以固定 writer schema 发布。源数据错误使用现有错误端口，Registry 一时不可用会阻止源确认。

普通消息流处理一个有界批次并发出 Barrier。Pipeline 确认目标效果并提交本地 Epoch 后，由消息源确认对应会话的交付。未知发布结果进入 `RecoveryRequired`。Broker 可能再次交付，依照目的地接收回执或业务键做幂等。MQ 资源授权包括连接本身、实际网络端点及对应的 `/topic/`、`/queue/`、`/exchange/` 或 `/group/` 资源。生产消息体、凭据和会话交付句柄均由已有资源策略保护。

Data Flow 的 `parallelism` 指定 `workerProcesses`、`consumerLanesPerWorker` 和 `threadsPerWorker`。独立消费者在同一 Worker 内共享根 Executor 和 Buffer，各有会话、组件状态及 Epoch 位置。Management 通过现有 Worker 进程和租约协调成员，按节点累计 CPU、进程和内存配额。远程 `ud_worker agent` 使用双向 TLS，Runtime 仍在实际节点直连 Broker 和目的地。产品会报告请求并行数与实际成员。需要共享状态的并行图由 Binder 拒绝，跨主机全局窗口、Sort 和 Join 的重分区、阶段调度与归并仍待实现。

`delivery.mode` 为 `kafkaTransaction` 时，业务目的地和提交位点位于同一 Kafka 集群。每次事务包含输出、组件状态块、Epoch 清单与消费者位点。状态主题需要日志压缩并授予对应 Topic 操作权限。单成员事务消费组由当前 Data Flow 独占，实际分配须覆盖整个输入主题。累积位点包括本次没有新记录的分区，恢复时须核对清单指纹、块摘要和当前消费组的完整位点。并行无状态成员各使用独立状态分区，允许其他成员推进已转移分区的位点。

消费到达有界切面后，原生算子消化输入结束事件并排空输出，同一事务提交最后输出和终态快照。主动停止时排空已经读取的消息并保留最后提交的开放窗口。跨节点恢复只有在旧租约失效、Broker 状态可证明及节点有权限时才能继续。

Kafka 源可用 `partition` 固定读取一个物理分区，并使用该分区专用消费组。`endOffsets` 只声明所选分区。Kafka 目标可以固定分区，或配置 `repartition` 的 `partitions` 与 `keys`，使用 Compute 的类型化哈希把等值键写入同一分区。阶段调度仍需冻结各分片的生产者集合、输入切面和完成证明。

Native Journal 和 Kafka 的组件 Epoch 都记录已连接输入的观测水位、idle 状态和已发出的水位线。恢复后沿已提交前沿继续归并。未观测且活跃的输入阻塞推进，所有输入 idle 时保持原水位线。

真实验证指令、预置资源和样例配置见 [MQ 验收](../../tests/message_brokers.md)。

Connection Manager 解析 Package 优先于 Project 的逻辑定义，绑定配置、能力、授权和 Secret，获得独占物理 Session 租约。根 manager 维护活动租约与可复用健康 Session；池键包含 driver/resource/tenant/credential identity 与策略，获取重新授权、解析凭据，凭据改变不能命中旧项。取消通过活动租约转发物理 I/O cancel。资源释放不因授权撤销或取消被阻止。

目标效果 → receipt/commit outcome → Journal → durable fact。Journal 已有分段索引、hash 链、原子 manifest、快照和批量帧，继续作为唯一持久 owner。外部效果成功后确认失败始终 CommitUnknown。容器事务不将尚未提交的子任务登记为持久成功；无法形成可靠回执边界的事务/恢复组合提前拒绝。

## 验收边界

完整产品在 Linux 和 Windows 验证 Native、JNI、.NET、真实数据源、并发与背压、故障恢复和安装消费。当前 ABI 13 的验收状态以 `metadata/product.json` 为准。SSIS 职责依据见 `etl-ssis-official-evidence.json` 与 `ssis-reference-matrix.md`。

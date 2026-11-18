# 真实消息代理验收

`test_message_brokers` 接收原生连接器模块绝对路径和测试配置文件。通过 `tests/integration_resource.py` 执行时，环境变量缺失或未指向实际文件会报告 Blocked，退出码为 77。配置给出后，模块、授权、订阅、发送、回读或断言失败均为测试失败。

## 准备资源

- 每次运行预置六个专用空地址，名称对应样例中的 `binarySource`、`binaryTarget`、`jsonSource`、`jsonTarget`、`redeliverySource`、`invalidSource`。测试期间只允许本工具消费这些地址。
- RabbitMQ、ActiveMQ Classic 和 ActiveMQ Artemis 使用队列。Classic 和 Artemis 分别配置可访问的 AMQP 1.0 acceptor。Classic 使用 `variant: "classic"`，Artemis 使用 `variant: "artemis"`。
- Kafka 使用六个单分区普通主题，关闭压缩。每次运行指定全新消费组，工具从最早位置开始消费，使用同一组重新订阅以验证已提交偏移。Kafka 广告的每个地址也必须可访问并列入 `endpoints`。
- Kafka 还需要三个互不重复的空主题 `transactionSource`、`transactionTarget` 和 `checkpointTopic`。源和目标各用两个分区并关闭压缩，检查点用一个分区并设置 `cleanup.policy=compact`。每次验收重新创建这些主题，使各分区的首条记录偏移为 0。如果启用 ACL，凭据需要能够读写这些主题、提交该消费组偏移以及初始化并提交 `ud-checkpoint-*` 事务 ID。非 Kafka 配置省略这三个字段。
- RocketMQ 使用 5.x gRPC Proxy、NORMAL 主题和预置的 SimpleConsumer 消费组。消费组需能消费这六个主题的新测试消息。Proxy 发回的所有 broker 路由地址也必须可访问并列入 `endpoints`。
- `scratchDirectory` 指向独占的空目录，使用绝对路径。工具保留 `binary-journal`、`json-journal` 和 `invalid-journal` 作为本次提交、确认及失败诊断的证据。重新运行使用新目录和已清空的专用地址。
- 在版本库外复制 [配置样例](message_brokers.example.json)，填写真实连接地址和所需凭据。主程序把 `credentials` 返回给驱动的凭据解析接口。Package、日志和 Journal 使用凭据引用。TLS 资源按驱动要求填写 CA 文件和已授权的端点。

| `broker` | MODULE | `credentials` 常用字段 | 端点授权格式 |
|---|---|---|---|
| `rabbitmq` | `ud_connector_rabbitmq` | `host`、`port`、`virtualHost`、`tls`、`username`、`password`、`caFile` | `amqp://host:port` 或 `amqps://host:port` |
| `kafka` | `ud_connector_kafka` | `bootstrapServers`、`tls`、`username`、`password`、`caFile` | `kafka://host:port` 或 `kafkas://host:port` |
| `rocketmq` | `ud_connector_rocketmq` | `endpoint`、`tls`、`namespace`、`accessKey`、`secretKey`、`securityToken`、`caFile` | `grpc://host:port` 或 `grpcs://host:port` |
| `activemq-classic`、`activemq-artemis` | `ud_connector_activemq` | `variant`、`host`、`port`、`tls`、`username`、`password`、`caFile` | `amqp://host:port` 或 `amqps://host:port` |

队列配置省略 `group`。Kafka 和 RocketMQ 配置必须提供 `group`。先准备真实凭据文件，再令表中的环境变量指向文件绝对路径。

## 执行

以下是 Linux 示例。Windows 以实际 `.exe` 路径和 `.dll` 路径替换测试程序及模块。两种 ActiveMQ 验收分别运行，使用同一个 MODULE 和各自的真实配置。

```sh
python tests/integration_resource.py UD_TEST_RABBITMQ_CONFIG "$TEST" "$PLUGINS/ud_connector_rabbitmq.so"
python tests/integration_resource.py UD_TEST_KAFKA_CONFIG "$TEST" "$PLUGINS/ud_connector_kafka.so"
python tests/integration_resource.py UD_TEST_ROCKETMQ_CONFIG "$TEST" "$PLUGINS/ud_connector_rocketmq.so"
python tests/integration_resource.py UD_TEST_ACTIVEMQ_CLASSIC_CONFIG "$TEST" "$PLUGINS/ud_connector_activemq.so"
python tests/integration_resource.py UD_TEST_ACTIVEMQ_ARTEMIS_CONFIG "$TEST" "$PLUGINS/ud_connector_activemq.so"
```

工具通过原生连接确认单条和多条消息被真实 broker 接受，并运行持久 Journal 下的 `mq.source → mq.destination` 管道。它在准确提交条数可观察后请求优雅停止，从独立输出地址核对二进制、结构化 JSON、键和消息头，再对输入地址确认未出现重复交付。RocketMQ 按内容集合核对并等待超过 60 秒的不可见期。其他产品按专用队列或单分区顺序核对。RocketMQ 测试两个非空二进制载荷，其他产品还比较空载荷和空值。

另有逻辑地址与拨号端点授权拒绝、关闭未确认会话后再投递、JSON 解码失败后再投递三个方面。失败验证只确认已核对的消息。Kafka 通过同组下相同分区和偏移证明未确认消息重新可见。这些通用 Case 使用 Journal 的至少一次模式。

Kafka 配置还运行独立的有界事务 Case。工具向分区 1 写入一条时间为 5、值为 7 的 JSON 记录，同时在分区 0 留下对照消息。管道固定消费分区 1，在偏移 1 自然终止。目标按窗口键哈希重分区，对应分区必须出现一条窗口起点为 0、终点为 10、总和为 7 的记录。工具随后以相同的 Package、执行 ID 和消费组在新 Journal 中要求从 Broker 恢复。第二次运行也必须自然成功终止。目标没有新增消息；确认分区 0 的对照消息后，源主题没有剩余交付。`transaction-journal` 和 `transaction-restore-journal` 分别保留两次执行的证据。

`integration_kafka_process_takeover` 使用独立的 `UD_TEST_KAFKA_TAKEOVER_CONFIG` 和三个新建的事务主题。测试在原生目标报告 Broker 事务已提交后强制终止原进程，并用相同执行 ID 和新 Journal 启动新进程。新进程从 Broker 恢复后，窗口结果、两个分区的输入以及重复输出均按上述规则核对。

## Kafka 与 Confluent Schema Registry

`integration_kafka_schema_registry` 使用独立的 `UD_TEST_KAFKA_REGISTRY_CONFIG`。配置沿用样例中的 `broker`、`endpoints`、`credentials`、`group` 和 `scratchDirectory`，另填写 `registry` 对象。为 `registry.source` 与 `registry.target` 创建两个单分区、未压缩的空 Topic，并指定一个全新消费组。该模式只使用 `registry` 内的消息地址。

`registry.url` 指向真实 Confluent Schema Registry。`registry.credentialReference` 标识 HTTP 连接凭据，`registry.credentials.headers` 由测试宿主通过凭据解析回调传给 HTTP 客户端。无认证的测试服务使用空对象。有认证的服务在仓库外的测试配置中提供所需 Header。真实测试需要 HTTP 能力、受信任的证书链，以及对指定 subject 和 ID 的读取权限。

在 `registry.subject` 中顺序注册两个 Avro schema。记录名需保持一致。下面的第一条为 v1，第二条为添加默认字段的 v2。

~~~json
{"type":"record","name":"RegistryAcceptance","fields":[{"name":"id","type":"long"}]}
{"type":"record","name":"RegistryAcceptance","fields":[{"name":"id","type":"long"},{"name":"label","type":"string","default":"reader-default"}]}
~~~

将实际注册返回的 ID 分别填入 `writerId` 和 `readerId`，将第二次注册的实际版本填入 `readerVersion`。`unknownId` 必须是同一 Registry 中经查询证实返回 `40403` 的正整数，样例中的数字仅演示配置形状。

~~~sh
python tests/integration_resource.py UD_TEST_KAFKA_REGISTRY_CONFIG "$TEST" "$PLUGINS/ud_connector_kafka.so" --registry
~~~

验收从输入 Topic 向 `mq.source` 提交带 v1 ID 的 Avro 记录，使用 Native reader schema 解析后经 `mq.destination` 发布。测试比较输出的完整 Confluent wire 载荷，核对 v2 ID、`id=7` 和 `label=reader-default`，并检查源记录已确认。随后向同一 Topic 写入未知 ID，核对执行以 `UD_QUALITY` 失败且未提交任何输出。同组重新消费时必须读到相同分区、相同偏移的原消息。`registry-journal` 和 `registry-unknown-journal` 保留两次执行证据。未知 ID 消息有意留在独占测试 Topic 中，下一轮请使用新 Topic 与目录。

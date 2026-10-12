# 四引擎运行时决策

- **Execution**：保留唯一 Package Execution。每个 task/container/iteration/child/pipeline/component 独立 Activation 身份，继承取消、期限与根资源；定义不可变。
- **Worker**：常驻有界线程与显式 Ready Work Group 归 `components/execution`，Control/Tree/Partition 不创建业务 worker。条件变量等待；内部事件直接唤醒目标索引。
- **Binding**：Control graph builder 为依赖与 precedence 唯一 owner。四种约束表达式求值模式与入边 AND/OR 分开。潜在并发时祖先写在绑定阶段拒绝。Pipeline configure 必须先于所有 prepare。
- **Buffer**：保留 Data storage visitor 与 ABI 5 Block Store 的唯一所有权体系；不新增估算/缓存/序列化链路。
- **Connection**：一个 manager owns factory/module/session/pool/lease。File/HTTP/Process/SQL 都使用 Activation Lease 授权，原生插件直接实现完整驱动契约。
- **Transaction**：Required/Supported/NotSupported 显式继承。仅接受能证实事务范围的资源；跨任意驱动组合不模拟 MSDTC。CommitUnknown 禁止重试与 cursor 推进。
- **Recovery**：现有分段 Journal 保留；增添事实也只能通过该 owner。分区稳定 identity 与源位置、算子恢复边界、目标回执归同一任务确认。
- **ABI**：内部 scheduler/control 改造无需独立版本；新增公开组件、driver、partition/inspection 契约时整批升级唯一版本族，旧布局整体拒绝，C/.NET/Java/worker/plugin/wire/package/checkpoint 同批迁移。
- **Delivery**：一张 `metadata/product.json` 账本保存 69 目标和所有四引擎条目，生成其他报告；Implemented 与 Verified 分开，外部环境阻塞保留真实原因。

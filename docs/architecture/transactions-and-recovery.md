# 统一事务、检查点与恢复

```mermaid
flowchart LR
Position[源位置/算子状态] --> Write[目标写入]
Write --> Receipt[同事务提交回执]
Receipt --> Checkpoint[持久检查点]
Checkpoint --> Publish[原子可见代次]
```

一个 version 3 Execution Journal，Package fingerprint、参数指纹、Run ID、作用域/循环位置、Task 状态、Pipeline source positions/operator state、target receipts；类型/wire 版本明确，不复制裸 struct。Task 与 batch 是同一日志的不同记录类别，不再保留 CSV .state/.commit 与 DAG 各自恢复入口。记录校验、原子不覆盖发布、fsync 文件/父目录、exclusive writer lock。

数据库批次 write+receipt 同一目标事务。新目标文件先 stage、flush、hash、原子发布再记录 receipt；写后 journal 前崩溃通过确定性目标 identity/content 验证已提交结果，禁止猜测成功。只支持明确可重放源/目标/算子集合；不支持的恢复策略在 Validate 拒绝。多目标不形成分布式事务，部分已提交时 Package 失败而不是全局回滚成功。

CommitUnknown→RecoveryRequired→停止推进源→receipt 查询→digest/源位置一致则确认→否则暂停/人工决策。TRANSIENT 不等于可安全重试。源内容/Schema/包指纹变化拒绝恢复；任何未知版本/断裂/损坏不回退旧成功。Quality Error Output 与正常目标使用同一 checkpoint/receipt，隔离提交失败不确认源。

轨迹：第 2 批已提交、Journal 未发布、进程终止→重开读取回执→确认第 2 批→从下一位置继续。Loop checkpoint 包括迭代栈，不能跨迭代错误跳过。恢复未实现的部分保持 Planned/Unsupported，不称为 Verified。归属 engines/package/checkpoint.c、connectors/、tests/recovery/。

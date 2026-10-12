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

## 当前实现：Checkpoint Journal 4

`state_directory` 是授权的绝对目录，`execution_id` 必须非空。`resume=0` 只创建新日志；目录已有日志拒绝覆盖。`resume=1` 要求有效日志，不能静默创建新执行。每个 `record-<20 位序号>.udj` 包含版本、连续序号、前一记录 SHA-256、记录类别/identity/payload 和当前 SHA-256；先写/flush 临时文件，再原子不覆盖发布。`writer.lock` 限制单 writer。

一个日志记录 Package/参数/tenant/execution/批次大小/队列预算的 manifest、任务 Begin/Done、各作用域变量和循环 identity、文件源指纹、目标 staging intent/commit、数据库批次 intent/receipt digest。恢复按相同任务及迭代 identity 跳过 Done 并恢复变量；未完成确定性 Pipeline 从不可变文件源重放并重新计算算子，通过目标回执去重。没有重新引入 CSV 专属 checkpoint 或第二运行时。

文件目标仅清理由本日志明确记录的 staging 路径；已发布文件必须与重算结果的 SHA-256 一致，已提交目标及文件源在重新运行前再次验证。SQL 目标的 journal digest 与物理 driver write/receipt 使用同一个 digest 函数，只有目标确认后才写 batch Done；CommitUnknown 不增加 batch 序号。日志提交发生在外部提交后而失败时返回 RecoveryRequired。

范围：当前支持文件源、确定性内置转换、SQL receipt 目标与原子 NDJSON 文件目标。SQL Source 尚无跨进程持久 snapshot 契约，启用 checkpoint 时在执行前 Unsupported；不可重放的 Task 中断后返回 CommitUnknown，不擅自重复外部副作用。自定义组件必须显式声明 REPLAY_SAFE，声明由宿主/插件负责履约。重放恢复不是跨任意目标全局 exactly-once；算子当前重算而非持久化所有内存对象。

实际门禁 `package_durable_recovery` 使用真实子进程 `_exit`，覆盖循环变量、数据库提交后/日志前、文件发布后/日志前、再次 resume 不写重复数据、源/包变化、校验损坏/日志缺口、writer 排他，以及不明自定义副作用停止。内存检查在 ASan/UBSan 下重跑相同门禁。日志见 docs/acceptance/evidence/ssis-recovery。

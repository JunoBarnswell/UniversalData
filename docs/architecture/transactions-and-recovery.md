# 统一事务、检查点与恢复，协议 10

C17 Execution、Connection Manager 和现有 Journal 是唯一执行与恢复所有者。Catalog 保存管理记录，不推断事务事实。旧协议与旧恢复状态明确拒绝，旧文件和数据库回执代次保留。

`transaction: {mode: "required", participants: ["business", "errors"], scope: "task", provisionReceipts: true}` 声明完整参与者。Manager 在业务副作用前检查所有连接的真实参与能力；普通本地事务能力不等于可恢复参与者能力。缺少 Native Journal 或任一参与者不支持时拒绝开始。

Task 范围整体提交。Data Flow 可显式选择 `scope: "epoch"`：每个屏障准备全部分支，先持久化共同决定，再解析分支结果，发布该 Epoch 的暂存事实，然后允许源确认，并开始下一分支。Task 范围中的 CDC 在绑定时拒绝，避免等待源确认与整体提交相互阻塞。

CDC 源可配置 `microBatchRows`、`microBatchMs`。两项省略时每笔源事务发出一个屏障。配置后以第一个读到的完整提交开始计时，达到行数或时间阈值后在完整源事务边界封存。封存记录保留批内源事务的顺序、各提交后的水位以及最后一个日志位置。`maxTransactions` 仍按源事务计数。

SQL 目标按输入批检查初始前像并记录收据。需要整个 epoch 在目标上原子提交时，由 Data Flow 的 `transaction.scope = "epoch"` 协调。源在目标确认和 checkpoint 持久化后推进日志位置。提交后中断时，恢复流程重放已封存数据，核对已有收据，并确认最后一个完整事务的位置。

PostgreSQL 使用 PREPARE TRANSACTION 并检查 max_prepared_transactions。MySQL/MariaDB 使用 XA START/END/PREPARE/COMMIT/ROLLBACK。Oracle 通过 OCI 使用 DBMS_XA 完成分支生命周期，将 64 个十六进制字符组成的 Native 分支 ID 编码为 32 字节 XID。Native 分支 ID、配置指纹和同事务的分支回执证明提交。缺少可验证的分支回执时，提交结果保持不确定。SQL Server/ODBC 需要通过驱动的 XA 能力探测才能成为参与者。

Task Done、批次 Done、游标、枚举器和 Epoch 在未决事务内先存为有序暂存事实。共同提交后才发布；回滚仅退役暂存事实。持久 COMMIT 决定后禁止改为回滚。恢复没有决定的分支时先持久化回滚决定；存在决定时按决定逐个恢复，即使一个资源不可用，也尝试其他参与者。任何无法证明的提交进入 RecoveryRequired，停止源确认和自动重放。

Journal 按固定上限滚动日志段，累计的日志大小达到前一快照的规模后再压缩。发布已提交事实时，连续且键互异的事实可同帧持久化，同键更新和带退役动作的记录按顺序生效。

SQL 目标的记录错误在真实事务保护下使用 savepoint 隔离。先尝试批量写入，记录错误回滚该尝试后使用原生参数绑定逐行处理。正常目标和错误目标均须属于声明参与者。转换、截断和业务约束可重定向；连接中断、取消、超时、语句/Schema 执行故障以及提交不明保持失败。原始输入只有明确启用才保留。

REST 统一声明 `delivery: "response" | "idempotent" | "receipt"`。普通响应模式只发送一次；未决发送意图不能自动重放。幂等模式使用 Native 请求身份；回执模式验证 digest 和源位置，并可查询远端回执。增量重叠根据已绑定的 KEYED_REPLACE 能力与键 lineage 判断，不依赖 SQL 组件名称。

PostgreSQL COPY 与受控 MySQL/MariaDB LOCAL INFILE 在同一业务写入/回执路径内。SQL Server BCP 与 OCI Direct Path 只装载受控暂存区；正式业务写入和回执仍由原事务拥有，暂存区生命周期延续至事务结果确定。Direct Path 的独立提交不能被描述为可随业务事务回滚。

以上是当前源码契约，尚未进行 ABI 10 编译或行为验收。注册的 transaction_fact_publication 和真实数据库 participant_recovery 测试覆盖暂存事实发布，以及 prepare 后退出、部分提交后退出的恢复边界；它们当前不是通过证据。SQL Source 错误隔离、在线 Schema 重绑定、完整 Epoch/指标恢复与 Windows 驱动验收仍按 metadata/product.json 追踪。

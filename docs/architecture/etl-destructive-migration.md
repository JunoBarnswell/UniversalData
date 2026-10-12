# 四引擎破坏性迁移及源码证据

PR #4 基线为 `6686597`；工作区初始干净。不是把 main 的已知问题重复开发。

| ID | Evidence / Root Cause | Target owner / Delete / Rewrite / Keep | Acceptance |
|---|---|---|---|
| A-01 | registry.c 已删除无契约类别、索引化并原子注册；已解决此前静默拒绝 | Keep Registry；新增正式类别必须完整生命周期 | 类别校验、owner retain/卸载 |
| A-02 | buffer.c + data/storage.c 已先按存储容量判断驻留，Wire 只用于 spill；已解决 | Keep Data/Block Store | resident 无 codec；共享分配去重 |
| A-03 | pipeline.c 已建立 tree jobs，但 executor 扫描并全体唤醒 WAITING | Rewrite event work groups；Keep binder/operators | 跨树并行、定向唤醒、背压/End |
| A-04 | control.c 的 task_wave、scheduled 批次、run 完后更新 states 仍是 wave | Delete wave/ready 全图重扫；Rewrite indexed dependency completion scheduler | 快任务后继在无关慢任务结束前运行 |
| A-05 | journal.c 已分段、索引、快照/hash chain；已解决此前小文件模型 | Keep Journal；禁止新 checkpoint owner | 缺段/损坏/截断/恢复拒绝 |
| A-06 | task_result 已将 executed/durable/uncertain 分开；已解决计数提前问题 | Keep task facts，扩展提交必须沿同一事实 | checkpoint fail 保留 executed/unknown |
| A-07 | product.json 已是权威，但 Linux 豁免和 Windows 待验是旧请求事实 | Rewrite 当前 scope/gates/projections；Keep 69 goals | Linux 命令证据；Windows 豁免 |
| A-08 | pipeline finish 拓扑顺序仍可能在部分目标提交后另一 finish 失败 | Outcome 必须考虑既有提交；Keep receipt owner，禁止盲重放 | 混合目标提交不确定性原样呈现 |

删除 `task_wave`、逐 group 临时 thread spawn/join、全等待树广播重扫及无引用辅助入口。保留正确的 Schema/Expression/Compute/Buffer/Journal/Registry。公共 breaking changes 和绑定随正式 ABI 契约一起提交，不保留旧别名、包装器、DLL 或转换执行链路。

# 四引擎目标架构

唯一链路：不可变定义 → 纯语义绑定 → Control Plan / Physical Pipeline Plan → Root Execution / Activation → Shared Scheduler / Compute / Buffer / Connection Lease / Journal → Events / Metrics / Catalog → C ABI / PInvoke / JNI。Catalog 与语言层不执行业务算法。

## 所有权与调度

Root Execution 拥有 Registry 租用、常驻 Worker Pool、根 Context 配额、Buffer Manager、Connection Manager、Journal 和诊断。任务、树和分区提交同一个 Work Group 契约。根调用线程参与执行，额外 worker 最多预算减一；嵌套 wait 在原生调用栈上推进自己的子工作，不创建线程，也不抢占无关祖先任务。所有回调退出后 group 才可销毁。

Control Binder 形成索引化前后继边、入度和规范化约束模式。前驱完成仅更新其出边，入度为零时评估约束并入 ready queue；Skipped 沿相同依赖图传播。不存在 task_wave。并行作用域禁止祖先写；绑定与执行使用相同保守并行策略。事务作用域独占声明的单一物理 Session，不能把不支持的资源组合描述成 ACID。

Pipeline Planner 形成真实树、树内拓扑、跨树 Path、Buffer/队列容量、输入/容量/End 等待原因及 ordering/state/concurrency 计划。树同一时刻仅有一个控制者。push 唤醒消费者，pop 唤醒生产者，End 唤醒后继；运行中事件置 pending wake，防止回调返回 Busy 时丢唤醒。禁止任意进度导致全体等待树重扫。

Compute 持有唯一 Sort/Join/Aggregate/Lookup/Window 算法。分区调度调用同一根 pool；结果合并遵循 canonical compare/hash/NULL/decimal 和显式排序规则。全局排序稳定归并，不因 worker 完成顺序改变相等键顺序。不可恢复 source/driver 明确拒绝恢复计划。

## Buffer、连接、恢复

Data 是 Schema/Value/存储清单唯一 owner。sealed Batch 驻留按唯一真实分配 retain/去重；仅 spill、交换和持久化编码 Wire。所有 Path、算子、分区共享同一个 Buffer Manager 和根内存/working/spill 配额，第三方不可计量 RSS 明确标注。

Connection Manager 解析 Package 优先于 Project 的逻辑定义，绑定配置、能力、授权和 Secret，获得独占物理 Session 租约。根 manager 维护活动租约与可复用健康 Session；池键包含 driver/resource/tenant/credential identity 与策略，获取重新授权、解析凭据，凭据改变不能命中旧项。取消通过活动租约转发物理 I/O cancel。资源释放不因授权撤销或取消被阻止。

目标效果 → receipt/commit outcome → Journal → durable fact。Journal 已有分段索引、hash 链、原子 manifest、快照和批量帧，继续作为唯一持久 owner。外部效果成功后确认失败始终 CommitUnknown。容器事务不将尚未提交的子任务登记为持久成功；无法形成可靠回执边界的事务/恢复组合提前拒绝。

## 验收边界

本轮集中 Linux native/JNI/.NET、SQL/文件、并发/取消/背压、Spill/Journal 故障恢复、插件和安装消费。Windows 标为 Exempt，不继承旧 Windows 证据。官方职责依据见 `etl-ssis-official-evidence.json` 与既有 `ssis-reference-matrix.md`。

# 流批统一：责任与状态边界

## 唯一链路

Package→Native Binder/Physical Plan→Root Execution/Activation→共享 Work Group/Executor→Component/Port/Path→Compute/Connection Lease→既有 Journal Epoch/Native Catalog。React 仅编辑和展示；Management 仅认证、派发与投影；Worker 仅调用 Native 并传送持久事实。

审计基线 09118e2：pipeline.c 已有 FIFO 控制事件、多输入 Barrier 对齐、Source 位点、State snapshot 和 Epoch commit；CDC 已有真实 vendor log；window.c 是关系窗口；debug.c 有有界行采样；Catalog 原先仅保存 Ready/Terminal 执行快照。它们均不是待重建子系统。

## 决策复核

- 生命周期：有限输入 END；连续源无输入返回 BUSY，并通过同一 Work Group 的期限唤醒重新取数。不存在 Source 私有线程、定时重启 Package 或第二个队列。取消唤醒执行器；等待时继续检查 Root/Activation 期限。
- 顺序：DATA、Watermark、Barrier、Schema 使用同一个有界 Path Queue。状态窗口先处理控制事件、排空已触发结果，再传递原控制事件；Barrier 在排空后调用 checkpoint，防止下游快照越过未发布结果。
- 时间：eventTime 通过 Native Watermark Policy 能力声明拥有输出时间策略，消费上游 Watermark 并仅发布由配置字段/lag 推导的水位，不能把源事务时间直接当作业务字段时间。Native UTC 微秒，Watermark 单调；按输入最小值规约，显式 idle 输入暂不参与，恢复仍不能倒退输出水位。已 END 输入不参与最小值。禁止把系统时钟当事件时间。
- 状态：事件窗口属于 ud_operator/Compute，不包装关系 window.c。复用 ud_plan/ud_accumulator/Canonical value 和统计状态 codec。窗口状态子配额从既有 Buffer Manager working 域扣账，Root 总内存、共享 working 和算子状态上限同时约束；空状态算子也持有 working 域生命周期引用。活动窗口受显式数量/内存预算约束，失控返回 Resource。快照只包含活动聚合状态、时间水位和定义指纹，使用既有 Batch/Wire→Journal，不保留无限事件重放历史。
- 恢复：Source Position、组件状态、Sink Receipt 在既有 Epoch owner 下确认。Stop 请求源在安全边界停止，保留完整 Drain/最后确认；Cancel 中断；Debug Pause 沿用原调试边界。没有确认的提交不可自动重试。
- 观测：Catalog 持有有序 Native 执行事件和进度记录；Worker 周期发布 Native 安全进度，Management SSE 只输出持久化数据。Last-Event-ID 是实际 Catalog revision。Native Catalog 只保留每次执行最近八份进度快照；Ready/Terminal、Journal 和源位点保留原治理政策，日志沿用既有显式保留清理。订阅有数量、时间、大小限制，慢客户端不能反向阻塞执行。采样仍走原 Native Debug 安全边界。

Session 采用事件时间准入下界 `Watermark − allowedLateness`，边界后收到更早事件按 latePolicy 处理；仅合并相交的半开会话区间。该约束防止在无撤回协议时复活已经发布并清理的 Session。计时等待的源通过 `ud_activation_stopping` 在事务边界完成优雅停止。

## 删除/保留矩阵

| 原实现 | 决策 |
|---|---|
| Package/Binder/Executor/Buffer/Journal/Lease | 保留并原位扩展 |
| 关系 Window/聚合/统计类型算法 | 保留并复用，不更名冒充流窗口 |
| CDC step 内 sleep+MORE 空转 | 删除，使用执行器停放/期限唤醒 |
| 控制通知禁止输出且立即越过窗口 | 替换为统一控制排空生命周期 |
| Web 多接口固定轮询 | 替换为 Catalog SSE+有界快照降级 |
| 巨型 Editor 内可独立的产品工作面 | 迁移到职责模块，保留唯一编辑历史与变更模型 |
| ABI 11 / 历史状态与协议兼容 | 不桥接；版本族成批升级并明确拒绝旧定义 |

可选高级能力必须以真实代码及验收标注，无法由单机内核提供的分布式事务不能被 UI 命名掩盖。

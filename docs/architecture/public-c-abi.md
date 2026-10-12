> 前轮设计记录：本轮架构和 API 以 [四引擎目标](../architecture/etl-four-engine-target.md)、[运行决策](../architecture/etl-runtime-decisions.md) 与当前公开头文件为准。当前版本、能力状态和 Linux 验收统一来自 `metadata/product.json`；Windows 本轮豁免。

# 当前唯一 C ABI：4

SDK 1.0.0 开发版，ABI/Package/Schema/Plan/Wire/Plugin/Worker/Checkpoint 为 4。所有布局必须 `struct_size == sizeof(current)` 且版本完全匹配；没有旧前缀兼容。调用方、driver、JNI、PInvoke 同批重编。

| 句柄 | 生命周期和所有权 |
|---|---|
| Context | 配额和 allocator；所有 native allocations 释放后销毁 |
| Package | 不可变 JSON 定义；Execution 拥有独立副本 |
| Execution | create/bind → Ready → Running → Draining → terminal → destroy；一个控制器，cancel/metrics 可并发 |
| Activation | task/child 的 scope、definition、身份和期限；借用到回调结束，禁止缓存 |
| Registry | 复制 metadata，租用模块；存在 Execution/Expression 租用时修改或销毁 Busy |
| Expression / Plan | 一个求值控制器；metadata owned，scalar view 借用到下一次求值；Plan 被 accumulator/operator retain |
| Batch | mutable 构建后 sealed；跨线程/端口 retain，destroy 释放一个引用；schema/lineage owned |
| Buffer Manager / Operator | 多实例共享线程安全 Manager；Operator 单控制器；块和 spill 由 Manager 所有，活块阻止销毁 |
| Connection | 每个游标/事务实例单控制器，cancel 可并发；Manager lease 在 Activation 内释放 |

序列化和 owned 字符串使用 `ud_free`；显式 UTF8 长度允许数据包含 NUL，资源标识和路径禁止 NUL。函数 evaluator 输出 owned 指针 payload，Expression 释放；普通 row/scalar view 是 borrowed。并发 Activation 只绑定声明 ThreadSafe 的函数；不满足即在 I/O 之前失败。

UD_END 只表示迭代结束；UD_MORE 只表示 Pipeline 组件仍有输出，下一步 POLL；二者不是 Task 成功。局部和根期限返回 UD_TIMEOUT，不伪装成取消或瞬态失败。事务结果不确定为 UD_COMMIT_UNKNOWN/RecoveryRequired。诊断为 calling-thread view；可观测事件携带 root sequence 和 borrowed Activation。

Execution destroy 在 Running/Draining 时 Busy；如果自定义组件残留 allocation/lease 返回 Resource 并保留句柄，不释放后留悬空指针。回调在事件序列锁下调用；允许 cancel/metrics，禁止重入控制、Journal 或保留 Activation。

Plugin 只提供 Connection factories/connector table 和 component table。Task、Source、Transform、Destination、Function 使用同一 Registry。未实现的 Connection/Enumerator/LogProvider descriptor 类别不接受注册。旧 `ud_plugin_execute(operation)` 和独立 ABS/CSV plugin 删除。模块在 Registry/Expression/Execution/Connection 释放之前不能卸载。

.NET/Java 只处理 ABI 布局、字符串、句柄、错误和取消/等待；业务计划、类型、表达式、事务、算子全部在 C17。

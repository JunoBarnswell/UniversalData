# Native Custom Task / Function 插件契约

只使用安装后的 public SDK headers。模块导出 `ud_plugin_get_api`，只接受当前完整 ABI 4；component-only 模块提供 `UD_PLUGIN_COMPONENTS` 和 component table，没有 dummy factory 或 generic execute 入口。Physical connector 模块提供单独 connector table 和必要连接 factories。

Registry 原子复制元数据并 retain 模块；重复 ID、旧版本、裁剪布局或非法 callback table 整体拒绝。Root Execution 合并同一 Registry 描述，并租用外部 Registry；Expression 同样租用函数所属 Registry。存在活动引用时 unload/destroy Busy，调用方先结束 Execution/Expression、销毁 Registry，再卸载模块。

Task `configure` 为可选纯绑定验证，不执行 I/O；`invoke` 收到 root Execution、borrowed Activation、当前 allocator、身份和表达式 resolver。资源经 `ud_activation_acquire_connection` 与授权取得；变量经 Activation 读写。回调不得缓存 Activation、重入执行控制或绕过目标提交/receipt 契约。Task 中的 End/More 返回 Invalid，Busy 在 Package 边界映射为 Transient。

Function descriptor 具有 Pure capability、arity、function_bind/function_evaluate。输出 metadata 经 Data model 克隆，pointer payload owned，由 Expression 释放。并行任务只绑定 ThreadSafe 函数。一个 Expression instance 只有一个控制器；宿主不能把函数放到第二套托管执行器。

Source/Transform/Destination 使用 configure → prepare → step(Data/InputEnd/Poll) → finish → dispose。每步每输出最多一个 sealed Batch；More 表示继续 Poll。finish 仅在所有输入结束且输出排空后提交。组件未声明 ThreadSafe 时不在并发 Activation 内运行；未声明 ReplaySafe 的外部效果中断进入 RecoveryRequired。

示例位于 `examples/plugins/custom_task.c`，测试使用实际动态模块、当前公共头、原子注册 rollback、模块引用、native AST/scopes、授权、取消/期限、真实文件效果及进程终止恢复。平台和证据以 `../acceptance/ssis-status.json` 为准；历史 Linux 结果不证明当前源码。

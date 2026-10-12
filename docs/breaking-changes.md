# Breaking Changes：1.0.0 / ABI 4

当前完整产物由 `packaging/package.ps1 -BuildDirectory ...` 按平台生成 Release SDK/ZIP 和语言包。旧 `-NativeArtifacts` 双平台前置条件删除；NuGet 原生打包属性改为 `NativeLibraryDirectory` 与 `NativeRuntimeIdentifier`，必须成对指定且库文件存在。当前 Windows 包含唯一 `ud_data.dll`，没有旧 ABI 文件或库名别名。

本轮是 Clean-break，旧 ABI 下线，没有兼容版本、别名或长期迁移桥。

- Package/Schema/Plan/Wire/Plugin/Worker/Checkpoint 全部只接受版本 4 和完整当前布局；旧文件/插件返回 Incompatible，宿主需重新导出定义及重新编译。
- 删除 CSV Job/DAG/Run 生命周期和旧托管/Java 类，统一 `ud_package` / `ud_execution`。
- 删除 Query 格式专属 sort/join/group API 和 `ud/query.h`。使用 Compute JSON Plan 或共享 `ud_operator`，资源归 `ud_buffer_manager`。
- 删除文本 schema/filter/group DSL 和 `ud_plan_create`；`ud_plan_create_fields` 接受 JSON AST Plan。
- 删除 public root variable setter。组件通过 borrowed Activation 读写；root getter 在运行中 Busy，并发任务禁止修改共享祖先变量。
- ExecutionOptions 添加 max_concurrency；ComponentContext/Event 添加 Activation，Event 添加 sequence；局部超时为 UD_TIMEOUT。Pipeline pending output 为 UD_MORE。
- Registry 添加纯函数 bind/evaluate 与模块租用。函数 pointer result owned，求值 view 有效至下次调用。
- 删除 generic plugin execute/create/instance API，重复 ABS/CSV plugin 及 ODBC/DuckDB 原型。Connection factories 只供 Manager，其他组件通过 Registry。
- Quality 删除 quarantine callback/store。Apply 返回 owned accepted/rejected/violation；必须将错误输出连到同一目标契约。分类计数不代表持久化或源确认。
- Aggregate component 配置使用 `plan` JSON；Lookup 当前为首匹配 inner semantics，Window 支持 rowNumber/runningSum。未实现扩展没有占位注册。
- Checkpoint manifest 纳入并发上限；模式/定义/参数不匹配拒绝恢复。已有未知外部效果不能猜测或自动重放。

没有 `.dtsx`/COM/SSISDB/MSDTC 兼容承诺；不提供跨所有目标的全局 exactly-once。事务与恢复边界见 [设计](architecture/phase-0-7-design.md)。

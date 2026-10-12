# ABI 3 / Package 迁移验证

基线 main `ed3237a9852ac29831d85d93c03fc8b173cdbf6f`，工作分支 `codex/ssis-package-runtime`，草稿 PR #3。下列是本轮实际运行证据，不继承历史 SDK 的通过状态。

| 检查 | 实际结果 | 命令/范围 |
|---|---|---|
| Linux native ABI 3 + JNI + SQLite | 通过 | CMake Debug build/abi3 |
| 原生 CTest | 21/21 Passed | `ctest --test-dir build/abi3 --output-on-failure` |
| ASan/UBSan + leak detection | 21/21 Passed | build/asan3，`ASAN_OPTIONS=detect_leaks=1 ctest ...` |
| Foundation/Columnar 最小 SDK | 5/5 Passed | core-only、安装导出及未改语义的独立 C 消费者 |
| .NET 8 / Maven Java 17 | 构建通过 | examples/dotnet、dotnet-data、bindings/java/pom.xml |
| 新 Package 三语言执行 | 通过 | package_language_parity.py；同 JSON/参数、循环、表达式优先约束、10,003 行 Derived/Split、多目标输出、状态/结果/任务计数一致 |
| 类型/批次 Wire 3 三语言 | 通过 | Decimal38、UTC、UTF8/NUL、Binary、Struct/List、NULL、JSON/Wire 字节一致 |
| 安装后托管 NuGet 1.0.0 消费 | 通过 | 仓库外 PackageReference 消费者从本地 NuGet 源恢复，再执行同 Package；native Linux 库单独安装 |
| 旧入口删除 | 通过 | 当前源代码静态 gate；ABI 3 共享库 `nm -D` 无 ud_job_/ud_dag_/ud_run_/ud_snapshot_latest 导出 |
| 公共契约/JSON/XML/Python | 通过 | scripts/static_check.py；git diff --check |
| Windows / GitHub Actions | Blocked | 当前机器 Linux；实际 GitHub job 未执行任何步骤，annotation 为账户支付失败/额度限制 |

GitHub 观察：[运行 38197069720](https://github.com/JunoBarnswell/UniversalData/actions/runs/38197069720)，Windows check 114641994899 的 annotation 明确说任务未启动，不能计为编译或测试通过。新的 CI 已改为 ABI 3 Package/语言消费者命令，但配置存在不等于运行通过。

剩余里程碑：统一持久 Journal/批次恢复、Catalog、完整 Pipeline Spill、更多任务与连接器、列级血缘/完整表达式函数目录和其他扩展。现已提供 Journal 3 的受支持恢复链路；SQL Source 持久 snapshot 仍明确 Unsupported；不称完整 SSIS 架构或全部验收完成。旧 .state/.commit 不再提供恢复入口。

统一恢复新门禁：native 22/22、ASan/UBSan Package 4/4，通过真实进程终止和日志/源完整性检查。证据在 docs/acceptance/evidence/ssis-recovery；范围见 transactions-and-recovery.md。

Catalog 基础新门禁：native 23/23、ASan/UBSan + leak detection 的 Package/Catalog 5/5。测试覆盖不可变版本、租户隔离、重启、授权撤回、结构校验、校验损坏/重命名和已有 staging 文件保护。证据见 docs/acceptance/evidence/ssis-catalog；部署/环境语义及完整历史仍未完成。

Project/Environment/Deployment 新门禁：当前源代码 native 24/24、ASan/UBSan + leak detection 的 Package/Catalog/Project 6/6，以及 C/.NET8/Java17 同 Package 的 Project 参数/共享连接/遮蔽、10,003 行输出和类型/Wire 一致性通过。明确固定修订绑定与回退、重复 checkpoint resume；完整激活/历史/观测不在本模块完成范围。证据见 docs/acceptance/evidence/ssis-project。

真正 Execute Package 新门禁：当前 native 25/25、ASan/UBSan + leak detection 的 Package/Catalog/Project/Child 7/7；C/.NET8/Java17 通过真实子 Package 的参数映射执行 10,003 行管道并得到一致结果（完成任务数 6）。专门测试真实子 checkpoint 终止恢复、子/孙调用、同一资源上下文、隔离、取消/CommitUnknown、深度和旧别名拒绝。证据见 docs/acceptance/evidence/ssis-child-package。

用户范围修订（2026-10-12）：“无需关注ci的状态，Windows 无需测试”。因此本轮完成门禁以 Linux 本地实际验证为准；以上历史 Windows/远程 CI 观察不再阻塞本轮目标，且不代表 Windows 通过。

运行历史新门禁：当前源代码 native 26/26、ASan/UBSan + leak detection 的 Package/Catalog/Project/Child/History 8/8；三语言真实子 Package/Project/类型与 Wire 一致性重新通过。历史测试覆盖原生成功/失败/取消/RecoveryRequired、Ready/终态快照、租户、BUSY、重开、参数指纹/部署版本和敏感正文不落盘。证据：docs/acceptance/evidence/ssis-history。Windows 和远程 CI 按用户指令不作门禁。

File System 新门禁：native 27/27、ASan/UBSan + leak detection 的 Package/Catalog/Project/Child/History/FileSystem 9/9；C/.NET8/Java17 同文件复制、子 Package、10,003 行管道和类型/Wire 一致性通过（任务数 7）。路径/覆盖/临时文件保护、移动/删除/建目录、中途取消/撤权、FIFO/符号链接拒绝和真实效应后终止/CommitUnknown 已实际验证。证据：docs/acceptance/evidence/ssis-filesystem。

Execute Process 新门禁：当前 native 28/28、ASan/UBSan + leak detection 的 Package/相关模块 10/10；真实进程授权拒绝、argv/cwd、退出策略/exec 失败、任务和 Package timeout、运行中取消/子进程组清理、效应后真实终止/Unknown、完成后 resume 实际通过。三语言文件/子 Package/数据回归通过，未把它冒充托管端正向进程授权测试。证据：docs/acceptance/evidence/ssis-process。

## HTTP Task 与安装消费（2026-10-12）

主线已 fetch，仍为 `ed3237a9852ac29831d85d93c03fc8b173cdbf6f`，当前工作分支已包含该提交。`UD_WITH_HTTP=ON` 使用系统 libcurl 8.14.1，最低契约 7.85 + TLS/异步 DNS；默认 OFF 的 Package 构建没有此依赖，并实测绑定时 Unsupported、无网络 I/O。

原生 29/29 和 ASan/UBSan+leak 的 Package/Catalog/File/Process/HTTP 11/11 通过。真实本地 HTTP/HTTPS 验证方法/NUL/Unicode body、状态变量、授权及凭据拒绝、CA/hostname 错误、禁用重定向、GET/HEAD 重试上限与 POST 单次请求、有界响应/头、任务/Package 超时、运行中取消/撤销授权、资源/配置拒绝。真实 POST 产生服务器效应后 `_exit(91)`，resume 返回 CommitUnknown 且未再次请求；完成后 resume 同样不重复请求。

安装后的 UniversalData::Package 静态消费者与 UniversalData::Data 共享消费者均执行同一完整 HTTP 行为套件。NuGet 包含 Linux ABI3 原生库，通过独立本地 feed/新缓存安装；Java 使用 Maven 1.0.0 JAR 和 ABI3 JNI。两者实际调用 HTTPS Task，检查明确 NETWORK 授权、resolver 凭据送达、网络拒绝及 resolver 抛异常的默认拒绝。Maven package 是构建检查（项目无单元测试），验证来自真实消费者，不将零测试算作通过。修复了 Linux-only NativeArtifacts 打包因不存在 Windows 路径失败的问题，未制造或测试 Windows 文件。

已有三语言 FileSystem/真实子 Package/10003 行 Pipeline 和 Decimal/UTC/UTF8/nested/wire 一致性回归通过。全部证据和当前源哈希见 `docs/acceptance/evidence/ssis-http/evidence.json`。HTTP Task 响应仅有界消费并丢弃，不冒充 G61 Source/Destination；libcurl/TLS 内部分配不计入 SDK quota；无端到端 exactly-once。Windows 测试及远程 CI 状态按用户指令免作门禁。

## Check/Data Profiling（2026-10-12）

`data.profile` 复用已有 Pipeline、Source/Transform、连接和恢复服务；唯一 `profile.statistics` Destination 调用新的共享 Compute profiler，流式 row/null count 和 bool/int64/float64/UTF8 extrema 不保留输入 Batch，全部预算在共同 Execution 中。Task 的非空 bool 检查使用现有表达式；false/NULL 均失败，完整 scan 后保留统计和失败数供 Failure 分支使用。绑定时拒绝未知/重复字段、类型/只读变量、非法列、空规则和其他写入 Destination。

当前源码原生 **31/31**、ASan/UBSan+leak 专项 **13/13** 通过。实际 File/SQLite 的多批读取验证行数、空值数、极值、空输入、全空值、Float/UTF8、多个失败与 Failure precedence。权限拒绝/撤销、读取后取消/Package deadline、row/memory 配额、生命周期 cleanup 均实际执行。进程在扫描中和统计产出后/TaskDone 前 `_exit(91)`，恢复重算并得到相同结果；已完成任务 resume 跳过并恢复变量，源数据变化拒绝恢复。持久 SQL Source snapshot 仍明确 Unsupported。

C/.NET8/Java17 同一 Package 增加 Profiling Task：10003 行实际 row/null/min/max 四个规则通过后再执行原有真实子 Package derive/split。两条 Pipeline 的总指标 input/output 20006、completedTasks 8；发布行仍为 10003，三语言输出和指标相同。Decimal/UTC/UTF8/nested/wire 既有回归通过。证据和源码哈希在 `docs/acceptance/evidence/ssis-profile/evidence.json`。高级分布/键关系和更广类型变量不冒充已实现；Windows/远程 CI 状态仍按用户指令免作门禁。

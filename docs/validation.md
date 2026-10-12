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

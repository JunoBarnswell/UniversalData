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

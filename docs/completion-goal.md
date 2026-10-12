# 全部任务完成目标

来源：用户附件 `UniversalData_SSIS_Architecture_Codex_Prompt.md` 和当前会话。范围以附件 Phase 0–7 及组件矩阵为准，不能把本轮已完成的 ABI/绑定迁移当作整体完成。

**最终交付目标：完成附件要求的统一 Package 架构、核心组件及扩展批次，取得全部必需的真实验收证据，形成可审查的完整 PR。** 当前草稿 PR #3 持续提交模块；不自动合并、不公开发版。原始附件的 SHA-256、每项验收和状态见 [机器清单](acceptance/completion-goal.json)。

## 完成判据

1. 一个 Package/Execution 核心，Control Flow 与 Data Flow 分责；无旧 Job/Dag/Run 或长期适配/桥接路径。
2. Schema/Batch/Expression/Compute/Connection/Status/Checkpoint 各只有一套契约；.NET/JNI 仅投影 native ABI。
3. 附件规定的任务、端口、同步/异步 Buffer、多输入输出、连接器、质量输出、恢复、Catalog/观测及扩展逐项实现；未知能力明确 Unsupported，不能计为完成。
4. C/.NET/Java 同 Package/参数/结果；Linux 本地、内存/线程/配额、事务、真实进程终止、CommitUnknown 和安装消费门禁全部实际通过。
5. 每个必需条目同时满足 Implemented + Verified + 当前源代码的可核查证据。Blocked、NotRun、静态检查、编译、PR 合并不能冒充行为验收。
6. 文档、代码归属、Breaking Changes、SSIS 映射和清单同步；最终报告列明组件、API、删除/合并清单、唯一代码路径及后续 BI 复用。

微软专属 WMI/SMO/SSAS/Agent/OLEDB 和 dtsx 的附件要求是可选扩展契约与诚实兼容边界，不默认追加微软专有实现。其他 P1/P2 项保留在目标清单，分期不等于从范围删除。需要外部测试实例/驱动的条目先完成独立代码和本地检查，再记录精确外部前提；阻塞项仍不算完成。

## 执行里程碑

| 顺序 | 交付内容 | 本轮定位 |
|---|---|---|
| 1 | 设计、审计、唯一核心、ABI 3 和薄绑定迁移 | 已交付开发批次，完整目录/门禁仍有缺项 |
| 2 | 统一任务/批次 Journal、提交回执、崩溃/取消/CommitUnknown 恢复 | 当前进行 |
| 3 | Catalog、Project/Environment、版本/部署/运行历史和结构化观测 | 恢复之后推进 |
| 4 | 完整核心任务、表达式/lineage、Pipeline spill、多端口/质量组件 | 按依赖提交和集成验证 |
| 5 | 所有列明的 P1/P2 组件与集成、CDC、BI 共用基础 | 分组件实现并记录外部测试前提 |
| 6 | 九项核心集成、Linux 本地、三语言、内存/线程和安装消费验收 | 全部门禁通过后才满足最终目标 |

不设置无依据的日历承诺。工作期限待完整依赖和外部资源梳理后评估；截止时间未设置不影响继续执行。每个功能模块单独 commit/push 到草稿 PR，并更新 Code/Verification/Evidence。范围变更必须显式记录，不能用缩小统计口径提高完成率。

## 当前边界

main 已重新 fetch，当前分支没有落后；已有基础批次和 ABI 3 迁移位于 PR #3。历史开发验证：Linux CTest 21/21、ASan/UBSan 21/21、core-only 5/5、三语言 10,003 行 Package 与类型/Wire 一致性通过；这些证明对应开发里程碑，不证明本目标全部完成。

用户最新指令“无需关注ci的状态，Windows 无需测试”：本轮移除 Windows 测试和远程 CI 状态门禁，保留 Linux 本地真实验证；Windows 不计为已验证。恢复与 Catalog 已交付受支持子范围；完整恢复、部署/环境管理、spill/组件目录及其他缺项仍在清单中保留。旧 0.2.0 验收清单不替代本目标。

## 逐项目标

| ID | Phase | 目标 | 代码 | 验证 |
|---|---|---|---|---|
| G00 | 0 | 产品/架构/SSIS 证据及破坏性矩阵 | Implemented | Verified |
| G01 | 1 | 唯一执行器和主版本切换 | Implemented | Verified |
| G02 | 1 | 统一 Schema 和列 identity/lineage | Planned | NotRun |
| G03 | 1 | sealed Batch/Buffer 所有权 | Implemented | Verified |
| G04 | 1 | 统一类型/转换/NULL/Compare/Hash | Planned | NotRun |
| G05 | 1 | 统一表达式 AST/Binder/函数 Registry | Planned | NotRun |
| G06 | 1 | 唯一状态/错误/诊断模型 | Implemented | Verified |
| G07 | 1 | 统一组件/插件 Registry | Planned | NotRun |
| G08 | 2 | Project/Package/Environment 与作用域 | Implemented | Verified |
| G09 | 2 | Package 定义/绑定/运行生命周期 | Implemented | Verified |
| G10 | 2 | Sequence/For/Foreach 控制流 | Implemented | NotRun |
| G11 | 2 | Success/Failure/Completion + expression + AND/OR | Implemented | NotRun |
| G12 | 2 | 受限并发/任务超时/事件处理器 | Planned | NotRun |
| G13 | 2 | DataFlow Task | Implemented | Verified |
| G14 | 2 | Execute SQL Task | Implemented | Verified |
| G15 | 2 | File System Task | Implemented | Verified |
| G16 | 2 | Execute Process Task | Planned | NotRun |
| G17 | 2 | Execute Package Task | Implemented | Verified |
| G18 | 2 | HTTP Task | Planned | NotRun |
| G19 | 2 | Check/Data Profiling Task | Planned | NotRun |
| G20 | 2 | Native Custom Task | Implemented | NotRun |
| G21 | 3 | 组件 metadata/设计时与运行时生命周期 | Planned | NotRun |
| G22 | 3 | 多输入/输出 Path/End/backpressure | Implemented | NotRun |
| G23 | 3 | Execution Trees/Buffer Pool/资源调度 | Planned | NotRun |
| G24 | 3 | 阻塞 Sort/Aggregate/Join 的 Pipeline spill | Planned | NotRun |
| G25 | 4 | 逻辑 Connection Manager 与物理 Driver | Implemented | NotRun |
| G26 | 4 | CSV Source/Destination | Planned | NotRun |
| G27 | 4 | JSON/NDJSON Source/Destination | Implemented | Verified |
| G28 | 4 | SQLite Source/Destination | Implemented | NotRun |
| G29 | 4 | PostgreSQL Source/Destination | Implemented | NotRun |
| G30 | 4 | Data Conversion | Implemented | NotRun |
| G31 | 4 | Derived Column | Implemented | NotRun |
| G32 | 4 | Copy Column | Planned | NotRun |
| G33 | 4 | Character Map | Planned | NotRun |
| G34 | 4 | Conditional Split | Implemented | NotRun |
| G35 | 4 | Multicast | Implemented | NotRun |
| G36 | 4 | Union All | Implemented | NotRun |
| G37 | 4 | Merge/Merge Join | Planned | NotRun |
| G38 | 4 | Aggregate | Implemented | NotRun |
| G39 | 4 | Sort | Implemented | NotRun |
| G40 | 4 | Lookup/Cache Transform | Planned | NotRun |
| G41 | 4 | Pivot/Unpivot | Planned | NotRun |
| G42 | 4 | Error Output/Reject/Quarantine | Planned | NotRun |
| G43 | 4 | Null/Unique/Range/Regex/Reference | Planned | NotRun |
| G44 | 4 | Row Count/Audit | Planned | NotRun |
| G45 | 5 | 统一任务/批次 Checkpoint Journal | Planned（已交付受支持子范围） | NotRun（完整目标） |
| G46 | 5 | CommitUnknown/事务和故障恢复 | Planned（已交付受支持子范围） | NotRun（完整目标） |
| G47 | 5 | Catalog/Deployment/Environment/Version | Planned（已交付受支持子范围） | NotRun（完整目标） |
| G48 | 5 | 结构化 Events/Logs/Trace/Metrics/Lineage/Audit | Planned（已交付受支持子范围） | NotRun（完整目标） |
| G49 | 6 | 薄 C/.NET/Java Package/Execution 绑定 | Implemented | Verified |
| G50 | 6 | 独立 C/Native/NuGet/Maven 安装消费 | Implemented | NotRun |
| G51 | 6 | Linux 原生与 ABI 门禁（Windows 免测） | Planned | NotRun |
| G52 | 6 | 内存安全/线程竞态/资源故障门禁 | Implemented | NotRun |
| G53 | 6 | 九项附件核心集成验收 | Planned | NotRun |
| G54 | 7 | Slowly Changing Dimension | Planned | NotRun |
| G55 | 7 | Fuzzy Lookup/Fuzzy Grouping | Planned | NotRun |
| G56 | 7 | Term Extraction/Lookup | Planned | NotRun |
| G57 | 7 | CDC | Planned | NotRun |
| G58 | 7 | 并行分区/Window/BI Dataset/Query | Planned | NotRun |
| G59 | 7 | ODBC/SQL Server | Planned | NotRun |
| G60 | 7 | MySQL | Planned | NotRun |
| G61 | 7 | HTTP 集成 | Planned | NotRun |
| G62 | 7 | SFTP | Planned | NotRun |
| G63 | 7 | Kafka | Planned | NotRun |
| G64 | 7 | RabbitMQ | Planned | NotRun |
| G65 | 7 | Cloud Object Storage | Planned | NotRun |
| G66 | 7 | Excel | Planned | NotRun |
| G67 | 7 | Parquet/Arrow | Planned | NotRun |
| G68 | 7 | Microsoft 专属扩展和兼容边界 | Planned | NotRun |

最新模块：原生 Project/Environment 绑定、四层作用域及共享连接已通过；Catalog 固定部署修订的选择、版本切换和回退已验证。G47 的激活路由/保留/完整历史和 G48 的完整观测仍保留待办。证据：acceptance/evidence/ssis-project。

Execute Package Task 已从草稿容器别名改为真正子 Package：参数映射、作用域隔离、同一 Execution/资源上下文、子/孙 Package、错误/取消/CommitUnknown 和真实终止恢复已验证。证据：acceptance/evidence/ssis-child-package。

范围修订（2026-10-12）：Windows 测试及远程 CI 状态按用户明确指令不作本轮完成门禁；其余实现及本地验收范围不变。这是显式免测，不是测试通过。

运行历史模块：原生 Ready/终态快照与事件持久化、取消后的元数据读取、租户边界、参数正文不落盘已通过。当前 native 26/26、内存专项 8/8、三语言子 Package 一致性通过；完整 G47/G48 仍有缺项。

File System Task 已实现并验证四种受限操作、逻辑连接/删除授权、纯配置预校验、路径/覆盖/原子发布/取消/未知恢复。最新 native 27/27、内存专项 9/9、三语言同文件复制与子 Package 通过；完整目标其他缺项继续保留。

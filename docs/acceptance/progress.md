# SDK 0.2.0 实施记录

基线：用户会话清单。基础提交 d86e18e，当前开发分支 `codex/sdk-0.2.0`。原 `codex/initial-sdk` 已经通过 PR #1 合并且远端分支已不存在。用户最新指令（2026-10-12）是在已知代码未完整实施及验收的情况下，立即提交、推送、创建新 PR 并合并当前源码；此次源码集成不表示需求验收或发行完成。

## 阶段状态

| 阶段 | 代码状态 | 运行验收 | 已写入的行为 / 剩余工作 |
|---|---|---|---|
| A | InProgress | NotRun | ABI/文本计划/Checkpoint/Worker/wire 2；Decimal128、Date32、Time64、UTC Timestamp、Binary、Struct/List/Map 列存、递归 Arrow、深复制和转换。已加入嵌套 JSON 交换。完整格式交换、全部边界及语言类型绑定仍待完成/验收。 |
| B | InProgress | NotRun | 类型化 PostgreSQL/SQLite 插件、连接测试、Schema/权限发现、有界游标、Append/Upsert/Delete 和目标类型校验。已加入有界 NDJSON 游标和文件位置恢复。HTTP、键集/分片公共模型、Overwrite/Replace、完整生命周期门禁仍待实现/验收。 |
| C | InProgress | NotRun | 字段映射、常量、NULL 默认值、类型转换、ASCII Trim/大小写；非空、唯一、范围、主外键、行数对账及隔离确认回调。已加入内置 UTF8 NFA 正则及 SDK 文件隔离存储，支持重开读取、幂等摘要、授权与损坏拒绝。外存 NDJSON Join/聚合代码及配额、偏斜、Decimal/NULL 夹具已写入。完整表达式/分支、独立正则插件、质量状态恢复及真实终止进程验证仍待实现/验收。 |
| D | InProgress | NotRun | 已写入结构化 DAG JSON/C 构建、拓扑/路径/端口校验和类型化参数绑定，以及目标批次凭据和版本策略。新增独立 Run/后台线程、每边有界队列、端口轮转、分支深复制、节点状态/指标及 CSV/NDJSON → Pass/Cast → NDJSON 原子文件发布和自定义节点。DAG 持久恢复、数据库/质量内置节点、与 CSV 统一恢复执行器、复合水位线、并行分区、有限重试及真实崩溃验收尚未完成。 |
| E | Planned | NotRun | pgoutput、复制 slot 导出快照、I/U/D 事务衔接、LSN 确认、重放与 Schema 变化处理尚未实现。PostgreSQL 插件不声明 CDC 能力。 |
| F | InProgress | NotRun | 类型化连接器已有凭据解析、授权回调及拒绝默认值。调度、任务依赖、日志/指标/Trace、审计/脱敏全链路、血缘、gzip/zstd/扩展 Parquet 尚未完成。嵌套 JSON 及 NDJSON 已写入。 |
| G | Planned | NotRun | PostgreSQL 任务存储、租约/fencing、三个 Worker/两个宿主、序列化通信、BI/ELT 版本化数据集/原子发布尚未实现。 |
| H | InProgress | NotRun | C API 新入口、ABI 2 和 .NET/Java 版本/CSV 示例同步。已接入 .NET/Java 类型化 Batch/Schema/wire/转换、DAG 契约/参数绑定、Run 启动/等待/取消/指标/节点状态/授权回调及统一错误码，写入三语言数据与同 DAG 文件执行夹具；完整连接器/质量/恢复/调度/Worker 绑定、安装包、消费、OS/内存/真实 DB/S01–S05/基准验收尚未完成。源码 PR 按用户最新指令单独集成，不改变本阶段的验收状态。 |

任何阶段均未标为 Complete。静态检查记录是开发证据，不是运行验收证据。没有把基础提交文档中的历史说明当作新验收结果。

## 本轮静态检查

- Tree-sitter C/C#/Java 语法解析，不调用编译器、不运行 SDK。
- 公共函数实现、Foundation 单向依赖、示例私有 API、JSON/XML/Python AST 及许可检查。
- `git diff --check`。

具体日志、源码摘要和命令元数据保存在 `evidence/static.json`。构建、CTest、数据库测试、语言消费、内存检查及基准均未执行，遵守先完成全部阶段代码的开发顺序。

## 环境及阻塞

- 一次性本地数据库测试所需的 Docker 守护进程可访问；未运行数据库验收容器。
- 已在 `work/toolchains` 准备官方预编译 .NET SDK 8.0.425、Java 17.0.20+101、Maven 3.9.11 并验证官方 checksum 和版本命令，证据为 `evidence/toolchains.json`；没有编译 SDK。libpq 17.11 开发包已根据 Debian 官方索引的 SHA256 验证并解包，配套系统已有 libpq 17.11 运行库；未执行数据库测试。
- 当前宿主为 Linux；Windows/MSVC 运行证据不可由此环境产生。
- 最近 GitHub Actions 运行 38160104876 的原生/Linux/Windows/内存任务均没有任何已执行步骤。check-run 114531030866 明确报告账户支付失败或额度限制，已保存实际 API 响应；不能作为验证通过。
- 当前源码按用户最新指令提交、推送、创建 PR 并合并，PR 必须明确记录未完成能力及 NotRun / Blocked 状态。验收与发行门禁仍未通过，没有把当前开发代码当作发行包。

## 关键当前契约

类型化连接器的目标业务写入和批次凭据位于同一事务。同一 tenant/job/batch_id 的不同目标、键、内容或游标摘要直接报 Corrupt；CommitUnknown 不能推进源位置。SQLite Decimal 使用 TEXT、时间使用 INTEGER，PostgreSQL 使用 numeric/timestamptz 等明确类型，避免隐式精度或时区转换。未实现的 Overwrite / CDC 不声明能力。

质量实例处理失败会失效，隔离回调失败不会推进 processed 指标。SDK 隔离存储已实现原子不覆盖发布及文件/目录同步，但其重开/冲突/损坏夹具和真实进程终止验证尚未执行，不能据此宣称持久性验收通过。新增测试源码覆盖 Decimal/时间/嵌套 Arrow/失败追加、映射、质量确认，以及 SQLite 10,000 条全量、200 新增/50 更新和逐行对账。PostgreSQL 容器、真实进程终止、三语言同计划及分布式验收仍缺执行链路/证据。

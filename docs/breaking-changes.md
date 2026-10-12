# 1.0.0 / ABI 3 破坏性变更

- 删除 `ud_job`、`ud_snapshot_latest`、`ud_dag`、`ud_run` 与其全部生命周期；删除 engines/etl 的 CSV/DAG 两套执行器。没有重定向、适配器或旧模型 JSON 导入。
- 唯一入口为 `ud_package_create`、`ud_execution_create[_json]`、execute/start/wait/cancel/metrics/destroy。Control Flow 与 Data Flow 是不同模型；Pipeline 不提供独立启动句柄。
- 删除 .NET/Java 的 CsvJob、Dag、Run 类，使用 Package 与 Execution。Batch 保留并增加 sealed 引用所有权。
- SDK 为 1.0.0；native SONAME、C ABI、插件 ABI、Package、Schema、Plan、Wire、Worker 和新 Checkpoint 为 3。旧值明确返回 Incompatible，不尝试猜测迁移。
- Sort、Slice、Join 集合 API 从 Query 移到 Compute，以共享类型/比较/NULL 行为。旧符号删除；Query 的纯计划调用不是独立运行时。
- CLI/Worker 只执行 Package。Worker 协议是 u32 帧长度加 Package JSON，回复 version/state/result JSON；没有旧 Query/Plugin 命令路径。无授权回调默认拒绝资源访问。
- SQLite/PostgreSQL 物理驱动 ABI 新增 Execute SQL 能力；任务与 SQL Source/Destination 引用同一 Connection Manager 定义。旧插件必须重新编译。
- Quarantine 文件存储保留为 Runtime 持久化原语，非执行器。历史 0.2.0 文档放入 docs/history；其验证不能满足新 Package 验收。

实现/验证分别记录在 docs/acceptance/ssis-status.json。数据库 schema 未自动迁移；旧 SDK 已产生的数据与检查点不被本重构修改。

草稿 ABI 3 的 `executePackage` Task 配置现为 `{package: <规范 Package>, parameters: {name: <表达式 AST>}}`。之前以 `children` 表示 Sequence 别名的草稿配置会返回 SCHEMA；这不是正式发布格式迁移，也不保留适配路径。

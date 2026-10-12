# 目标代码结构与单向依赖

完整产品/技术设计及三次反思见 [Phase 0–7 设计](phase-0-7-design.md)。

| 路径 | 唯一职责 | 依赖 |
|---|---|---|
| components/foundation | allocation/quota、平台文件/锁、JSON、hash、线程、诊断 | C17 + Windows/POSIX |
| components/data | schema owner、identity/lineage、typed value、sealed Batch、Arrow/wire/codec | Foundation |
| components/contracts | Registry、plugin module、physical connector 调用契约 | Columnar |
| components/expression | typed AST、binder、函数注册及求值 | Columnar、Plugins |
| components/execution | 状态模型、bounded queue、Buffer Manager | Columnar、Threads |
| components/compute | typed collection kernels、JSON Plan、stream operators、quality/profile | Columnar、Expression、Runtime |
| engines/package | immutable Package、root Execution、Activation、绑定、Journal | 上述基础设施 |
| engines/control-flow | task/container/precedence/handlers 调度 | Package 内部帧、Registry |
| engines/data-flow | 端口/path/execution trees、组件生命周期和 backpressure | 同一 Package、Compute、Manager |
| connectors | 逻辑连接定义、授权/resolver、Activation lease | Package、connector 契约 |
| catalog | immutable artifacts/deployments/metadata/history | 唯一 Package 状态和事件 |
| plugins | SQLite/PostgreSQL physical protocols | public host/connector ABI |
| hosts / bindings | CLI/Worker、C# PInvoke、Java JNI | public ABI |

删除范围：旧 engines/etl、旧 Job/Dag/Run；engines/query 的格式专属外存实现及无语义转发层；Pipeline 全输入缓存；Compute 文本 schema/predicate DSL；独立 quarantine store；generic operation plugin API；重复 ABS/CSV plugin；ODBC/DuckDB 未接入统一链路的原型和相应旧测试。

合并范围：Query/ETL 集合计算进入唯一 Compute operator；阻塞状态、wire spill 与配额归 Buffer Manager；所有纯函数进入 Registry/Expression；quality 只分类，错误持久化使用正常 Pipeline destination；child/task 可变状态从根迁移到 Activation。

Windows/POSIX 只有基础设施差异，同一组件及算子源码同时支持两平台。本轮 Windows 实际门禁，Linux 按用户约定免测。

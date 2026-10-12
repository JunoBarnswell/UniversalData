# 本轮交付范围与真实状态

用户已确认：先完成统一架构与核心组件并验收，协议及高级扩展保留真实状态。Windows 编译/测试，Linux 保留实现并免测。Phase 0–7 指本轮统一架构和核心链路；原 G00–G68 的完整扩展目录继续追踪，不声称全部实现。

核心交付要求：唯一 Package Runtime；独立 Control/Data Flow；唯一 Schema/AST/Compute/状态/资源/错误契约；纯 C17；没有旧 ABI、旧 Job/Dag/Run、generic operation、重复 Query/spill/quarantine 执行路径；真实 Windows 内存/配额/并发/事务/恢复/安装消费证据。

权威设计：[Phase 0–7](architecture/phase-0-7-design.md)。结构和删除合并矩阵：[目标代码](architecture/target-architecture.md)。实际组件和扩展边界：[组件清单](product/component-catalog.md)。状态与可复查证据：[当前状态](acceptance/ssis-status.json)、[69 项清单](acceptance/completion-goal.json)、[门禁](acceptance/gates.json)。

`python scripts/release_guard.py` 核对当前源码 fingerprint、必需核心门禁和证据哈希；不运行编译、发布或合并。原全目录目标、先前 Linux/ABI3/0.x 记录属于历史，不得用于证明当前核心交付。范围外的 Planned/Partial/未验收项目不能显示为完成。

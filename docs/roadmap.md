# 按依赖拓扑实施

```mermaid
flowchart LR
P0[审计/设计] --> P1[共享基础/ABI]
P1 --> P2[Package/Control]
P2 --> P3[Pipeline]
P3 --> P4[Managers/Components]
P4 --> P5[Recovery/Catalog]
P5 --> P6[Bindings/Delivery]
P6 --> P7[Extensions/BI]
```

| 批次 | 文件/处置 | 完成判据 |
|---|---|---|
| 0 | docs/product、architecture、adr、破坏性矩阵 | 设计/所有权/迁移/官方证据无矛盾，提交 draft PR |
| 1 | include/ud/execution、expression、registry；data、compute | 单一状态/表达式/共享 Batch；Query 算子内核统一；边界测试 |
| 2 | engines/package、control-flow | Definition/Binder/作用域/容器/约束；DataFlow 正式调用 Pipeline |
| 3 | engines/data-flow；删除 etl.c/run.c/dag.c 旧执行路径 | 生命周期、多输入输出/End/有界队列/所有权与取消 |
| 4 | connectors、组件 Registry 和内置插件 | 文件/SQL 经 Manager；derive/split/union/join/sort/aggregate/error output 原生模型 |
| 5 | package/checkpoint、catalog、events/metrics | 同一任务/批次日志、回执/CommitUnknown/原子提交；故障门禁 |
| 6 | bindings、hosts、examples、CMake/CI | 删除旧入口、同一 Package 三语言、ABI/安装消费者 |
| 7 | 新插件与共享 Planner/Compute | 按目录 P1/P2 分期，不能把未实现项目标 Verified |

每批次独立 commit 并推送同一 draft PR，不合并、不发版；按影响范围集中验证。状态分 Planned/Implemented/Verified/Blocked，记录命令、提交、日期与真实结果。Windows 不可在 Linux 伪造验证；CI 额度失败为 Blocked。未完成恢复/组件不能伪装通过。

典型纵向验收：含控制循环/参数的 Package→CSV/SQL Source→Derived/Split→多目标；NULL/Decimal/UTC 对照 Query；同步共享/异步 spill；取消/超时/资源超限/拒绝授权/CommitUnknown；C/.NET/Java 同定义运行；插件 unload 生命周期。暂不兼容微软 proprietary artifacts。

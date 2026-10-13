# PR #5 Native 基础与高级 ETL 架构

当前补充附件优先。基准是实际 PR HEAD fd03e6508b0a13c4e418225c91bd60e56c547418；附件中的 204c3ea 是历史观察。用户明确要求 PR #5、批量实现后集中验收、Linux 必验、Windows 豁免。旧测试已停止。研究配套文件未上传，代码是能力判定依据。

## 初始源码事实

这里的 L1 仅表示实际算法或回调存在，不表示本轮 ABI 9 通过验收。L2 必须有当前版本真实集成日志；L3 必须包含独立 SDK 消费者。历史 ABI 7 日志不提升新交付等级。

| 能力 | 接口声明 | 实际实现 | 集成验收 | 缺陷与约束 |
|---|---|---|---|---|
| SQLite Prepared | include/ud/connector.h | plugins/sqlite.c read_open 先 prepare/bind，read_batch 只 fetch，L1 | 本轮 NotRun | 附件所述未声明 s/options 已在实际 HEAD 消除；禁止移除绑定 |
| PostgreSQL Prepared / Receipt | include/ud/connector.h | plugins/postgresql.c typed 参数、同事务 Receipt、显式 provision，L1 | 本轮 NotRun | 新显式 provision 尚需迁移所有调用方 |
| MySQL / MariaDB / OCI | plugins/CMakeLists.txt | 基准无实现文件 | NotRun | 客户机来源必须分别证明，服务端不能使用 Mock |
| ODBC | include/ud/connector.h | plugins/odbc.c 实际 SQLGetInfo/BindParameter/GetData，L1 | NotRun | Oracle/DB2 写入、Changeset、取消句柄生命周期尚不完整 |
| SQL 类型映射 | include/ud/data.h | components/data/src/schema_evolution.c 原先三种方言，L1 | NotRun | 厂商 UTC、Unsigned、RowVersion、LOB 不可用通用映射掩盖 |
| 组件声明 | include/ud/component.h | components/contracts/src/registry.c / engines/data-flow/pipeline.c，L1 | NotRun | Schema 文案和独立 allowlist 重复；部分关系声明仅 object |
| 动态端口 | include/ud/component.h | pipeline.c 绑定 Stable ID/name；builtins.c N 路分支/合流，L1 | 历史 ABI 8 局部真实集成；ABI 9 NotRun | Schema 在绑定阶段冻结 |
| 六类 Join / Merge | include/ud/operator.h | compute/src/relational.c 共同 merge 算法，L1 | 历史 ABI 8 SQL 对照；ABI 9 NotRun | 基准 strategy 未真正决定 Hash/GraceHash |
| Window | include/ud/operator.h | compute/src/operator.c 仅 rowNumber/runningSum，L1 | NotRun | Rank/Lag/Lead/Frames 不能按已有枚举认定完成 |
| Aggregate | include/ud/compute.h | compute/src/compute.c / plan.c 支持 count/sum/min/max 与部分合并，L1 | NotRun | Distinct/ApproxDistinct/条件聚合缺失 |
| Pivot/Unpivot/Sampling/SCD | include/ud/operator.h | compute/src/relational.c 原生分类与有界状态，L1 | 部分历史集成；SCD 未通过；ABI 9 NotRun | SCD 乱序重算及缓存 schema 必须闭合 |
| Quality / Cache | include/ud/quality.h / cache.h | compute/src/quality.c / cache.c，L1 | NotRun | assert 应复用 Quality；远端 Partial/Negative Cache 缺失 |
| Regex / Profile / Expressions | include/ud/regex.h / profile.h / expression.h | compute/src/regex.c NFA；profile.c 四类统计；expression.c 同一 AST，L1 | NotRun | 命名捕获、批向量核、完整画像待实现 |
| Arrow / Wire / XLSX | include/ud/data.h / xlsx.h | data/src/arrow.c、wire.c、connectors/xlsx.c，L1 | 历史 XLSX 局部集成；ABI 9 NotRun | Arrow C 内存互操作不能当作 IPC/Parquet 文件驱动 |
| 持续 CDC / Barrier | include/ud/component.h / connector.h | 基准无原生日志消费事件；只有 DATA/END/POLL | NotRun | CDC capability 是 L0；不得宣称全局 Exactly-Once |
| 恢复 | execution/src/journal.c / package/checkpoint.c | 唯一 Journal、Receipt、Source confirm，L1 | NotRun | 持续多输入 Epoch 对齐必须在同一 Queue/ExecutionTree 上完成 |

## 统一设计决策

1. SDK 1.5.0 / ABI 9 / Wire、Plugin、Package、Plan 同一 epoch。原版本在历史证据中保留，不存在双 ABI 回退执行路径。
2. Native Descriptor 的 Configuration Schema 是唯一参数语法。Foundation 的声明绑定器检查 required、枚举、类型、范围、数组、对象，并物化默认值。Registry、纯 Configure、直接算子绑定与 SDK 查询使用同一声明；字段 ID/类型证明属于语义绑定，不是第二份参数语法。
3. 端口固定 Schema、Stable ID、排序、分区证明在纯绑定生成。配置绑定不访问网络或数据库。prepare 获得授权 Lease；step 消费有界事件；finish 完成效果；confirm 才推进源位点。
4. 物理计划由 Compute 给出确定性选择与资源预算。未知统计明确为未知；已有排序优先 Merge，适配内存的构建侧选 Hash，超出预算选分区 Spill。所有算法共享类型比较、键哈希、结果行构造和 Buffer 状态，不能按组件再实现一个 Join。
5. MySQL 和 MariaDB 分别编译实际厂商 SDK，共用 Prepared 语句生命周期与 Typed Batch 协议；ODBC 是一个 Driver Manager 实现；OCI 是独立 Native 客户机。连接创建、权限、租户、取消、Reset、Receipt 全由唯一 ConnectionManager 管理。
6. SQL 值全部绑定。业务 DML 和 Receipt 同事务；Receipt DDL 只允许显式授权 prepare 前 provision。外部事务不能隐式提前提交；CommitUnknown 禁止 Source confirm。MySQL/MariaDB 先证明目标为 InnoDB。无时区 datetime 只有明确 UTC 策略才能映射 Timestamp；Oracle 空串及 NUMBER 精度必须显式处理。
7. 有状态关系、质量、文本、嵌套和匹配计算只归属 Compute / ud_operator。DataFlow 组件只绑定资源、翻译统一错误和发射端口。状态驻留与 Spill 由既有 Buffer 管理，引用和 Wire 生命周期不另设管理器。
8. DATA、END、Watermark、CheckpointBarrier、SchemaChange 共用有界 Queue。多输入屏障阻止该输入越过 epoch；对齐后状态与源位置进入唯一 Journal。Sink Receipt 核验完成后才能确认该 epoch。不能重放的源或无 Receipt 的目标声明保证边界；跨独立资源不宣称分布式原子事务。
9. 高级窗口与全文/时间去重要求明确有限 Frame / state budget。Fuzzy 使用候选索引，不做输入规模乘参考规模的全表扫描。Schema 与候选字段预绑定；动态 Pivot 列先发现后冻结，运行中拒绝 Schema 修改。
10. Native Catalog / Explain / Diagnostics 是 C、CLI、.NET、JNI/Java 的事实源。跨语言层不执行私有 ETL。当前开发阶段不运行编译、回归和压测；整批源码闭合后集中验证。

# PR #5 补充范围的统一实施决策

用户于本轮明确继续 PR #5、Linux 验收、Windows 豁免；补充附件关于独立实施 PR 和 Windows 必验不覆盖该明确请求。基础 Head 为 643aeb4d733722d5e831a7fc70c1ef6c98752374；SDK 1.4.0 / ABI 8，一次升级整个结构与协议族。

| 领域 | 唯一 owner | 收敛结果 |
|---|---|---|
| 实例端口 | 既有 ComponentDescriptor 的 bind_ports + Pipeline Binder | 同一注册契约按绑定配置生成稳定 ID/名称/方向/角色；Explain 输出真实拓扑 |
| Batch/Identity | Native Columnar | Batch 的可复用追加工作区与规范二进制值摘要；Row/Rule 信息保留 |
| Join/Sort/Window | Compute Operator | 一个关系 Join 语义与 Cache/Sort 物理算法；旧 Inner-only 实现收敛到相同类型内核 |
| 并发/持久化 | Root Executor / Journal | 移除持久目录强制单树；Journal mutex 串行持久帧，目标 Receipt 决定外部效果 |
| SQL 参数 | 既有 Connector read command / execute / write | 参数是 Canonical Batch；插件只负责物理绑定、方言、事务和 Schema |
| SQL 驱动 | 原生独立插件 + 一个 ODBC 插件 | MySQL/MariaDB 客户端分别链接；所有 ODBC 供应者共用同一控制/类型/Receipt 路径 |
| CDC | 既有 Source Cursor/Journal/Lease | 正式 Canonical Change Envelope，日志位置只在全部目标提交屏障之后确认 |
| 文件/网络 | 既有 Lease、Stage、Receipt | XLSX/Parquet/HTTP/对象只做物理差异，不新增执行或确认状态机 |
| 语言 SDK | C ABI / 现有 Package/Execution | C# 与 Java 不实现 Compute/交易/CDC 语义 |

完整范围 E01–E15、N01–N11、D01–D12 纳入产品状态账。按 A 底层、B 驱动、C 企业 ETL、D 扩展发行整批开发后集中验收。已经落地的 Merge/Pivot/Sampling/XLSX/SCD 保留在同一内核；该补充要求的动态端口、迟到 SCD 等进一步收敛到相同 owner。

没有服务器/供应商库的验收精确记 NotRun，不把代码缺失写成已完成。供应商驱动不随包擅自再分发。没有 merge/release 自动授权。

# R1–R5 范围与主分支源码证据

本轮基线为 main `d2d44131423963185c82a1c0ec6ff5e2330db191`。PR #4 已于 2026-10-12 20:33:45 UTC 合并；开发分支为 `codex/etl-next-phase-r1-r5`。用户将范围限定为 R1–R5，Linux 必验、Windows 豁免。附件的 R0 不是新增功能阶段；合并事实与历史证据归现有权威账本维护。

| 阶段 | 现有证据 / 根因 | Keep / Rewrite / Delete | 唯一 owner | 验收 |
|---|---|---|---|---|
| R1 | quality.c 有 FK，但 transforms.c quality_create 不绑定 Reference；apply 在首个失败处 break；错误行身份只用组件累计行号 | 保留规则语义；重写完整绑定/多 violation/政策；删除重复 key 比较和不完整错误协议 | Data 行身份、Compute Quality、正规 Pipeline error port | FK/MATCH SIMPLE、多规则、双目标真实恢复、低配额 |
| R2 | 文件已有摘要/receipt 重放；SQL_SOURCE 缺可持久重建快照与 typed composite cursor | 保留 Lease/Reader/Journal；新增同 owner 的快照帧与确认游标；不造增量执行器 | Source + Data Cursor + 现有 Journal | 同水位多键、部分提交、断连、分区恢复 |
| R3 | discover 输出元数据，缺 canonical snapshot/diff/policy | 保留 metadata/discover；类型映射、fingerprint、diff、mapping 统一到 Data | Data Schema；Source/Sink preflight 调用它 | 三策略、精度/NULL/列 ID/嵌套/权限 |
| R4 | operator.c join_pull 逐行扫描右侧；lookup 与 join 共用这一逻辑，无缓存资产生命周期 | 删除扫描内核；使用 Compute 内唯一有界有序索引，复用 Sort/Buffer；Lookup/Join/FK 共用查找 | Compute cache；Execution 拥有声明的缓存资产 | 复合键、重复策略、Spill、两 Pipeline 共享只读状态 |
| R5 | 旧百万行数据真实反映并行 Sort 变慢；缺 tree 时间及 codec/I/O 归因 | 保留 pool/ready queue；增加原有 owner 指标与工作粒度对照；禁止自动调并发 | Executor / Buffer / Pipeline | 六负载、低内存、一致性与实际峰值 |

稳定 Source Row Identity 需要进入 sealed Batch 的原生元数据，并随 Wire/Spill、投影、分区、排序与 Join 保留。因此有意识地升级为 SDK 1.3.0 / ABI 7，所有协议同批切换，ABI 6 记录仅作历史证据，不引入旧布局读取或兼容层。C/.NET/JNI 只投影 Native 结果。

# R1–R5 Linux 验收结果

main 基线 `d2d44131423963185c82a1c0ec6ff5e2330db191`；实现提交 `fc028063fdfafd07719fe8011630b9f5305423c8`。SDK 1.3.0 / ABI 7，源码指纹 `d7387494a8455cdc45cd661c2987256305c1894b50d31bad2eb60ed0cd511696`。Windows Exempt。

Release 与实际 ASan/UBSan 均 40/40，最小 Foundation/Columnar 5/5；真实 ZIP 的 53 文件、动态 C、独立 NuGet 原生加载、独立 Maven/JNI 和三个语言查询通过。SQLite 双正常/错误目标、多规则 FK、两个分区不同批量的独立进程崩溃、权限撤销、Wire 损坏及 Schema Drift 拒绝通过。PostgreSQL 验证实际服务器取消、断连 CommitUnknown，另以单个物理会话完成 1003 行 typed composite source → Canonical Schema preflight → snapshot → SQL upsert，254 行提交后杀进程、独立恢复并逐列对比结果。

Linux 6.18.44 / glibc 2.41，主机可见 5 CPU，cgroup 配额 4 CPU。各样本 100 万事实、10 万参考，Native Buffer resident=64 KiB、working=4 MiB、spill=1 GB、Context=64 MiB。六负载 19 样本均验证正常/错误行数与不同并发的规范内容；增量还校验所有 PK 范围及每行 FK 值。下表 wall 单位秒；只测本次实际行为，不设速度承诺。

| 负载 | Workers | Wall | CPU | RSS 峰值 KiB | Native Spill 峰值 bytes |
|---|---|---|---|---|---|
| streaming | 1 | 2.137 | 2.134 | 12288 | 0 |
| streaming | 2 | 2.200 | 3.857 | 12416 | 22883 |
| streaming | 4 | 2.235 | 4.072 | 12544 | 41559 |
| streaming | 8 | 2.161 | 4.477 | 12544 | 45766 |
| sortSpill | 1 | 5.473 | 5.471 | 12544 | 145845736 |
| sortSpill | 2 | 5.536 | 6.984 | 12544 | 145831912 |
| sortSpill | 4 | 5.597 | 7.131 | 12544 | 145831912 |
| sortSpill | 8 | 5.562 | 7.139 | 12544 | 145831912 |
| join | 1 | 7.636 | 7.633 | 12544 | 80694394 |
| join | 2 | 6.538 | 9.352 | 12544 | 81674869 |
| join | 4 | 7.662 | 11.194 | 12672 | 83589714 |
| join | 8 | 9.326 | 14.101 | 19580 | 87401159 |
| errorDualSink | 1 | 2.260 | 2.258 | 12672 | 79337 |
| errorDualSink | 2 | 2.087 | 3.217 | 12672 | 158674 |
| errorDualSink | 4 | 2.220 | 3.690 | 12800 | 158674 |
| incrementalResume | 1 | 2.814 | 2.801 | 24644 | 0 |
| incrementalResume | 4 | 2.723 | 2.716 | 24580 | 0 |
| cacheLookup | 1 | 10.241 | 10.238 | 15524 | 15497256 |
| cacheLookup | 4 | 7.070 | 19.921 | 15524 | 15497256 |

增量行的表内数据是恢复进程；首次捕获/终止进程单独保存在 `interruption`。两次均在 6144 行目标确认后退出，游标在完整 DataFlow barrier 前不推进；完整捕获 + 恢复 wall 分别 1 Workers=9.102s, 4 Workers=8.980s。普通文件水位模式按声明顺序验证，快照正文归 Journal，不属于临时 Buffer Spill；不能用表内 0 Spill 推断没有持久磁盘 I/O。Journal 块仍保留在 State Directory，临时 Spill 清理则逐样本实际断言。

Streaming/错误双目标受解析、编码和窄 Pipeline 吞吐约束；增加 Workers 同时增加 CPU 和排队，不提供稳定线性收益。Join 2 Workers 最快，8 Workers CPU/同步成本扩大且超过 4 CPU 配额；JSON 同时保留逐 Node CPU/prepare/finish、Tree ready/first/end/quantum/waits 与 Buffer codec/read/write 指标。Cache Lookup 的两个 reader 在 4 Workers 可以重叠 CPU，wall 降低，但总 CPU 明显增加。Sort 的小 Batch=256 低于静态工作粒度 4096，增加宽度没有有效的细粒度计算收益。没有自动扩大配额或调整并发。

相同 CSV 降序百万行 Sort，字段/内容与 ABI 6 历史基线完全相同，输出 SHA-256 均为 `499abcf3852d58e7c0eebc40a7b10293bcb1763087cc758eff5dbf9aef734ce2`。历史测量与当前测量时间不同，应按方向性回归证据理解，不能归因为单个改动。

| Workers | ABI 6 wall | ABI 7 wall | ABI 6 total Spill | ABI 7 total Spill |
|---|---|---|---|---|
| 1 | 1.831 | 4.273 | 80015232 | 277870720 |
| 2 | 2.072 | 4.002 | 135753824 | 337871640 |
| 4 | 2.269 | 4.048 | 127401440 | 326360400 |
| 8 | 2.614 | 4.020 | 137326304 | 326950720 |
| 16 | 2.833 | 4.422 | 138858048 | 329902320 |

该 Sort 存在真实回归。ABI 7 的稳定行身份/规则元数据随 Wire、Sort、临时 Block 和目标传递，额外逐行哈希及更大的编码量增加 CPU 与磁盘读写；总 Spill 约为旧版的 2.4–3.5 倍。这不是统一吞吐提升的交付声明。当前版本优先保证身份、恢复和错误交付语义，后续优化应保留本次内容/行身份及同预算基准。

唯一权威状态仍为 `metadata/product.json`；此页解释机器 JSON 和日志，不建立第二本状态账。原 ABI 6 日志仅保留为历史，包括其失败中间尝试，不替代当前结果。GitHub Actions 在 main 的实际运行因账户账单/消费额度限制未启动步骤，不能记为通过；新 PR 的远程状态按实际运行更新。

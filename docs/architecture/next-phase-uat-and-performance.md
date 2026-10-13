# 五阶段集中验收与性能计划

代码整批完成后，集中 Linux Release/JNI/.NET/Java、ASan/UBSan、隔离 SQLite/PostgreSQL、真实进程崩溃恢复和安装消费。Windows Exempt。构建/测试/产物的源指纹必须一致，历史 ABI 6 成功和失败均单独保留，不代替当前证据。

纵向 Package：typed composite source snapshot → schema preflight → derived/cast → FK/多规则 Quality → accepted cache lookup / regular error target → SQL/File receipts → Journal → confirmed cursor → Native diagnostics。用多个进程在读取、提交和日志边界终止，再恢复，检查部分效果与未知提交。

类型覆盖 NULL、Decimal、NaN/Infinity、UTC、Unicode/NUL、Binary、Nested、Column ID/Lineage 与稳定 row identity。压力覆盖 100 万事实行 / 10 万参考键、10 万错误行、低 resident/working/spill、两个缓存 reader、不同输入速度、取消与授权撤销。

六个负载记录机器、源码、配置、Rows/s、wall/CPU、RSS、Tree ready/running/blocked/end、Scheduler signal/quantum、Buffer encode/decode/读写放大：Streaming、Sort Spill、Join、Error Dual Sink、Incremental Resume、Cache Lookup。同一 ABI 7 定义和相同数据在不同 Workers 间比较；ABI 6 的 CSV Sort 历史数据单列，不能当作相同负载的前后优化证据，性能无收益则报告实际瓶颈，不自动调整全局并发或扩大配额。

验收代码：`tests/next_phase.c`（typed cache/cursor/schema、真实 Wire/Spill）、`tests/next_phase_pipeline.py`（FK/多规则/正规双目标与进程 kill）、`tests/next_phase_execution.c`（原生事件边界与完整诊断）。性能矩阵为 `benchmarks/next_phase.py`；六负载按附件的 1/2/4/8、1/4 宽度运行，包含百万事实/十万参考，另记录十万错误压力。

当前静态 Sort 工作粒度为 4096 行；小 Batch 不启动无收益的细分 Work Group，仍执行同一稳定 Sort。不存在自适应并发。节点诊断分开 prepare、step、finish CPU/wall；Tree 记录 ready/first/end、busy/quantum；Buffer 记录 encode/decode CPU、bytes、I/O、实际峰值。源码、命令、二进制和 JSON 证据以同一源指纹封存。最终判定只以实际执行结果为准。

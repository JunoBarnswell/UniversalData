# Windows 集中验收

本轮在统一架构及核心源码完成后集中编译和验收；失败后只重跑受影响范围，修复涉及共享运行边界时执行完整门禁。Linux 按用户最新决定免测，核心算子和 POSIX 实现继续支持。

| 门禁 | 真实覆盖 |
|---|---|
| MSVC Debug / JNI | C17、完整 ABI 4、零编译告警 |
| Native CTest | 31 项实际行为；平台进程/文件、类型、表达式、队列、多端口、当前插件、Catalog/Project/Child/History |
| MSVC AddressSanitizer | 同一 31 项；地址安全。MSVC 本轮没有 UBSan/LSan/TSan 结果 |
| Ownership / quota | allocator fault sweep、context allocations 清零、writer fence、CRC/hash 错误、限额与取消清理 |
| Concurrent Activation | 到达屏障证明实际并发、局部超时与兄弟隔离、串行作用域写入、事件顺序、共享 Journal |
| Buffer / Compute | 实际外存多轮 Sort、Window、Aggregate、Join 值一致性、完整 schema/lineage、错误目标提交 |
| SQLite / Recovery | 真实事务、rollback、receipt/upsert/幂等、源/Package 改变、journal 损坏/缺口、实际进程终止和 CommitUnknown |
| C/.NET/Java | 同一 Package、Project/child/参数/10003 行、指标；Decimal/UTC/UTF8/NUL/nested/NaN/Infinity/NULL/wire 逐字节一致 |
| Installed consumers | 仓库外 CMake install 消费、真实 nupkg 消费、独立 Maven repository/JAR 和 JNI 消费 |
| Release distributable | `packaging/package.ps1` 生成版本/RID SDK 和 ZIP；Release Native CTest；逐文件 SHA256 校验 |
| ZIP consumers | 仓库外解压后链接动态 `UniversalData::Data`；ABI/version、资源/并发、SQLite、实际 Pipeline、三语言一致性；NuGet 自带 DLL 且没有 SDK loader PATH |

命令与源码 fingerprint 见 `acceptance/gates.json`，结果日志只保留有限摘要；长日志位于本机临时目录。构建/测试退出码保留。`scripts/static_check.py` 只做契约/元数据/JSON/XML/删除边界检查，不能代替行为证据。

最新完整状态见 `acceptance/ssis-status.json`。PostgreSQL、可选 HTTP 和协议/高级扩展没有本轮系统验收；跨任意目标的全局事务、BI planner、CDC 等不在已完成能力内。

先前的临时目录安装消费记录只证明当时的核心行为。当前产物交付还必须通过 `distributable-bundle` 门禁：磁盘上 ZIP、SDK、JNI、NuGet/JAR 和 manifest 均存在且 digest 匹配，仓库外独立消费成功。`scripts/release_guard.py` 同时检查这些实际文件，无法用仅有测试摘要代替产物。

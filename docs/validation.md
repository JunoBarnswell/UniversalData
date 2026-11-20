# ABI 14 原生 SQL 与 ETL 验收

使用 Windows 和 Linux 的 Release 构建，记录运行平台、源码、数据规模、命令和校验结果。完整构建使用已配置的 SQL 实例。`UD_TEST_POSTGRESQL_CONFIG`、`UD_TEST_MYSQL_CONFIG`、`UD_TEST_MARIADB_CONFIG`、`UD_TEST_ODBC_CONFIG`、`UD_TEST_ORACLE_OCI_CONFIG` 均引用隔离资源的本地配置文件。SQLite 用例创建自己的数据库。

```powershell
cmake --build build/core --config Release --parallel 4
ctest --test-dir build/core -C Release --output-on-failure --no-tests=error -L sql-etl
npm --prefix products/designer run typecheck
python scripts/static_check.py
```

发布验证还运行完整 PRODUCT CTest 和性能矩阵。性能脚本在同一事务模型和数据形状下交替运行逐条写和原生批量写，比对完整结果、影响行数和读回签名。它收集冷连接、首批延迟、读写吞吐、峰值内存、CPU、拉取 P95 和 SDK 批次调用次数。

```powershell
python benchmarks/etl_sql.py --native <benchmark-executable> --plugins <plugin-directory> --output <report-json>
```

SQLite 的端到端对照用同一份数据分别运行快照和流式作业，从目标库观察首次提交，并记录总耗时、队列峰值及回读校验值。

```powershell
python tests/sql_streaming_etl.py --cli <ud_cli> --sqlite <sqlite-plugin> --performance-json <report-json>
```

Studio 的 Chromium 验收使用隔离的 SQLite 源库、目标库和按摘要授权的 C# 转换。启用 SQL 路径时，浏览器会保存流式模式、重新打开作业、预览原生源、发布并执行，再比对目标数据和运行历史。

```bash
python tests/web_host.py --dotnet dotnet \
  --management build/core/hosts/product/management/UniversalData.Management.dll \
  --worker build/core/hosts/product/worker/ud_worker.dll \
  --sqlite-plugin build/core/plugins/ud_connector_sqlite.so \
  --managed-sql-module build/core/tests/managed-sql/ManagedSql.dll \
  --evidence build/core/browser
```

测试通过记为 Pass，断言或已运行的性能门槛不满足记为 Fail，缺独立驱动或验收库记为 Blocked。结果以具体构建和报告为证。

本轮 Linux 实库、Studio 和批量性能结果见 [ABI 14 SQL ETL Linux 验收](acceptance/evidence/sql-etl-linux-abi14/README.md)。

Windows 运行库专项使用只含 Windows 系统目录的 `PATH`，逐进程加载原生库、各宿主目录中的同一原生库，并通过原生插件入口装载已启用的 SQL 连接器。对应命令为 `ctest --test-dir build/core -C Release -R sql_etl_windows_runtime --output-on-failure --no-tests=error`。数据库连接和读写以各自的实库用例验收。

### 历史 Windows 验收

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

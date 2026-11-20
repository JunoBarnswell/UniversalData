# ABI 14 SQL ETL Linux 验收

后续修复及千万行压力测试见 [跟进验收](follow-up.md)。

基于 GitHub `main` 的 `d891d10b89820579edc419fa22363e57aa42adbe`，验证源码提交为 `ae47eb9771bd33ff049649fbcfee98fc5a155058`。Linux 使用 Debian 13 x86-64、GCC 14、CMake 3.31、.NET SDK 10.0.404、JDK 17 和 Chromium 154。性能数值来自验收期间的候选 Release 构建。全部测量使用隔离库。

## 功能

| 验收 | 返回结果 | Pass | Fail | Blocked |
| --- | ---: | ---: | ---: | ---: |
| CORE `sql-etl` | 9 | 9 | 0 | 0 |
| PRODUCT 全量回归（源码 `b868b9b`） | 128 | 105 | 13 | 10 |
| PRODUCT 可移植回归 | 85 | 85 | 0 | 0 |
| Oracle SQL、行命令、原生装载和强类型回归 | 4 | 4 | 0 | 0 |
| MySQL 行命令、CDC 与参与者恢复复测 | 3 | 1 | 2 | 0 |
| 双管理进程恢复隔离复测 | 1 | 1 | 0 | 0 |

`sql-etl` 的九项包含六库实测、v13 Package 离线迁移、SQLite 批量事务以及快照、首批流式提交、取消、错误端口和已提交行数。强类型用例覆盖分页、单条和批量增改删、组合键、空值、十进制精度、跨块冲突回滚和提前关闭游标。托管 ETL 用例还撤销并恢复逻辑连接的读取权限。PostgreSQL 在 SDK 读到首批后终止其数据库会话，后续拉取返回 `Io`，新会话完整回读 100 行。仓库示例在 .NET 8 下完成 SQLite 流式读写和两行业务转换。

Chromium 完成了流式作业的保存、重新打开、只读预览、发布和执行。SQLite 目标逐行比对了 35 条由 C# 转换的记录，SHA-256 为 `a0a89d9c00ab9765de2a2bd574f9e20d730563ddfcc0768424738269fb56bddb`。运行历史标记 `requiresJournal: false`。有 Journal 的快照作业在 Worker 被终止后恢复了相同的 Native 执行 ID。

完整 PRODUCT 的 CTest 退出码为 8。SSIS 的 50,000 行测试完成了 8 个 CSV/XLSX 金额与分支校验场景。逐行 `csv-clean-batch1` 子进程达到 1,200 秒超时，整项 CTest 失败。以下是全量运行的失败项和已有的跟进复测结果。

- `integration_mariadb_cdc`
- `integration_mysql_cdc`（跟进复测 Fail）
- `integration_mysql_participant_recovery`（跟进复测 Fail）
- `integration_mysql_row_command`（跟进复测 Pass）
- `integration_odbc_cdc`
- `integration_oracle_oci_cdc`
- `integration_oracle_oci_native_bulk`（跟进复测 Pass）
- `integration_oracle_oci_participant_recovery`
- `integration_oracle_oci_row_command`（跟进复测 Pass）
- `integration_oracle_oci_sql`（跟进复测 Pass）
- `integration_postgresql_cdc`
- `integration_ssis_mysql_50000`
- `management_multi_process_claim_recovery`（跟进复测 Pass）

完整 PRODUCT 还需要可用的独立消息代理、Schema Registry、SQL Server XA 验收环境和 Windows SSIS Golden 环境。Windows Release 构建为 Blocked，已连接的 Windows 设备离线。GitHub 基线运行的 Windows CORE 作业在构建阶段失败。 [GitHub Actions](https://github.com/JunoBarnswell/UniversalData/actions/runs/44642318700)

## 性能

| 数据库 | 原生 DML 10 万行 | 原生 DML 100 万行 | 10 万宽行和慢消费 | 10 万行乘 4 并发 | 10 万行乘 8 并发 | C# 10 万行 | C# 100 万行 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite | Pass | Pass | Pass | Pass | Pass | Pass | Pass |
| PostgreSQL | Pass | Pass | Pass | Pass | Pass | Pass | Pass |
| MySQL | Pass | Pass | Pass | Pass | Pass | Pass | Pass |
| MariaDB | Pass | Pass | Pass | Pass | Pass | Pass | Pass |
| SQL Server | Pass | Pass | Pass | Pass | Pass | Pass | Pass |
| Oracle | Pass | Pass | Pass | Pass | Pass | Pass | Pass |

原生批量每次调用传入至多 512 行，C 驱动按自身限制继续拆分。在 10 万和 100 万行测试中，每种操作分别调用 SDK 196 次和 1954 次。六库原生回读的校验值分别为 `423d91055f76c410` 和 `5b29f426a3be98a0`。宽行负载为 1,024 字节，每批消费等待 2 毫秒。

单行调用与批量调用的配对比较覆盖六库各 2,000 行、SQLite 10 万行的 1/4/8 并发、SQLite 100 万宽行慢消费及 PostgreSQL 10 万行。已执行的配对用例均满足增改删的批量耗时低于单行调用。MariaDB 使用服务端 `Com_insert`、`Com_update` 和 `Com_delete` 验证了 200 行每类写入在分块 1/64/512 下分别执行 200/4/1 次，回读签名 `be1cfcec1096c3d4`。

| SQLite 行数 | 快照首批提交 ms | 流式首批提交 ms | 流式队列峰值 / 容量 | 回读校验值 |
| ---: | ---: | ---: | ---: | --- |
| 100,000 | 745.3 | 265.0 | 2/2 | `423d91055f76c410` |
| 1,000,000 | 4207.1 | 220.5 | 2/2 | `5b29f426a3be98a0` |

在 100 万行流式样本中，原生工作缓冲峰值为 `107,882` 字节。可控读取边界的功能测试另行验证了在源到 EOF 前目标已提交第一批。吞吐、首批延迟、冷暖连接、拉取 P95、CPU、峰值内存和托管分配保存在同目录的 `results.json`。

剩余性能门禁包括六库 100 万宽行与 4/8 并发的组合、逐库同规模逐行配对、逐库服务端执行计数和各库 ETL 快照对照。扩展样本期间可用磁盘一度低于 6 GiB，8 路百万宽行的单份业务载荷约 7.6 GiB，另需数据库索引、事务日志与作业状态的空间。完整矩阵尚未执行。

## 复现命令

```bash
cmake --build build/core --config Release --parallel 4
ctest --test-dir build/core -C Release --output-on-failure --no-tests=error -L sql-etl
cmake --build build/product --config Release --parallel 4
ctest --test-dir build/product -C Release --output-on-failure --no-tests=error
ctest --test-dir build/product -C Release --output-on-failure -LE 'real-integration|ssis-golden'
ctest --test-dir build/product -C Release --output-on-failure -R '^(integration_oracle_oci_(sql|native_bulk|row_command)|sql_etl_managed_oracle_oci)$'
npm --prefix products/designer run typecheck
npm --prefix products/designer run lint
python scripts/product_model.py check
python scripts/static_check.py
```

Studio 的实测命令见 `docs/validation.md`。性能对照使用 `benchmarks/etl_sql.py`，窄行与宽行的批量样本使用 `build/product/ud_sql_etl_benchmark`，强类型样本使用 `tests/managed_sql/ManagedSql.csproj`。每库连接配置由 `UD_TEST_*_CONFIG` 指向实例的隔离凭据文件。

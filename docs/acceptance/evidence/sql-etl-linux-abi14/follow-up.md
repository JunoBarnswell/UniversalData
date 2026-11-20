# ABI 14 C# 流式写入与恢复跟进验收

Linux Release，基线提交 `505a089`，验收源码 `db53082`。结果来自隔离的 SQLite、PostgreSQL、MySQL、MariaDB、SQL Server 和 Oracle 资源。

## 千万行 C# 流式搬运

源和目标各用一个 SDK 会话。以每批 4,096 行读取，逐行去除 Unicode 文本两端空格并增加十进制金额，再用一个原生事务把全部批次写入目标。计时覆盖从首次拉取到目标提交。随后逐行验证目标全部业务值。

| 数据库 | 成功回读 | 首批可用 | 提交耗时 | 吞吐 | 客户端峰值内存 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQLite | 10,000,000 | 89 ms | 34.04 秒 | 293,745 行/秒 | 94.8 MB |
| PostgreSQL | 10,000,000 | 116 ms | 52.27 秒 | 191,330 行/秒 | 94.0 MB |

两库校验值均为 `82da80c54b8880a5`。读、转换和写入阶段的托管累计分配各约 3.20 GB，包含逐行实体、字符串和数值转换。

独立 SQLite 百万行写入用例以每批 512 行交替运行基线与本轮 SDK，各执行三次。两组逐行回读的校验值均为 `13e7b46efdc29ca5`。

| 百万行 `Fastest<T>().BulkCopy` | 基线中位数 | 本轮中位数 |
| --- | ---: | ---: |
| 写入及提交 | 3.48 秒 | 2.43 秒 |
| 托管累计分配 | 1,015 MB | 217 MB |
| 客户端峰值内存 | 387 MB | 87 MB |

## Release 回归

| 验收 | Pass | Fail | Blocked |
| --- | ---: | ---: | ---: |
| CORE `sql-etl` | 9 | 0 | 0 |
| PRODUCT CTest | 117 | 0 | 10 |
| 单独执行的 MySQL SSIS 工作流场景 | 24 | 0 | 0 |

PostgreSQL、MySQL、MariaDB、SQL Server 和 Oracle 五项 CDC 均通过。PostgreSQL、MySQL、MariaDB 和 Oracle 通过真实进程终止后的准备分支回滚、提交以及部分提交恢复。Oracle 同时验证了 PDB 上的 XA 恢复及 CDB 根上的 LogMiner 增量消费。六库 C# 用例验证了惰性输入、跨分块回滚、取消及 `Fastest<T>` 的跨批增改删。.NET 8 示例验证了两个已转换的 SQLite 业务行。

SSIS 的 `csv-clean-batch1` 使用每批 1 行、两个 Worker、原生目标装载和原有 1,200 秒上限。5 万条业务行在 868.88 秒完成，四个业务分支、汇总和逐行金额核对通过，输出签名为 `d6b6baa7a237d6b58cf2f236b08cb08da4441eeecc597f73f9bafd48e1e36303`。

SQL Server XA 与 SQL Server/PostgreSQL 混合恢复因当前 Microsoft ODBC 的 XA 恢复探测未通过而跳过。其余跳过项需要独立消息系统、Schema Registry 或 Windows SSIS Golden 资源。Windows Release 在此轮待验收。

## 复现

```bash
ctest --test-dir build/core -C Release --output-on-failure --no-tests=error -L sql-etl
ctest --test-dir build/product -C Release --output-on-failure --no-tests=error -E '^integration_ssis_mysql_50000$'
python tests/ssis_parity/workflow.py --runner "$PWD/build/product/tests/test_ssis_workflow" \
  --database "$PWD/build/product/tests/test_ssis_database" \
  --plugin "$PWD/build/product/plugins/ud_connector_mysql.so" --output ./ssis-results
```

千万行命令见 [C# 验收入口](../../../../tests/managed_sql/README.md)，逐项结果和实测数值见 [follow-up.json](follow-up.json)。PostgreSQL CDC 使用 `wal_level=logical` 和 `sslmode=verify-full`。MySQL 与 MariaDB 使用行格式完整 binlog，给隔离账户授予复制、快照锁和读取物理表身份所需的权限。Oracle LogMiner 使用启用归档和全列补充日志的实例，XA 测试账户获准访问 `DBMS_XA` 与 `DBA_PENDING_TRANSACTIONS`。 [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_LOGMNR.html) · [Oracle XA](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_XA.html)

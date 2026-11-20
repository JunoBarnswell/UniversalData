# UniversalData 7.0 Linux Release 验收

2026-11-20 使用 Linux x64 Release、ABI 15、Worker 协议 4 和 Catalog 修订 2。测试使用独立进程、TLS 和隔离数据库。运行时提交为 `d4fe2be`，对应源码指纹为 `a6cfb63f9d6bfb806fe3124edff68e775574f888163f30a1dc957532d184117a`。

## 原先受阻的十项

| 验收项 | 结果 |
| --- | --- |
| SQL Server XA | Pass，SQL Server 2022 Developer 与 ODBC Driver 18.7 通过分支提交、回滚和进程中断恢复 |
| SQL Server 与 PostgreSQL 混合 XA | Pass，两种提交顺序均通过重启后核对 |
| Microsoft SSIS 逐行基线 | Pass，DTExec 17.0.1000.7 运行 50,000 行，两个 Derived Column 共 100,000 项类型化观测一致 |
| ActiveMQ Classic、Artemis、RabbitMQ、RocketMQ | Pass，各自在真实 broker 完成回读、确认和重投递 |
| Kafka、Schema Registry 和 Kafka 进程接管 | Pass，覆盖缺失 Schema ID 和独立进程重启 |

Microsoft 基线使用 Windows Server 2025 上的实际 DTSX，并验证观测文件与原始捕获清单的 SHA-256。 [Windows DTExec 运行](https://github.com/JunoBarnswell/UniversalData/actions/runs/44689502092) · [逐行结果](ssis-golden.json)

## 千万行强类型 SQL 流式读写

C# 从源 Worker 读取 4,096 行的批次，转换文本和 Decimal，由目标 Worker 写入，再从数据库逐行核对顺序与值。两个用例均读取、写入和回读 10,000,000 行，校验和均为 `82da80c54b8880a5`。

| 数据库 | 首批 | 写入吞吐 | C# 峰值 RSS | 两个 Worker 各自峰值 RSS | C# 累计分配 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQLite | 108 ms | 150,547 行/秒 | 110 MiB | 78 / 86 MiB | 3.21 GB |
| PostgreSQL | 117 ms | 92,818 行/秒 | 106 MiB | 79 / 87 MiB | 3.22 GB |

同一跨进程实现启用缓冲池前约分配 5.15 GB，启用后减少约 38%。各进程分别采样峰值，累计分配覆盖写入阶段的全部托管线程。 [SQLite](sqlite-10000000.json) · [PostgreSQL](postgresql-10000000.json)

SQLite 每路 100,000 行、1,024 字节负载、四并发和三次试验。512 行批次的插入、更新和删除耗时中位数对应加速分别为 7.46、4.83 和 5.35 倍，批次间读取校验值一致。 [SQLite 四并发](sqlite-wide-4.json)

六个数据库分别通过 100,000 和 1,000,000 行的 Native 单并发读写，使用 512 行批次和 32 字节负载。每个规模的六库回读校验值一致。报告保留各阶段耗时、首批延迟、P95、CPU、峰值内存及批量接口调用数。 [六库读写结果](native-sql-scale.json)

## 产品验证范围

- Linux CORE 启用 HTTP 时通过 86 项常规 CTest，关闭 HTTP 时通过 84 项。PRODUCT 88 项常规 CTest 全部通过。两个独立 mTLS Worker 节点执行普通作业，核验节点容量、派发、参数传输和 HTTP 结果入库。Chromium 核验 Studio 的 SQL 源、强类型 C# 转换、SQL 目标和 Worker 中断恢复。
- SQLite、PostgreSQL、SQL Server、MySQL、MariaDB 和 Oracle 通过强类型 SQL 功能验证。五个有日志源的数据库均通过 CDC 验证，包含 Oracle 26ai Free `23.26.4.0.0` 的 TCPS、归档日志和 LogMiner。基于进程中断的单驱动部分提交恢复及 SQL Server 与 PostgreSQL 混合恢复已在实库通过。
- MySQL 8.4.13 的 24 个 CSV/XLSX 工作流通过逐行回读，覆盖每批一行、异常行、写入冲突、部分提交及进程中断恢复。每个输入 50,000 行，单行批次用时 1,127.60 秒，已提交与已确认行数均为 50,011（含汇总行）。 [MySQL 工作流结果](mysql-ssis.json)
- 独立 .NET 8.0.31 应用从实际 Linux 原生运行时包还原，加载实际发行 SDK 程序集，通过 SQLite 强类型读写与 HTTP 200 前置任务，逐行核验 35 行。随包的六库驱动均通过实际连通查询，ODBC 和 OCI 使用宿主安装的供应商客户端与 TLS 信任配置。跨平台 `PackageReference` 完整验收仍需要 Windows x64 运行时包和独立发行门禁。

| Linux 安装验证使用的包 | SHA-256 |
| --- | --- |
| `UniversalData.7.0.0.nupkg` | `780a9321f12a9b11afbe333730047c5db4a4077c1f0f82592a3e888b16d1d0ba` |
| `UniversalData.Runtime.linux-x64.7.0.0.nupkg` | `974f8b9ac362477ba943f9880375846e6b1310985b93adbb37d15f3381da7f27` |

## 复现入口

```text
ctest --test-dir build/core -C Release --output-on-failure
ctest --test-dir build/product -C Release --output-on-failure
python scripts/product_model.py check
python scripts/static_check.py
```

Windows 逐行采集使用 `tests/ssis_parity/windows/Capture-GoldenRows.ps1`。使用 `tests/ssis_parity/run_native_golden_rows.py` 处理相同输入，通过 `UD_SSIS_GOLDEN_MANIFEST` 和 `UD_SSIS_NATIVE_OBSERVATIONS` 运行 `ssis_microsoft_golden`。 [独立 .NET 8 安装入口](../../../../tests/installed_nuget.py)

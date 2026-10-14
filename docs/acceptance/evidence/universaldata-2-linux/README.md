# UniversalData 2.0 Linux 当前验收证据

当前范围为 SDK 2.0.0 / ABI、Package、Wire、Plugin、Checkpoint、Worker 协议族 10。完整 52 项状态与三轮设计复核只维护在 `metadata/product.json`，本文说明原始证据的读取方法，不另建任务账本。

- `configure.log`、`build.log`：Linux Release PRODUCT，六个数据库驱动、真实 Microsoft BCP / OCI Direct Path SDK、Arrow/Parquet、HTTP/XML/network、JNI、.NET 10 与设计器一起构建。
- `ctest-final-linux.log`：82 个已注册入口中 78 项实际通过，Oracle 的 SQL、CDC、原生装载及参与者恢复 4 项因环境 Blocked 跳过。PostgreSQL/MySQL/MariaDB/SQL Server 使用隔离数据库和真实客户端；不是模拟驱动。`ctest-components.log` 补充整批分发、复制、混合表达式错误与列文件的实际输出核对。
- `pipeline-performance.json`：百万事实行、十万参考键及十万错误行，Streaming、Sort Spill、Join、双错误目标、增量进程恢复及 Lookup，1/2/4 worker 共 18 个样本。记录吞吐、CPU、RSS、溢写、批次延迟及结果签名。
- 四个 `*-bulk-performance.jsonl`：同一隔离 TCP/TLS 数据库、相同百万行输入，比较参数模式与真实原生装载，逐批核对回执并校验业务行、整数和 Unicode/控制字符。PostgreSQL 2.944×、MySQL 1.768×、MariaDB 1.372×、SQL Server BCP 1.980×。这是本验收环境测量，不是通用吞吐承诺；Oracle 没有性能通过结论。
- `browser.json` 与截图：真实 Chromium 编辑、字段映射、拖动、撤销重做、保存重载、部署、真实 Worker 输出/日志/断点，以及终止 Worker 后以相同 Native execution ID 恢复。截图数据来自本次隔离验收包。
- `installed.json`、`installed-final.log`、`bundle-manifest.json`：同源 PRODUCT ZIP 的 267 个文件和 SHA256 均核对通过，旧 ABI 拒绝；公共 C/CMake、离线 NuGet/.NET、Maven/Java/JNI、CLI、三语言 Package/Data/Schema，以及安装后的 Web/Worker 真实浏览器恢复全部通过。`installed-browser/` 保存安装版截图和行为记录。
- `resources.json` 和 `ctest-oracle.log`：用户确认缺少的外部资源、后来建立的隔离驱动资源，以及 OCI 会话 ORA-12170 阻塞的实际原因。模拟 HTTP 的协议测试不替代 SMTP/FTP/SFTP/S3/Azure/SSAS/WMI 的独立服务验收。

可复现入口：`ctest --test-dir <PRODUCT-build> --output-on-failure`，真实驱动配置通过 `UD_TEST_<DRIVER>_CONFIG` 引用注入；`ud_remote_bulk_benchmark <plugin> <private-config> 1000000`；`benchmarks/next_phase.py --help`；`tests/web_product.py --help`；`tests/installed_sdk.py --help`。凭据与私有配置不随证据提交。独立 Java 消费者通过系统受信 CA 访问 Maven 仓库，不关闭证书验证。

Linux 通过不替代 Windows 验收。外部资源缺失、未完成的源码契约和未覆盖的恢复场景都保留为 Partial/Blocked；当前交付仍是草稿 PR，不能据此宣布 50 项全部完成。

`release-guard.log` 是完整产品门禁的预期拒绝：同源、文件完整性和证据哈希都一致，但 Windows、完整正确性/恢复、外部集成、双平台消费、剩余源码契约与完整发布尚未通过。没有通过改写状态或放宽门禁来发布产品。Linux 候选 ZIP 保存在本地 `artifacts/universaldata-2-linux/`，未发布、未合并。

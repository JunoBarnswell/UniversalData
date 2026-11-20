# Worker 集群验收

2026-11-20。Worker 集群验收覆盖实现提交 `d94149f` 和收敛后的 `4ecf6c5`。Catalog 修订 3，Worker 私有协议 5。Linux 使用 PRODUCT Release，启用 PostgreSQL 17 和 Kafka。Worker 接管和扩缩使用真实 mTLS、独立的 Management 与 Agent 进程、两个带 systemd/cgroup 的 Linux 容器以及同一测试宿主机上的实际 Broker 和 PostgreSQL。

| 项目 | 实测结果 |
| --- | --- |
| `management_worker_autoscale` | 两个不同 PID 同时复制同一份 500 万行输入，两个输出分别核对行数与 SHA-256，共 1,000 万行。预热 PID 被强杀后自动补位，暂停的任务经管道取消，重载和守护进程退出后验证配额回收。该项 CTest 用时 22.32 秒 |
| Windows Server 2025 CORE | `4ecf6c5` 的 84 项全部通过，`management_worker_autoscale` 用时 48.71 秒，包含 Job Object 进程回收和两个实际 Worker 的 1,000 万行校验 |
| Linux CORE | `4ecf6c5` 的 Native、JNI、.NET 主机 84 项全部通过。该次浏览器步骤在重载草稿后隐藏 SQL 源节点。统一画布尺寸来源后，本地 8.0 PRODUCT 复跑通过 SQL 编辑和执行、200/500/1,000 节点打开、断点调试及 Worker 强杀恢复 |
| CSV 独立吞吐测量 | 两个 Worker 共提交 1,000 万行，0 行拒绝，总耗时 9.10 秒。Worker 峰值 RSS 为 95.9–96.3 MiB，Management 峰值 RSS 为 161.6 MiB |
| `management_sentinel_ha` | 两次强杀 Management 后分别在 11.80 秒和 11.12 秒重新就绪，复用原执行编号和同一个 Worker PID，HTTP 目标只有一次实际写入。原会话继续有效，已撤销会话继续被拒绝 |
| PostgreSQL 主库和多数仲裁 | 同一宿主机运行三个 PostgreSQL 17、Patroni 和三个 etcd 成员，启用严格同步复制。强杀主库后 Management 在 32.21 秒恢复就绪，已确认的执行回执仍为一份。停止两个 etcd 成员后 10.51 秒停止接单，返回 HTTP 503 |
| 在线扩容期间强杀主 Management | 新分组写入 PostgreSQL 后强杀主进程，`4ecf6c5` 复跑的备机在 12.52 秒恢复就绪，接续原执行编号与已保存分组。`read_committed` 核对 600,000 条记录及四个源分区位点 |
| Kafka 千万条数据与稳态扩缩 | `4ecf6c5` 的 `management_kafka_elastic` 用时 1,362.50 秒，在两个 Agent 上观察到稳定的 `1→3→1`，最终计划修订为 5。`read_committed` 逐条核对 10,000,000 条输出，重复和缺失均为 0，四个源分区的提交位点均为 2,500,000 |
| `management_catalog_migration` | 同一快照重复导入和校验结果一致，历史日程、原会话和加密参数引用可读取，摘要篡改后拒绝核对 |
| Kafka 原生回归 | `integration_kafka` 与 `integration_kafka_process_takeover` 使用独立实际 Topic，后者在 Broker 提交后强杀进程，再从同一事务身份恢复 |
| CDC 与 XA | PostgreSQL TLS 快照与 WAL 恢复通过，SQLite 目标提交后强杀并续跑逐行核对 100 行。SQL Server CDC、独立 SQL Server XA、PostgreSQL XA 与 SQL Server/PostgreSQL 混合 XA 在实库通过，混合 XA 覆盖两种部分提交顺序 |
| Microsoft SSIS 逐行基线 | 使用 Windows 上 DTExec 生成的原始清单重跑当前 Native 计算，50,000 行、100,000 项类型化观测一致 |

Linux PRODUCT 注册 135 项，执行通过 92 项，43 项因该次运行缺少外部资源而跳过。Kafka、XA 和 SQL Server CDC 使用独立实际资源补跑。Windows 与 Linux 实机双向接管、单向断网和 Patroni 硬件 watchdog 的旧主隔离仍需用对应拓扑验证。

[部署和 CTest 配置](../../../architecture/worker-cluster-ha.md) · [Windows 和 Linux CORE 构建](https://github.com/JunoBarnswell/UniversalData/actions/runs/44755916388) · [Microsoft SSIS 捕获](https://github.com/JunoBarnswell/UniversalData/actions/runs/44689502092)

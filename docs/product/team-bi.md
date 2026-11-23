# 团队看板

分析员在 Studio 的分析工作区选择数据源、核对样本并保存字段用途、币种和指标，再发布数据集。看板使用一份已发布的数据集。作者可以组合 1 至 12 张指标、趋势、排行或表格卡片，预览草稿，发布后再指定可查看的同租户成员。成员从分析工作区或看板链接进入，按自然月和分类筛选，用同一次查询的结果下钻或导出。

## 登记数据库

管理员在宿主策略对应的租户下登记 `analysisSqlSources`。每项填写 `id`、`name`、`connection`、`table` 和 `allowSharing`。表或视图确定分析员和看板成员可见的行列范围。对有数据范围限制的报表，登记已经在数据库内限制范围的视图。每个已授权成员获得同一份已发布数据的读取范围。作者需要数据集读写权限、对应 `analysis.source` 授权以及宿主资源读取授权。浏览器只接收来源标识和显示名称。

| 数据库 | 宿主驱动 |
| --- | --- |
| SQLite | `ud:sqlite` |
| PostgreSQL | `ud:postgresql` |
| MySQL、MariaDB | `ud:mysql`、`ud:mariadb` |
| SQL Server | `ud:odbc` |
| Oracle | `ud:oracle_oci` |

SQLite 连接的 `resource` 填宿主授权的绝对路径。网络数据库沿用对应 Native 连接器的宿主资源、网络授权和 `credentialReference`，由宿主解析凭据。Worker 必须能加载对应连接器与客户端运行库。允许分享的来源需要显式指定 `allowSharing: true`。

例如，下面是某个租户的只读 SQLite 来源配置片段。宿主也须允许相应的文件根目录和插件目录。示例中的 `sales` 可以是数据库表，也可以是数据库管理员定义的只读视图。

```json
{
  "readOnlyRoots": ["D:/bi/sources"],
  "pluginRoots": ["D:/UniversalData/bi-win-x64/plugins"],
  "analysisSqlSources": [{
    "id": "sales",
    "name": "门店销售",
    "connection": { "driver": "ud:sqlite", "resource": "D:/bi/sources/sales.db" },
    "table": "sales",
    "allowSharing": true
  }]
}
```

Windows x64 的 BI 宿主需要启用 Parquet 的 Native 构建。CMake 的 `ud_product_hosts` 会将同一次构建的 `ud_data.dll` 和运行依赖复制到 CLI、Management 以及 Worker 各自的程序目录。直接执行 `dotnet build` 或 `dotnet publish` 时，默认使用 VS2026 预设中相同配置的 Native 输出。使用其他构建目录时，请先构建其 `ud_core_windows_runtime` 目标，再用 `-p:UniversalDataNativeRuntimeDir=<包含 ud_data.dll 及其依赖的绝对目录>` 指定它。数据库插件和客户端运行库位于宿主策略授权的插件目录。

Windows 交付目录为 `artifacts/bi-win-x64`。保留其中的 `management`、`worker` 和 `plugins` 目录结构，安装 x64 ASP.NET Core 10 运行时，并把交付包中 `plugins` 的绝对路径加入宿主和对应租户的 `pluginRoots`。在交付目录运行以下命令即可使用该策略启动管理服务和浏览器工作台。

```powershell
.\run-management.ps1 -Policy "C:\UniversalData\host-policy.json" -Urls "http://127.0.0.1:5080"
```

交付包由 `scripts/package-bi-win-x64.ps1` 组装。`-NativeRoot` 指向启用了 Parquet 及六个 SQL 连接器的 CMake 构建目录，`-ManagementOutput` 和 `-WorkerOutput` 指向使用同一套 Native 运行库发布的 .NET 目录。`checksums/SHA256SUMS.txt` 记录交付文件的 SHA-256，`VERIFICATION.md` 记录组包时执行的加载检查。

## 指标、期间和数据版本

基本指标可计算行数、字段非空数量、金额总和或字段精确去重数。派生指标按指标 ID 引用两个操作数，执行加、减或除法。保存字段草稿时校验引用和单位。金额使用 Decimal，空值与零分别回显，除数为零时比值为空。

选择一个完整自然月后，可将本期与上月或上年同月比较。两期使用同一已发布模型和相同的分类条件，分别计算指标。同比、环比的变化率为差额除以对比期值，对比期为零时变化率为空。

数据库刷新使用最近成功发布的模型。作者可手动刷新或开启周期刷新。刷新成功后新查询可读取新数据，已经取得的卡片查询上下文继续绑定原版本；失败回执保留原因，当前发布指针继续指向上一次成功的数据。对数据集重新建模后，看板作者需重新确认并发布看板才能采用新模型。

## 授权和验收

发布者为同租户成员授予查看权，并分别配置明细、导出。导出明细需要两种权限同时满足。撤销成员后再次使用已有明细或导出上下文将返回 HTTP 403。

接口与 SDK 对照见[API 文档](../api.md)。端到端验收入口为 `tests/analysis_team_dashboard.py`、`tests/analysis_team_browser.py` 和 `tests/analysis_sql_sources.py`。前两项覆盖真实管理进程、Native Worker 和浏览器，数据库来源测试可传入独立 PostgreSQL 测试库。

`benchmarks/analysis_dashboard.py` 接收已发布的六卡看板、作者以及各成员独立的浏览器会话文件、经源库核对的筛选期望值。它统计发起筛选至六卡完成渲染的延迟，核对每一张卡的指标，记录同时执行的刷新回执和耗时。默认验收目标为 3,000 万行、50 名成员、整页 P95 三秒、刷新三十分钟。运行条件和实测结果应随验收记录单独保存。

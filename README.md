# UniversalData

当前候选版本为 **SDK 8.0.0 / ABI 15**。C17 提供统一的 Package、Execution、Activation、Component 与 Connector。C# 通过 `DataClient` 管理命名数据源和独立 Worker 会话，在会话内使用官方 SqlSugar，并将批量数据处理编译为原生 ETL。作业支持多个日程，Catalog 在一个事务中登记触发、运行和时间游标。SDK 用法与升级见 [C# API](docs/api.md) 和 [升级说明](docs/breaking-changes.md)。

Studio 4 引入事件时间与 Tumbling/Sliding/Session 聚合、原生状态快照与 Spill、持续源空闲停放、优雅停止、Catalog 事实订阅，以及多标签工作台、命令面板和主题。契约及该版本的验收记录见 [Studio 4 PRD](docs/product/studio4-prd.md)、[流批统一设计](docs/architecture/stream-batch-unification.md) 和 [Studio 4 追踪表](docs/acceptance/studio4-traceability.md)。

现有产品模型继续追踪 52 项审查，并纳入本次 SSIS 的 30 项 Derived Column、23 个运算符、39 个函数、29 个类型域、31 类转换和 20 个业务验收条目。[逐项源码与缺口](docs/acceptance/ssis-strict-parity.md)由同一产品模型生成。Linux 的 Native、数据库、语言和浏览器结果只证明其中列明的行为；**没有真实 Windows SSIS Golden 的条目均不声明微软等价**。Windows 与独立供应商资源缺失单独记录受阻，代码缺口保持 Partial。

SSIS 公式文本和 JSON 表达式共用 Native typed IR、Registry 和求值器；Derived Column 的新增/替换共用读取原始输入的行级写入计划。29 个逻辑类型域保留位宽、精度、长度和代码页，新增 DateTime 保留 100ns 与显式偏移。C# / Java 通过 Native Registry 注册扩展函数与组件。[50K 真实 MySQL 验收](tests/ssis_parity/README.md)覆盖 CSV/XLSX、七目标事务、逐行回读、错误隔离和进程恢复。[远程批量装载](docs/architecture/sql-bulk-loading.md)使用 PostgreSQL COPY、MySQL/MariaDB LOCAL INFILE 和 SQL Server BCP；OCI Direct Path 的真实验收仍受环境限制。

```mermaid
flowchart LR
Definition[Package Definition] --> Runtime[唯一 Package Execution]
Runtime --> Control[Tasks / Containers / Precedence]
Control --> Flow[DataFlow Task / Pipeline]
Flow --> Registry[组件 / 端口 / 路径 / sealed Batch]
Registry --> Connections[Connection Managers / Physical Drivers]
Registry --> Compute[共享 Expression / Compute]
```

当前代码提供：不可变 Package、原生绑定与标量作用域、Sequence/For/Foreach、表达式优先约束、失败控制流、后台执行/取消/指标、多端口 Pipeline、有界队列、共享不可变 Batch、CSV/NDJSON/SQL 源、Derived Column、Conditional Split、Multicast、Union All、Cast、Sort/Join/Aggregate、文件及 SQL 目标、Connection Manager 和统一组件描述注册。组件与恢复的实际完成/验证状态见 [实施状态](docs/acceptance/ssis-status.json)，不能把源代码目录或旧 SDK 验收记录当作新架构验收。

SDK 仅保留 `ud_package` / `ud_execution` 执行链路。旧 CSV Job、DAG、Run 及其 .NET/Java 类已删除，没有兼容桥接。嵌入式查询和 ETL 复用同一 Compute 算子；未来 BI planner 保留待实现状态；C# P/Invoke 和 Java JNI 只投影 native ABI。

## 构建

### Visual Studio 2026

安装 VS2026 的“使用 C++ 的桌面开发”（含 MSVC x64、Windows SDK、“适用于 Windows 的 C++ CMake 工具”）和“.NET 桌面开发”组件，并安装 .NET 8 SDK/目标包。VS2026 生成器需要 CMake 4.2+；构建入口优先使用当前 VS 实例自带的 CMake，没有内置 CMake 时使用 PATH 中的 CMake。

双击根目录的 `UniversalData.sln`，选择 `Debug | x64` 或 `Release | x64`，执行“生成解决方案”（Ctrl+Shift+B）。解决方案包含 `UniversalData.Native`、.NET SDK、`Consumer` 和 `DataConsumer`；原生项目先构建，再编译托管项目，并将对应配置的 `ud_data.dll` 复制到托管输出目录。

原生项目通过 Makefile 工程调用现有 CMake，支持源码浏览、IntelliSense、生成、重新生成与清理。CORE 明确构建原生 SDK 子集；产品宿主通过 `UD_BUILD_HOSTS=ON` 构建，要求 .NET SDK 10。PRODUCT 还要求全部驱动、Web 和语言依赖。JNI、SQLite/PostgreSQL、HTTP、测试和 benchmark 使用其他预设或显式配置启用。

原生产物位于 `build/vs2026/<Debug|Release>/ud_data.dll`，CLI/Worker 位于 `build/vs2026/hosts/product/<cli|worker>/`；托管 SDK 和示例位于各项目的 `bin/x64/<Debug|Release>/net8.0/`。原生构建完整日志保存在 `%TEMP%/UniversalData-vs2026/`，失败时输出真实退出码及错误摘要。默认原生启动项目为 CLI，调试前在项目属性中填写执行参数。

在 VS2026 Developer PowerShell 中也可编译完整解决方案：

```powershell
MSBuild.exe UniversalData.sln /restore /m /p:Configuration=Debug /p:Platform=x64 /nologo /verbosity:minimal
MSBuild.exe UniversalData.sln /restore /m /p:Configuration=Release /p:Platform=x64 /nologo /verbosity:minimal
# 独立编译原生项目（自动查找 VS2026），也支持 -Action Rebuild 或 Clean。
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/build-vs2026.ps1 -Action Build -Configuration Debug
```

`build/vs2026` 为专用构建目录。切换 VS 安装实例导致缓存冲突时，确认路径后仅删除该目录，再生成解决方案。`Clean` 清理当前配置的产物，`Rebuild` 清理并重新编译当前配置。

### 其他构建方式

通用预设需要 CMake 3.25+、C17 编译器；Windows 预设需要 CMake 4.2+ 和 VS2026，Ninja 预设需要 Ninja。现有 `windows`/`linux` 预设启用 JNI，需要 JDK 17；.NET 绑定需要 .NET 8 SDK，Java 包需要 Maven。

```sh
cmake --preset windows
cmake --build --preset windows --parallel 4
ctest --preset windows
# 2.0 要求 Windows 与 Linux 均验收
cmake --preset linux
cmake --build --preset linux --parallel 4
ctest --preset linux
# 可选 SQLite/PostgreSQL 必须显式提供依赖，默认不下载。
# HTTP Task 可选：cmake --preset linux -DUD_WITH_HTTP=ON（需 libcurl >=7.85，TLS/异步 DNS）。
dotnet build examples/dotnet/Consumer.csproj
mvn -f bindings/java/pom.xml package
```

最小消费者只需 Foundation/Columnar；Package 静态组件自动选择 Expression/Compute/Runtime/Plugins。Windows 需支持 C17 的 MSVC；2.0 的 Linux 与 Windows 均须编译和验收。

原生 [HTTP Task](docs/architecture/http-task.md) 使用同一逻辑连接、宿主网络授权和凭据 resolver，强制 TLS 校验、禁用重定向并限制重试。HTTP Data Flow 源/目标仍为独立待办。

CLI：`ud_cli execute host-policy.json package.json '{}'`。SDK 未提供的新执行 ID 由 Native 生成；CLI 将原状态目录保存在 Native Catalog，恢复时使用 `ud_cli execute host-policy.json package.json '{}' ORIGINAL_ID resume`。CLI、Worker 与管理服务共用宿主资源范围、凭据引用及固定 C# 程序集加载入口。产品宿主使用 .NET 10，默认管理地址为 `127.0.0.1:5080`。Java JAR 使用同一发布物内的 JNI 和原生库；数据库连接器由宿主显式加载。

管理服务要求登录和显式租户策略。首次部署使用 `ud_cli identity-init IDENTITY_FILE SUBJECT TENANT` 初始化宿主身份目录；参考 [宿主身份与隔离契约](docs/product/hosting-security.md) 和 [策略示例](products/host-policy.example.json)。作业和执行记录只保存受保护参数引用；不可信扩展必须由可用的操作系统沙箱运行。

## Linux 产物交付

完成 Release/JNI 构建后，打包脚本直接安装已构建的二进制，生成 native NuGet、JAR/POM、SHA256 清单及 ZIP：

```sh
python packaging/package-linux.py --build build/four-engines-linux --output artifacts/2.0.0/linux-x64
python tests/installed_sdk.py --bundle artifacts/2.0.0/UniversalData-2.0.0-linux-x64.zip --work /tmp/ud-sdk-consumer-fresh --evidence artifacts/linux-consumers.json
python benchmarks/four_engines.py --cli build/four-engines-linux/hosts/ud_cli --output artifacts/linux-benchmark.json
```

输出目录和消费目录必须不存在。PostgreSQL 模块为显式可选项，要求 libpq 17+，使用可取消非阻塞命令和游标，每次复用前清理会话状态。窗口、浮点 SUM、恢复与事务组合的实际边界见权威产品账本。

## Windows 产物交付

Windows 构建自动将 `ud_data.dll` 的依赖放在原生库和各宿主旁。启用 SQL 连接器时，构建同样装配连接器的运行库。若供应商将运行库放在独立的目录，配置时使用 `-DUD_WINDOWS_RUNTIME_DIRS="<运行库目录>;<其他运行库目录>"`。在启用 JNI、hosts 和所需连接器的构建目录配置完成后运行以下命令。

```powershell
./packaging/package.ps1 -BuildDirectory build/windows -NativeRuntimeLicenses 'D:/SDK/native-licenses'
ctest --test-dir build/windows -C Release --parallel 4 --output-on-failure
$consumerWork = Join-Path $env:TEMP ('ud-sdk-consumer-' + [guid]::NewGuid().ToString('N'))
python tests/installed_sdk.py --bundle artifacts/UniversalData-8.0.0-win-x64.zip --companion-packages <Linux运行时包目录> --work $consumerWork --evidence artifacts/consumers.json
```

脚本重新构建 Release，再生成 `artifacts/8.0.0/win-x64/sdk`、带原生依赖的 NuGet、Java JAR/POM、逐文件 SHA-256 清单和 ZIP。独立消费测试仅使用解压后的产物，C# 进程无需将 SDK 加入 `PATH`。`-NativeRuntimeLicenses` 随包收录本次所用供应商运行库的许可证。

将 Windows 和 Linux 的运行时包、同版本 `UniversalData` 和打包时解析的 NuGet 依赖放入同一个本地源。安装项目引用 `UniversalData`，通过 `dotnet restore --source <本地源> --use-lock-file` 在本地解析依赖并记录锁文件。构建与发布会复制当前平台的自包含 Worker。使用 `python tests/installed_nuget.py --source <本地源> --complete --evidence <验证报告>` 在独立 .NET 8 项目中验收六库与 HTTP。两个系统的安装报告记录依赖锁和包哈希。发行工作流核对同一提交的报告后生成审核产物。 [NuGet 发行工作流](.github/workflows/publish-nuget.yml)

Windows 原生运行需要 x64 Microsoft Visual C++ Redistributable。C# SDK 面向 .NET 8 或兼容运行时，产品宿主需要 ASP.NET Core Runtime 10，Java 需要 JDK 17。SQL Server 由安装程序注册与构建时 SDK 一致的 x64 Microsoft ODBC Driver。部署 Oracle 和 MySQL 连接器时，分别将 Oracle Instant Client Basic 和 MySQL C 客户端的匹配运行库放在连接器 DLL 旁。每个 Windows 进程按应用或连接器所处目录加载依赖。

当前权威 [Phase 0–7 产品和技术设计](docs/architecture/phase-0-7-design.md)、[核心与扩展清单](docs/product/component-catalog.md)。

设计与边界：[产品](docs/product/product-model.md)、[架构](docs/architecture/target-architecture.md)、[SSIS 公开文档映射](docs/architecture/ssis-reference-matrix.md)、[破坏性变更](docs/breaking-changes.md)、[公共 ABI](docs/architecture/public-c-abi.md)、[路线图](docs/roadmap.md)。不声称 `.dtsx`/COM/SSISDB/SQL Agent 兼容；不提供默认 Web 管理平台或跨任意目标的全局 exactly-once。

四引擎本轮设计与追踪：[产品模型](docs/product/etl-four-engine-product-model.md)、[目标架构](docs/architecture/etl-four-engine-target.md)、[运行决策](docs/architecture/etl-runtime-decisions.md)、[删除矩阵](docs/architecture/etl-destructive-migration.md)、[78 项工作项与 69 目标同源映射](docs/acceptance/etl-four-engine-traceability.md)。`ud_cli inspect host-policy.json package.json` 读取原生绑定计划；`ud_execution_diagnostics` / `.NET EtlRun.DiagnosticsAsync()` / `Java Execution.diagnostics()` 返回统一运行事实。

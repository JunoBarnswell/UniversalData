# 兼容性与发行

| 边界 | 当前版本 | 规则 |
|---|---|---|
| Native package / SDK | 0.2.0 开发中 | 尚未生成或验收发行包 |
| Facade / Plugin ABI | 2 / 2 | struct_size、ABI 版本及必需能力必须匹配 |
| Checkpoint / Worker / Batch wire | 2 / 2 / 2 | 不认识的版本失败，不猜测迁移 |
| Windows x64 | MSVC C17 | DLL/CRT 内存由创建方释放 |
| Linux x64 | GCC C17 | libm、dl；ETL 的后台 Run 依赖线程；POSIX 文件语义 |
| .NET | net8.0 | CsvJob、Batch、Dag、Run + 原生库配对；新增包装未编译验收 |
| Java | Java 17 | JNI CsvJob/Batch/Dag/Run 与 ud_data 配对；新增包装未编译验收 |
| SQLite | 3.35+，CI 3.49.1 | 只支持已声明类型和事务批次 |
| PostgreSQL | 构建要求 libpq 14+ | 新连接器未编译/运行；PostgreSQL 17 容器验收待执行 |
| DuckDB | 开发锁定 1.4.4 | 公开 C API；独立可选依赖 |
| ODBC | ODBC 3 / Unicode API | 事务与数据类型需运行时及真实驱动验证 |

不同 SDK 版本不可随意替换 native 文件。Facade、插件和 JNI 必须在同一受支持平台/架构编译。默认不下载依赖；显式构建选项需要提供头文件/库，详见 packaging/dependencies.lock.json。

Native：CMake install/export + CPack ZIP/TGZ，附 LICENSE/NOTICE/Arrow Apache-2.0 许可。

NuGet：`dotnet pack`，NativeArtifacts 指向包含 win-x64/ud_data.dll 与 linux-x64/libud_data.so 的目录。缺少双平台原生文件时不能把纯托管包装当完整 SDK 发行。

Maven：`mvn package` 生成包装 JAR，原生库独立按平台交付；ud_java 与 ud_data 放在宿主加载路径。当前未实现 JAR 内原生库自动解压/装载。

CI 工作流配置了原生编译/行为测试、最小安装消费、三语言相同结果、可选依赖和包产物；不配置公开发布凭据，不自动发布 NuGet/Maven。推送源码不等于跨平台/驱动验收通过。

最近实际 CI 因 GitHub 账户支付/额度问题未启动任何步骤，不计为验证通过。0.2.0 尚未触发新运行。

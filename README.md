# UniversalData

组件优先的 C17 通用数据处理 SDK，MIT 许可。ETL、Query、插件与语言绑定可组合；Web UI、产品调度与业务权限属于外部消费者。

当前实现：独立 CMake 组件、配额分配器、Arrow 列式交换、严格 CSV、原生结构化计划、过滤/投影/聚合、目录增量与崩溃恢复、连接/排序/分页、外存归并排序、动态插件、可选 SQLite/ODBC/DuckDB/Parquet、.NET 8、Java 17、CLI 与独立 Worker。

聚合目前使用受配额约束的内存状态，**不提供外存聚合或分布式流处理**。Worker 是进程故障隔离，不是操作系统权限沙箱。尚未验证的能力与环境见 [交付记录](docs/validation.md)。

## 构建与测试

依赖 CMake 3.24+、C17 编译器。Presets 需要 CMake 3.25+；JNI 需要 JDK 17，语言包需要 .NET 8 SDK / Maven。

```powershell
# Windows，默认构建不下载第三方依赖。
cmake --preset windows
cmake --build --preset windows
ctest --preset windows
dotnet build bindings/dotnet/UniversalData/UniversalData.csproj --nologo --verbosity:minimal
mvn -q -f bindings/java/pom.xml package
```

```sh
cmake --preset linux
cmake --build --preset linux
ctest --preset linux
# 最小消费者只需要 Foundation / Columnar。
cmake --preset core-only
cmake --build --preset core-only
cmake --install build/core --prefix /absolute/sdk
cmake -S examples/c -B build/consumer -DCMAKE_PREFIX_PATH=/absolute/sdk
cmake --build build/consumer
```

外部插件只链接 `UniversalData::Contracts`。DuckDB、SQLite、ODBC 默认关闭；显式启用而依赖缺失会配置失败，不静默降级。支持版本及依赖来源见 [兼容性](docs/compatibility.md)。

## CSV 恢复链路

生产方先写临时文件，再原子改名为 `.csv`。输入目录与状态目录使用不同的绝对路径，状态目录的父目录须已存在。

```powershell
build/windows/hosts/Debug/ud_cli.exe once D:/data/input D:/data/state examples/etl/schema.ud examples/etl/plan.ud
```

`watch` 持续发现新文件；SDK 可从其他线程取消。读取结果必须通过 `ud_snapshot_latest` / SDK 的快照方法选择已提交代次，不能直接选择最新 CSV 文件。

独立消费示例： [C](examples/c)、[.NET](examples/dotnet)、[Java](examples/java)、[插件](examples/plugins)。

文档： [架构](docs/architecture.md)、[公共 API](docs/api.md)、[检查点与 Worker 协议](docs/protocols.md)、[兼容性与发行](docs/compatibility.md)。

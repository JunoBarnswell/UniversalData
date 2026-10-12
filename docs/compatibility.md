# 兼容性与安装边界

| 边界 | 当前版本与约定 |
|---|---|
| SDK | 1.0.0 开发版，破坏性重构 |
| ABI / Package / Schema / Plan / Wire / Plugin / Worker / Checkpoint | 只接受 4，完整布局，拒绝旧前缀与旧对象 |
| Windows | x64 MSVC C17；本轮实际构建、native/ASan/事务恢复/安装消费 |
| Linux | 相同核心算子 + POSIX 文件/线程/进程/动态加载；本轮按用户决定免测 |
| C# | net8.0，薄 PInvoke；NuGet 包配合同 ABI native DLL/SO |
| Java | Java 17，薄 JNI；Maven/JAR 配合同 ABI ud_java 和 ud_data |
| SQLite | 显式可选依赖，本轮 3.49.1 官方 pinned amalgamation |
| PostgreSQL | 当前连接契约源码；本轮无 libpq/数据库验收 |
| HTTP Task | libcurl 可选依赖；本轮未启用，关闭时 Unsupported |
| ODBC/DuckDB/Parquet/其他扩展 | 旧 generic 原型删除；扩展状态见组件清单 |

Native install/export 提供组件静态库和同源码 shared facade。第三方 native 内存必须按创建方契约释放；SDK context 限额跟踪 SDK 自有 allocations，不等同于进程 RSS 或第三方依赖的全部内部 allocations。Registry、Connection、Expression 和 Activation 生命周期见公共 ABI 文档。

本轮验证属于核心交付，不自动合并 PR、发布包或承诺 Linux 当前二进制已经验收。未测平台和可选系统有独立状态，历史证据不代替本轮证据。

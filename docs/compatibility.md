# 兼容性与安装边界

| 边界 | 当前契约 |
|---|---|
| SDK / ABI | metadata/product.json 生成 SDK 1.1.0、唯一 ABI/协议 5；拒绝 ABI 4 及更早的定义、插件和检查点 |
| Windows | x64 MSVC C17；本轮集中构建及 native/ASan/事务恢复/独立安装消费 |
| Linux | 相同核心算子；POSIX 文件、线程、进程及动态加载实现；本轮按用户决定免测 |
| C# | net8.0 薄 PInvoke；版本及 ABI 从产品模型生成 |
| Java | Java 17 薄 JNI；JAR/Maven 与 ud_java/ud_data 同版本 |
| SQLite | 显式可选依赖；官方 pinned 3.49.1；事务及 receipt 纳入本轮验收 |
| PostgreSQL | 当前连接契约及源码；本轮不作数据库验收 |
| HTTP Task | 可选 libcurl；关闭返回 Unsupported；本轮不启用 |
| 其他扩展 | Planned/Partial 独立记录于统一组件清单和目标模型 |

Native install 提供组件静态库及同源码 shared facade。SDK context 计量自有 allocation capacity（含 header）；第三方数据库和平台库内部资源有各自所有权，不宣称 SDK quota 等于整个进程 RSS。

所有权、并发、Checkpoint 与 Breaking Changes 见 architecture/infrastructure-convergence.md 和 breaking-changes.md。编译、行为验收、产物、远程 CI 与合并分别记录；历史 ABI 4 证据不能代替本轮证据。

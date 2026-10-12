# 有界架构与实施清单

仓库基线来自远程初始提交 `09bebba`，原本仅有 README 与 .gitignore。这里是新 SDK 的实现，不存在需要迁移的旧产品代码或旧运行时。

## 依赖与发行边界

Foundation → Columnar → Compute；Runtime 依赖 Columnar；ETL/Query 依赖 Compute/Runtime。Contracts 只有公开头文件，Plugins 加载器依赖 Contracts/Columnar。箭头表示向上提供能力，实际链接由消费者指向提供者。

每个组件有 CMake Target、公开依赖、安装导出及对应行为测试。`UD_COMPONENTS` 选择组件，自动加入必要依赖。`UD_BUILD_HOSTS`、`UD_BUILD_JNI` 会要求完整 Facade；最小发行必须关闭这些选项及插件构建。

`ud_data` 从同一份源码构建单个共享实例。独立静态组件与 Facade 句柄不能混用；同一进程加载多个 Facade 副本也不能互换对象。每个上下文所有公开对象都由同一分配域持有。

## 对象与失败语义

- context 必须比 batch、plan、job、plugin、Arrow 导出对象活得更久；有存活分配时销毁返回 BUSY。
- plan 在 accumulator 生命周期内保持存活。job 内部拥有独立 plan、聚合状态、队列与目录锁。
- batch_get/field 返回借用视图。Arrow 导出复制数据并提供独立释放回调，可先销毁源 batch；上下文仍须存活。
- 队列 push 成功才转移所有权；pop 转移给调用方，销毁队列释放剩余 batch。首期为同步 pull 执行，容量 1 实施背压，不宣称已经实现并行调度。
- 所有上下文操作由宿主串行调用，唯一例外是 `ud_cancel` / `ud_is_cancelled`。错误消息属于 context，在下一次失败时替换。
- 聚合更新、job poll 失败后当前对象不可继续；关闭并从已提交状态重新创建。部分状态绝不作为成功结果暴露。

## 权限、事务与扩展

原生插件采用版本化函数表，不访问私有结构。宿主表负责分配、数据访问与释放；存活实例或执行回调期间禁止卸载。插件需要的依赖由安装者提供，核心不会自动安装驱动。

SQLite 一次 WRITE 的所有输入行在一个事务中执行，约束失败整批回滚。ODBC 运行时检查事务支持；失败的 COMMIT/ROLLBACK 被标记为结果未知，必须关闭连接并人工确认。Parquet 是 DuckDB 可选适配能力，写文件不具备 CSV 检查点的跨故障提交保证。

Worker 用显式序列化协议隔离进程崩溃；不可信代码仍能访问 Worker 用户的系统资源。权限沙箱、进程资源限额及 Worker 池未实现。

## 阶段边界

P0/P1：工程契约与可恢复 CSV 聚合链路；P2：插件与可选 SQL；P3：内连接、排序、分页、外存排序及连接器；P4：独立 Worker 与原生/语言包配置。

未实现的后续能力：外存聚合/Join、优化器代价模型、并行执行、状态代次自动回收、事件时间/窗口、数据库 CDC、分布式检查点及权限沙箱。这些能力没有占位生产入口，不计入完成度。

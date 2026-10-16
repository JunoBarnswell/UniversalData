# UniversalData Studio 4.0 产品契约

基线：main `09118e27f6cab45524c8bbeab581c1fa805a0529`。上传的升级计划作为需求资料，当前源码与本文件的责任决策作为实现依据。已有 Designer 3 的草稿 CAS、发布版本、只读预览、原生表达式、连接发现、字段映射和隔离测试保留。

## 用户旅程

1. 从项目打开 Package。工作台标签按 Package/修订身份去重；每个标签保有自己的唯一编辑历史、草稿与画布。切换导航不销毁编辑；关闭未确认更改须明确选择。
2. Develop 使用 Control/Data 同一个 Package 图；Stream 是 Source 运行契约和同一 Data Flow 的视图，不产生第二份配置。组件目录、表单、端口和能力由 Native descriptor/binder 提供。
3. Validate/Explain 将图提交同一 Native Binder。持续上游接无限 Sort/关系全量聚合必须拒绝。窗口显式给出时间字段、窗口/滑动/会话间隔、聚合计划、迟到政策、状态容量；UI 不计算窗口结果。
4. Debug 保留安全 Preview、输入输出 Schema/错误/行对照及断点。Preview 有界、只读；真实运行样本是获授权的 Native 调试采样，标记来源及采样限制，不承诺完整数据。
5. Observe 使用 Catalog 有序事件与同一 Worker 原生进度投影。显示 Native 状态、组件/端口、队列、Watermark、Barrier 及最后持久点；断线显示重连/缺口，安全刷新快照。累计行数与实时速率分开。
6. Release 延用 immutable Package→Deployment→正式执行的现有治理流程。预览、隔离测试、发布和正式执行按钮不能互相代替。

## 工作台与交互

导航包含项目、开发、执行观测、日志、作业。开发工作台采用资源/组件目录、中央图、Inspector、底部数据/运行抽屉。命令面板提供搜索与键盘执行；每个动作调用现有编辑事务和 API。支持主题与密度，焦点可见，图状态有文字，菜单可 Escape 退出并归还焦点。编辑、调试、Live 图共用稳定节点/Port ID；Live 事实只能绑定实际执行的 Package 定义，修改草稿后显示过期。

窗口输出采用最终结果：窗口结束加 allowedLateness 到达 Watermark 后只发布一次；后续迟到事件按 fail/drop/独立 late 端口处理。不提供没有目标撤回协议的“更新已发布窗口”。时间统一为 Native 有符号 UTC 微秒；区间为 [start,end)。Session 按 Canonical key 合并半开区间；不允许 t < Watermark − allowedLateness 的事件重新打开已发布 Session，按相同迟到政策处理。Decimal/整数复用精确聚合，浮点复用原生确定顺序策略。

## 能力及发布事实

BOUNDED 自然 END；CONTINUOUS 空闲不 END；SNAPSHOT_THEN_STREAM 仅在真实 CDC 驱动声明支持时使用。MICRO_BATCH 不是定时重启 Package。不能由前端开启未声明能力。无限 Join、刷新维表、限速及分布式 exactly-once 等未完成扩展必须在权威模型中标明 Planned/Unsupported；禁止用按钮假装实现。任何多 Sink 部分提交仍以 CommitUnknown/RecoveryRequired 处理。

SSIS/Hop 提供工作台/调试职责参考，ADF/StreamSets 提供数据校验与观测参考，NiFi 提供背压可视化参考，Flink 提供事件时间契约参考；不引入其运行时或配置语言。

验收以 metadata/product.json 中 studio4 工作项及 UI01–UI10/ST01–ST16 为唯一状态来源。代码实现、当前构建、真实外部资源验证、Windows/SSIS 对照和生产可发布性分别记录。

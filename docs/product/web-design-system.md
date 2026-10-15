# UniversalData Designer 工作台

设计器保留 React Flow 和唯一的 Native 执行链路。浏览器维护草稿、布局、选择和撤销历史；Native 决定类型、端口、表达式、行处理、错误、提交与恢复。SDK 2.2.0 新增工作台 API，ABI/Package/Wire/Checkpoint 保持 11；所有版本投影来自 `metadata/product.json`。

## 操作状态

项目首页可以重新打开已保存版本及当前用户的工作草稿。Package 与组件使用稳定 UUID；用户修改的是版本化 `layout.label` 和节点标签，名称变化不改变执行身份。

| 操作 | 输入与结果 | 外部写入 |
|---|---|---|
| 自动保存草稿 | 当前文档 → 用户私有 Catalog CAS 记录 | 仅产品状态 |
| 保存版本 | 当前文档 + 预期版本 → 不可变 Package 版本 | 仅产品状态 |
| 校验 | Native 结构、配置、字段、端口和执行计划 | 不执行任务或目标 |
| 预览 | Native 选中节点及祖先的有界数据流 | 不执行目标、控制任务或源确认 |
| 完整测试 | 当前文档 + 宿主批准的 SQL 测试连接 + 每个目标表 | 仅明确的隔离目标及授权临时文件 |
| 发布版本 | 当前文档经校验/CAS 保存后固定部署版本 | 不启动执行 |
| 正式执行 | 已发布版本、资源、表、写入方式与显式确认 | 由同一 Native Execution 执行 |

发布按钮始终取得工作台当前文档。草稿、保存版本和发布版本分别显示；版本冲突返回 409，停止自动覆盖。布局变化不会使数据样本过期，执行语义变化会。旧样本明确标为过期；公式前后对照和分支命中不会把旧计算结果当成当前结果。

## 数据连接与观察

CSV/XLSX 向导支持上传和授权路径，使用 Native 编码/分隔符探测、工作表发现、Schema 推断及真实样本。用户确认格式、标题和类型；有歧义时必须明确选择。类型编辑后必须重新读取样本。可明确选择全部按文本读取，再用 Native 转换隔离异常值。发现得到的可空性不能证明全文件约束；写入必填目标前需要明确源契约或相应转换。

上传限制为每文件 64 MiB、每用户 512 MiB/512 文件。文件经过临时写入与原子发布，位于授权根目录内的 `.ud-imports/<tenant>/<subject>`；其他用户和租户不能读取，宿主私有状态及凭据文件不会进入包资源范围。

连接中心展示授权目录中实际可加载的 Native 模块及能力。连接保存真实模块路径、资源身份和凭据引用，不保存密码。数据库、表和字段发现经同一 Worker 调用 Connection Manager。映射表支持同名匹配、未映射提示及 Native 类型/精度检查；业务键使用 Native 投影索引。

Derived Column 使用 CodeMirror，提供字段/函数补全、括号匹配、Native UTF-8 诊断定位及输入/输出行对照。全部公式仍读取原始输入。Conditional Split 按首个 TRUE 分流，名称与稳定分支身份分开保存，拖动优先级不会改变连线的业务分支。Multicast 与 Aggregate 的复制/归并语义在画布中分别说明。Join、Aggregate 和 Lookup 使用业务表单。

画布支持拖入组件、在单输入转换连线上插入、模板、复制/删除、自动布局、撤销重做及快捷键。目录点击新增节点会避开已有节点；动态端口重新绑定时同步刷新画布命中区域。组件栏、属性栏和底部数据区可调整尺寸；窄屏采用抽屉。派生列与条件分流均可打开完整工作台，不需要在窄属性栏内横向寻找规则。节点展示命名端口、字段数、真实端口计数和故障；底部提供输入、输出、异常行、Schema、逐行对照及 Native 计划。

## Native 契约

- `ud_execution_preview` 使用普通 Pipeline 的调度、类型、Buffer、运算及错误端口。预览不创建第二套执行器。每源上限 10,000 行、每输出最多 32 条样本、最长 30 秒，保留样本受内存和字节上限约束。
- 预览只允许明确声明 `UD_COMPONENT_PREVIEW_SAFE` 的组件，并在绑定后检查外部写入效果。SQL 表扫描额外要求驱动声明并实施 `UD_CONNECT_READONLY_SCAN`；PostgreSQL/MySQL/MariaDB 使用只读事务，SQLite 拒绝可写语句。自定义 SQL、脚本/扩展回调、CDC 和控制任务不会进入预览。
- `ud_execution_inspect_file`、`ud_execution_metadata`、`ud_execution_connector_catalog` 提供文件、数据库及真实驱动元数据；C#、Java/JNI 使用同一公开接口。
- `ud_execution_reidentify_package` 依据 Native 已绑定计划固定端口引用，再更换包身份。隔离测试使用此接口，克隆目标连接并重绑事务参与者，不改变源连接，也不在产品层复制端口哈希算法。作用域存在不一致物理端口分配时明确拒绝。
- `ud_execution_progress` 返回受生命周期锁保护的原子计数；终态指标与有界调试样本保存在现有 Catalog 中。每个组件保留实际失败阶段/状态。
- 端口 ID 表示逻辑包图，依据包、数据流、组件、方向和逻辑分支生成，不受部署项目或租户绑定影响；字段和数据行继续使用自己的物理隔离范围。旧版本生成的端口哈希引用需要重新生成，不能依赖隐式回退。
- 调试支持运行身份断点及 `definitionBreakpoints` 任务定义路径。浏览器声明任务选择，Native 匹配作用域。暂停、继续及单批次仍受原生安全边界控制，活动事务内不暂停。

## 服务与权限

新增 `/api/preview`、`/api/files/inspect`、`/api/files/upload`、`/api/connectors`、`/api/metadata`、`/api/schema/compare`、`/api/drafts`、`/api/versions`、`/api/publications`、`/api/tests` 和执行 `/metrics`。它们共用现有身份、CSRF、审计及资源授权。新权限包括 `package.preview`、`package.discover`、`package.test`、`resource.upload`；保存、部署、调试和执行继续分别检查对应权限。测试版本也需要包写入/执行权限。

宿主及租户策略必须共同批准 `testResources`。完整测试只接受数据流任务和 SQL 目标，逐目标明确测试表；外部命令、模块、文件目标等不会被隐式改造成测试操作。源和目标共享同一连接时需先分成独立连接。事务参与者只允许隔离目标。未知提交继续显示 `RecoveryRequired`，不会被预览、测试或管理层自动重放。

Worker 在就绪前失败也返回结构化状态及生命周期阶段，并保存在现有执行记录中。管理层和界面不持久化原始异常文本或 stderr；启动失败不会伪装为已开始的 Native 执行。

## 验收与范围

现行证据及三轮设计复核归入 `metadata/product.json` 的 `productAlignment.designer3`，不增加并行任务账本。关键验收使用真实 Chromium、Management、Worker、Native 和隔离 MySQL：50,000 条 CSV/独立 OOXML 数据，49,950 条业务行、50 条错误行、四个分流签名、两张维度统计表及全部业务字段回读。隔离测试还检查正式数据库保持原结果。

样本不等同于所有生产行的持久查询账本。任务依赖建立的 full-cache、脚本和 CDC 尚不进入只读预览；系统明确拒绝这些预览。Windows 与独立 SSIS Golden 环境缺失仍记录为 Verification Blocked。历史截图和旧版本日志不作为当前版本的通过依据。

可复现的无数据库浏览器门禁为 `tests/web_host.py --help`；Linux 核心 CI 安装 `tests/requirements-browser.txt` 后执行该入口。它启动真实身份认证服务、Worker 和 Native，验证文件导入、公式、连线、版本发布、断点、杀进程和同 ID 恢复。`tests/designer_mysql_product.py --help` 是五万行 CSV/Excel 与七张 MySQL 目标的独立验收入口；需要明确配置宿主批准的正式/隔离测试资源、凭据引用和 Native 数据库验收工具，缺少这些资源不能记为通过。

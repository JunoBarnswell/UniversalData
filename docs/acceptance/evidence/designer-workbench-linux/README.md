# Designer 3.0 Linux acceptance

基线为 `main@bf5ab4e4195bf59eb06b016d094c38e397f9a214`，交付 SDK 2.2.0 / ABI、协议 11。源码指纹、安装包校验和与实际结果见 `validation.json`、`manifest.json`、`installed-sdk.json`；52 项历史审查继续由 `metadata/product.json` 管理，本次结果不代表完整 SSIS 整改完成。

Native 是唯一数据处理与提交恢复所有者。管理层授权并派发同一 Worker；React 只编辑和呈现 Native 元数据、样本、诊断及计数。三轮设计复核和代码映射保存在 `productAlignment.designer3`。

| 当前验收 | 实际结果 | 证据 |
|---|---|---|
| Linux PRODUCT | 六类数据库驱动、真实 BCP/OCI SDK、Arrow、HTTP/XML/network、JNI/.NET/Designer 构建完成 | build.log、version-build.log、manifest.json |
| 核心回归 | 75/75，通过 | native-core.log、native-core.xml |
| ASan/UBSan | Native 预览、Package、子包及 SSIS 行错误恢复共 4/4，通过 | sanitizer-preview.log、sanitizer-row-recovery.log |
| 真库 | MySQL SQL、类型边界、Prepare/Commit 杀进程恢复及 MariaDB SQL，4/4 | drivers.log、drivers.xml |
| 本机浏览器 | 真实文件导入、Native 公式与样本、画布连线/撤销/重做、发布当前 v2、显式执行、定义断点、单步、Worker Kill、同 ID 恢复和草稿重开 | browser-local.json |
| MySQL 浏览器 | 文件/工作表发现、连接测试、表/Schema 发现、可视化映射、Native 公式错误定位与分支排序、七目标正式执行、只读 SQL Source、Excel 隔离测试 | browser-mysql.json、row-accounting.json |
| 独立安装 | 330 文件校验；C/CMake、NuGet、Maven/JNI、CLI/Worker/Management/Designer 与安装后的真实浏览器恢复 | installed-sdk.json、installed-consumers.log |

两次五万行输入均为 49,950 条业务行及 50 条转换错误；区域聚合 4 行，产品聚合 7 行，共提交并确认 50,011 行。四个业务分支为 3,004 / 15,327 / 14,284 / 17,335 行；全部业务字段、来源 ID 签名、两张聚合表及原始错误内容经 Native MySQL 回读对比。CSV 与独立 OOXML 的输出签名均为 `51a233b26419fcc616f47d90eb8cdf49697c50f60638d36da6335099edf5ed6c`。Excel 完整测试明确指定独立测试数据库并重绑全部七目标；正式库保持原结果。

基本 CSV → MySQL 流程从空工作台通过向导和业务表单创建，无 JSON 编辑。复杂七目标图使用独立 Native Package 业务夹具初始化，再通过界面编辑、预览、发布和执行；不把它描述成从空白画布逐节点创建的验收。所有计算与行处理仍由真实 Native 完成。浏览器工具插件不可用，使用真实 Chromium + Playwright，未使用 Mock 服务或 JavaScript 数据计算。

验收中发现并整改的契约问题包括：部署项目改变逻辑端口身份；目录添加节点覆盖已有端口；动态端口未刷新命中区域；画布重绘丢失尺寸；视图保存丢失友好包名；Worker 就绪前失败丢失状态；CMake 复用旧产品版本。修复覆盖底层身份、生命周期和唯一版本源，不在产品层转换端口哈希或伪造就绪状态。

源码指纹封存后，仅增加证据和模型中的验收引用；发布包与独立消费者使用同一源码指纹。版本投影检查和静态边界检查均已执行。远程 CI 必须以当前 PR 实际状态为准，不能从本机通过推断 GitHub 或 Windows 已通过。

可复现入口：

- `ctest --test-dir <PRODUCT-build> --output-on-failure -LE 'real-integration|ssis-golden'`。
- 配置 `UD_TEST_MYSQL_CONFIG`、`UD_TEST_MARIADB_CONFIG` 后运行注册的对应驱动/恢复用例。
- `tests/web_host.py --help`：私有临时身份、真实服务和浏览器，Linux CI 已接入。
- `tests/designer_mysql_product.py --help`：批准的正式/隔离数据库、凭据引用、Native fixture helper 与五万行业务夹具；`--prepare-tables` 仅显式创建指定前缀验收表。
- `packaging/package-linux.py --help`、`tests/installed_sdk.py --help`：明确许可证运行库清单、全新安装消费目录和真实浏览器。

只读预览有源行数、样本数、字节与时间限制，仅允许明确的 Native 安全组件；脚本、扩展、CDC、自定义 SQL、控制任务依赖的 full-cache 暂不能预览。发现样本不能证明全文件可空性，Excel 验收显式声明 RowId 必填。上述检查不是受控 Native Bulk/参数 SQL 性能对比，不报告未经测量的加速倍数。

Windows、独立 SSIS Golden 及未提供的专项外部资源保持 Verification Blocked；继承的完整动态 Schema、SQL Server 跨库参与者和 Avro 等历史缺口仍按产品模型中的 Partial 状态处理。未把这些缺口扩大成本次全部开发阻塞，也未把 Linux 通过冒充完整生产发布。

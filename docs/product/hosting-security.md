# 宿主身份、参数与扩展执行契约

管理服务要求显式身份目录及租户资源策略。默认监听 `127.0.0.1:5080`；远程监听要求 HTTPS。部署者修改 [宿主配置示例](../../products/host-policy.example.json) 的绝对路径，在运行账户拥有的私有目录保存策略、身份和凭据文件，数据目录不得与包文件目录或插件目录重叠。

使用 `ud_cli identity-init /etc/universaldata/identities.json administrator team-a` 建立首个管理员。密码从标准输入读取，终端输入不回显。身份目录保存 PBKDF2-SHA256 哈希、随机安全戳、租户及角色名称。角色由权限和资源选择器组成：资源可为具体 ID、`prefix/*` 或 `*`。部署者直接维护这一宿主文件，服务在每次请求和派发时重新检查当前身份。修改密码会轮换安全戳，使已有会话及旧排队授权失效。

权限包括 `package.read/write/discover/deploy/execute/debug`、`deployment.read/write/manage/execute`、`execution.read/cancel/resume/debug/logs`、`job.read/write`、`script.compile/load`、`component.read`、`logs.retention`、`credential.use`、`resource.list` 和 `resource.open/read/write/receipt/execute/network/discover/delete`。租户策略先缩小宿主资源上限，再与用户权限求交。Catalog、凭据、身份文件和其他租户的状态目录不属于包资源范围。

Native Journal 使用单独的 `CheckpointOpen/Read/Write` 授权目的。普通文件连接不能借用这些权限访问当前执行的状态目录。产品宿主通过 Native Registry feature 位协商此契约，缺失时明确拒绝执行。

当前 Package 协议为 13，检查点格式修订为 4。头记录保存协议、修订和执行指纹，不匹配即拒绝恢复。Epoch 中每个组件保存产出和处理行计数、输入水位线与 idle 状态，用于恢复行身份和事件时间前沿。Kafka 归并 Epoch 可引用已提交的计算快照版本，并和输出一并提交归并游标。目标和行命令共用 Native Journal 的类型化写入意图与结果，区分已应用、已提交、已拒绝和提交未知。

Web 登录使用 HttpOnly、SameSite=Strict 会话 Cookie；修改操作还校验内存中的 CSRF 令牌。会话撤销持久化在租户 Native Catalog。审计仅记录主体、租户、路由模板、状态和时间，不保存请求正文、SQL、参数或编译源代码。

运行和作业参数作为不可变参数集加密保存在私有凭据目录，Catalog 只保存引用。每个 Worker 只解析本次授权的参数集。Native 检查点中的只读参数只保存身份，恢复时重新绑定并校验执行指纹。运行事件持久化使用结构化错误码；调试预览对已解析的参数字符串进行脱敏。业务自行复制参数到可变变量或数据列所产生的数据仍属于业务数据，此类输出应按其数据契约设置访问策略。

历史记录通过停机维护命令 `ud_cli protect-history POLICY_FILE CATALOG_DIRECTORY TENANT` 迁移。命令为 `run/`、`job/` 历史参数创建受保护引用，由 Native 重算文档完整性并清理 SQLite 空闲页和 WAL。遇到未知含参数的审计类型或被占用的 WAL 时失败，不能宣称迁移完成。历史记录的执行所有权仍须显式重新授权，旧排队状态不会自动恢复执行。

Management 不加载业务程序集。组件检查、Native 校验、Schema 发现、编译和执行统一由独立 Worker 入口完成。程序集必须符合 SDK/协议版本、固定 SHA-256 和授权路径；加载失败封锁当前 Registry，直到完整销毁。

宿主按程序集哈希显式声明可信扩展。未列入清单的扩展在 Linux 必须经 systemd 用户服务的 cgroup 限制及 bubblewrap 命名空间执行，只挂载本租户资源、本次 Journal/控制目录和本次凭据。网络命名空间默认隔离；该模式当前不提供外部网络访问。没有可用用户 cgroup/命名空间时拒绝运行。Windows 当前没有配置不可信代码沙箱时也拒绝运行。可信扩展属于运行账户的信任边界。验收中应分别记录沙箱实际运行、拒绝越权、超时终止与缺失平台能力，不能把拒绝执行记为完整沙箱验收成功。

Worker 在同步绑定本次控制端口后才发送带 `controlReady=true` 的 ready 帧；管理服务持久化原生执行 ID 后才发布 Running 并授权 start。开始执行前的 EOF 或拒绝仍会取消并关闭控制端口。没有控制就绪声明的旧 Worker 明确拒绝。控制通道连接失败统一为 I/O 故障，管理 API 返回不可用状态。

Worker 私有协议修订为 3，旧请求在加载包前拒绝。事件先写入 Worker 私有 Native Catalog，再以规范 Native 记录回传。恢复时先分页补齐管理端遗漏的事实，执行版本与事件序号从持久化记录继续。多进程派发使用 Native Catalog 条件写入和租约，未知提交与租约过期进入人工恢复。

在 Management 的 HTTPS 配置中，为每个远程 Worker 设置 `WorkerNodes:<nodeId>:CertificateSha256` 及 `Tenants`。在执行节点运行 `ud_worker agent CONFIG_FILE`，配置 `managementEndpoints`（每项设置 `url` 和 `certificateSha256`）、`nodeId`、`certificatePath`、`policyPath`、`maxCpu`、`maxProcesses`、`maxMemoryBytes`、`tenants`。节点证书由管理端信任的 CA 签发，PFX 密码可从 `certificatePasswordFile` 读取。注册容量及租户范围取管理端与执行端策略的交集，实际消息 Broker 和文件资源由执行节点授权并直接访问。

在执行节点的租户 `resources` 中为 MQ 配置逻辑连接和具体地址。连接的 `Open`、`Read`、`Write` 按实际操作授予，Broker 使用的网络端点持有 `Network`。例如，逻辑连接 `orders-broker` 下的 Kafka 源主题 `orders-broker/topic/orders-in` 需要 `Read`，消费组 `orders-broker/group/etl-orders` 需要 `Write`，目标主题需要 `Write`。同时授权 Broker 实际返回的端点，例如 `kafka://broker.example:9092`。MQ 凭据仍通过 `credentialFiles` 引用。

执行详情与事件流共用相同的对外运行模型。Management 负责租约、预算和成员状态。Kafka 事务通过 `POST /api/executions/{ticket}/resume?brokerRestore=true` 在符合授权的节点从 Broker 检查点恢复。阶段包按原父运行和逻辑分片恢复未完成的成员，并保留已成功的成员。旧租约必须先失效，恢复需核对原不可变包和 Kafka 位点。

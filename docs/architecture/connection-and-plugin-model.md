# Connection Manager、驱动与行命令

Package → Execution → Activation → Component → Connection Manager → Driver 是唯一执行链路。连接定义保存资源与凭据引用；宿主负责身份、资源授权和秘密解析，Native 管理租约、物理连接、事务及恢复事实。文件也经过同一 Manager。

Registry 持有组件描述和回调，Execution 租用其快照；有活动实例时不能卸载。插件、回调、Batch 和诊断字符串均遵循 Native 分配与释放契约。.NET、Java、CLI、Worker 和 Web 调用 Native Package，不各自实现数据处理或事务恢复。

每个物理连接同一时间执行一个操作。租约为当前操作绑定自己的授权、取消和截止时间检查；操作结束后恢复物理连接所有者。共享事务中的多条租约不覆盖彼此的长期回调。等待连接的操作可以取消；驱动仍负责实际 I/O 的取消。独立连接可以并行。取消可以并发，销毁不得与正在使用连接的调用并发。

## 行级 SQL

`ud_connection_execute_record` 接受单条事务性 DML 和最多一行密封参数 Batch。参数使用驱动绑定，不插值。SQL Command 允许一个输入字段绑定多个参数，参数列具有独立的逻辑身份与原字段血缘。成功输出保留原始 Row Identity；错误输出使用现有 Native Error 结构，原始输入仅在 `includeInput` 开启时保留。

| 状态 | 含义 |
|---|---|
| NotExecuted | 尚未执行业务 DML |
| Pending | Native Journal 已保存执行意图，尚无已知结果 |
| Applied | 本行已应用，仍属于调用方的未提交事务 |
| Committed | 独立行事务已收到提交成功响应 |
| Rejected | 本行未执行，或已获得回滚成功的证明 |
| Unknown | 无法确定提交或回滚结果，禁止自动重放 |

独立行命令使用 Serializable 事务。已有事务使用 Savepoint；物理连接锁覆盖建立 Savepoint、执行及释放/回滚的完整过程。驱动的 `savepoint` 原语执行 Native 编译的控制语句；普通用户 SQL 不能借此执行事务控制。驱动必须声明 `TRANSACTIONAL_DML` 并提供该原语，才能接受行命令。

只有已知 Rejected 且属于可恢复记录错误的结果可以 redirect/ignore。连接中断、超时、取消、未知提交与 Journal 故障仍导致真实执行失败。`fail` 停止处理；独立行事务中之前已提交的行保留。需要整体事务的 Package 必须显式使用具备真实参与能力的事务资源。

SQL 仅接受一个 DML 语句；事务控制、DDL、过程批次、可执行注释及无法确定转义方式的文本在副作用前拒绝。一般 SQL Task 继续承担管理语句与其他明确声明的外部效果。MySQL 无参数语句使用文本协议；类型化参数使用 prepared 协议，支持其不能 prepare 的 DDL，同时不对数据参数做字符串拼接。

## 驱动证明

PostgreSQL、SQLite、OCI 以及能力探测证明支持事务的 SQL Server/PostgreSQL/SQLite/Oracle/DB2 ODBC 资源可以声明事务性行 DML；通用 ODBC 不能凭驱动名称推断支持。

MySQL/MariaDB 先在本行事务内持有目标元数据锁，再证明 InnoDB、触发器可见权限及不存在触发器。分析使用 Native SQL 分析器，当前只接受可证明的单目标 INSERT/UPDATE/DELETE 子集，拒绝多目标写入、INSERT SELECT、自定义函数、触发器和无法证明的语法。仅通过角色间接获得的 TRIGGER 权限尚不接受。MyISAM 和缺少证明的资源在业务写入前拒绝。

普通事务和持久事务参与者是独立能力。SQL Server ODBC 当前具有普通事务及 BCP，尚未实现可靠的跨库 Prepare/Resolve/Recover；不能把它加入全局事务并模拟成功。PostgreSQL、MySQL/MariaDB、OCI 的持久参与者继续由 Native 协调器和同一 Journal 管理。

## 恢复事实

普通 SQL/REST 目标与行命令共用 Native Journal 的类型化意图和结果。恢复键绑定执行、任务、组件和行序号；摘要绑定语句及类型化参数，防止同键替换输入。

已提交结果恢复时不重复产生 SQL 副作用；已拒绝记录在 redirect/ignore 模式重建同一错误结果。人工恢复可以重试已证明回滚的失败命令。重试先持久化撤销已知拒绝的结果；原意图保留，因此此后若在新结果落盘前崩溃，仍进入 Unknown。Pending 没有可证明事务结果时返回 `RecoveryRequired`，禁止自动重试。

Applied 结果使用现有事务事实信封，仅在 Native 协调决策确认提交后发布。Epoch、错误目标及来源位置确认继续使用现有屏障；没有另建恢复系统。普通 REST 的响应确认、幂等和回执能力分别声明，不互相冒充。

检查点格式修订为 2，协议族保持 10；旧修订明确拒绝恢复，旧文件保留。插件公共结构增加行命令证明与事务控制原语，旧插件因结构大小不匹配而拒绝，必须重新构建。

## 验收范围

当前 Linux 验收使用真实 SQLite 混合行、重复参数、同连接并发 Savepoint、独立行提交、外部回滚、人工恢复、进程终止和未知提交禁止重放。PostgreSQL、MySQL、MariaDB、SQL Server 真实服务验证行命令、普通 SQL 与 Native Bulk。百万行装载逐字段回读并比较完整结果签名，写入时间与生成、回读时间分别记录。

Windows、OCI TCPS、跨库 SQL Server 参与者、完整连接中断/超时故障矩阵仍须分别记录代码缺口或验证受阻，不能根据上述 Linux 结果判定全部产品交付完成。

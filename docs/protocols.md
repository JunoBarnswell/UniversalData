# 检查点和 Worker 协议 v2（开发中，未运行验收）

## 文件代次提交

状态目录独占 writer.lock。代次命名 `generation-00000000000000000001.{state,csv,commit}`，commit 是唯一可见性入口。生产方提交后保持所有已接收 CSV 不可变；文件名、设备/文件 ID、SHA-256 均记录在输入账本。

提交顺序：验证已接收输入 → 编码完整状态 → 写 state 并刷新 → 写累计聚合 CSV 并刷新 → 写 commit.tmp 并刷新 → 再验证输入 → 原子发布 commit。Linux 用 link/unlink 实现不覆盖发布，Windows 用不覆盖的 MoveFileExW。未提交 state/csv 可被下次重算覆盖，已提交文件永不修改。

commit 是 80 字节：u64 版本 2、u64 代次、state SHA-256（32 字节）、csv SHA-256（32 字节）。整数均为 little endian。读取最高连续已提交代次，验证双校验；发现缺口、错误名、缺文件或校验失败即停止，不退回旧结果。

state：u64 版本、u64 代次、32 字节 schema/plan 指纹、输入目录字符串、NULL 标记字符串、u64 输入数、每个输入的文件名/设备 ID/文件 ID/内容摘要/字节偏移/逻辑记录/完成标记，最后是完整聚合 batch。

字符串使用 u64 字节长度 + UTF-8 字节。元数据字符串不含 NUL。Schema/plan 指纹对长度前缀及原始文本计算，因此即使仅改变空白也需要显式新作业状态目录。快照不包含进程指针、结构体 padding 或 allocator 状态。

崩溃前未提交部分允许重算；已提交进度与聚合状态一起恢复，最终快照不会重复累计。校验 SHA-256 用于完整性检查，不是签名或访问控制。要求本地、支持原子文件发布的文件系统；NFS/对象存储和断电耐久性未验收。已接收文件改写是生产契约违例；持续恶意改写不在一致性模型内。

## Worker

stdin/stdout 为二进制帧：u32 payload 长度（little endian，最大 64 MiB）+ payload。stdout 只有协议字节，诊断不得混入。进程死亡/EOF 是失败，不自动重试有副作用的操作。

请求头 u64 协议版本 2 + u64 操作：

- 1（Query）：schema 字符串、commands 字符串、batch。
- 2（Plugin）：绝对库路径字符串、配置字符串、u64 必需能力、u64 插件操作、argument 字符串、u64 是否有 batch（0/1）、可选 batch。

回复：u64 ud_status、错误字符串、u64 是否有 batch、可选 batch。每帧使用独立 context/plugin 生命周期；连续读取型插件的一次请求只取首批，持续源应由受控宿主持有实例，不能把此模式宣称为 Worker 流会话。

batch：u64 格式版本 2、u64 列数、u64 行数；每列递归记录 name 字符串、u64 类型、u64 nullable、u64 precision、u64 scale（有符号二补码）、timezone 字符串、u64 child_count 及各子字段。随后逐行逐列记录 u64 is_null；非 NULL Int64/Bool/Date32/Time64/Timestamp 为 u64 位模式、Float64 为 IEEE-754 64 位模式、UTF8/Binary 为 u64 长度 + 原始字节（允许 NUL）；Decimal128 记录 low/high 两个 u64；Struct/List/Map 记录 u64 元素数及递归值。Struct 的元素数必须等于字段数，List/Map 的元素遵守其唯一子字段。深度上限 32；分配受 context 配额约束，完整反序列化拒绝尾随字节。生产数据块不复制裸结构体或 padding。

Schema 文档必须以 `ud-schema 2` 加换行开头，文本计划必须以 `ud-plan 2` 加换行开头；缺少头或旧版本返回 Incompatible。C 的类型化构建接口显式传入 UD_PLAN_VERSION。此处仍是 CSV/Compute 模型，通用 DAG 及统一恢复执行器尚未实现。Checkpoint / Worker / Batch 的旧版本 1 明确拒绝，不迁移。

系统权限沙箱、网络限制、执行超时和资源控制由启动 Worker 的宿主负责；当前仅提供独立进程与有界序列化入口。

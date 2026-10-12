# 公共 API 与语言绑定

API 从 `include/ud/` 导出。固定宽度整数、长度明确的字符串值及不透明对象用于跨语言边界；文件路径是 NUL 结尾 UTF-8。返回 UD_END 表示枚举结束，其他非零状态是真实失败。详见 `status.h` 的状态码。

## Schema 与原生计划

`ud_plan_create` 接收两个带长度的 UTF-8 文本。它们是结构化计划命令，不是 SQL，不解析表达式或执行任意脚本。字段名与聚合别名使用 ASCII 标识符，禁止重复名称。Schema 每行：

```text
ud-schema 2
team utf8 required
amount int64 nullable
active bool required
```

支持 `bool/int64/float64/utf8` 与 `nullable/required`。计划每行一个命令：

```text
ud-plan 2
filter amount ge 0
project team amount
group team
sum amount total
count * rows
min amount minimum
max amount maximum
```

过滤操作支持 eq/ne/lt/le/gt/ge/isnull/notnull。前六种操作的行内剩余文本是按字段类型解析的字面值（可以包含空格）；`\N` 为 NULL。多个 filter 用三值 AND 连接，只有 true 行通过；`isnull/notnull` 不接收字面值。

project/group 后按空格列举字段。聚合计划中 projection 必须包含所有分组和聚合输入字段；输出为分组键及聚合别名。group 必须与聚合一起使用。COUNT(*) 计算行数，COUNT(field) 忽略 NULL，SUM/MIN/MAX 忽略 NULL，空集合为 NULL。无分组的空输入输出一行 COUNT=0、其他聚合=NULL。

MIN/MAX 通过类型化比较支持扩展类型；SUM 支持 Int64/Float64/Decimal128，Decimal 保留字段 precision/scale。整数、Decimal 精度及有限浮点范围溢出失败；不会转成字符串或零。字符串按 UTF-8 字节序比较，无隐式排序规则。扩展字段的构造与兼容性见下文。

## CSV

UTF-8（允许首部 BOM）、逗号、有表头、LF/CRLF、双引号字段、重复引号转义及字段内换行。表头必须与显式 Schema 名称/顺序匹配。不自动推断编码和类型，不跳过错误行。

默认 NULL 标记 `\N`，空字符串保留为空字符串；标记不能含分隔符、引号、换行或 NUL。标记比较作用于解析后的字段，包括被引号包围的字段。为避免丢失信息，输出字符串等于 NULL 标记时失败，要求宿主换标记。

错误给出文件、逻辑记录（表头为 1）、字段位置。标量浮点解析/格式化使用线程私有 C numeric locale；库不修改宿主全局 locale。

默认批大小 4096，检查点 10,000 行或 5 秒，在当前批末完整记录边界提交；作业 poll 结束也提交剩余状态。配额默认 256 MiB，包括工作批次、聚合状态和序列化快照，第三方数据库内部内存不计入此配额。

## 插件

`ud_plugin_get_api` 协商 ABI 2。描述包含 id/version/roles/capabilities；initialize 创建实例，execute 执行，destroy 释放实例，shutdown 清理模块级状态。空 shutdown 表示该插件没有模块级状态。

- CSV：配置为 UTF-8 文件路径；READ 输入是 Schema batch，连续 READ 返回批次或 END；WRITE 输入为完整输出 batch。
- ABS：无配置；SCALAR 接受一个 Int64/Float64 列，保留 NULL，Int64 最小值取绝对值失败。
- SQLite：配置为数据库路径或 `:memory:`；READ 首次 argument 为只读 SQL，后续使用空 argument 继续取批，input 为显式结果 Schema；WRITE 使用参数化 SQL，input 每行绑定一次。空 batch + 无参数语句执行一次，用于 DDL。
- ODBC：配置为连接字符串（不记录）；READ/WRITE 使用显式 Schema 与 Unicode API。结果字符串上限 8191 SQLWCHAR 单元，超长失败而不截断；数值列不接受隐式窄化。驱动兼容性需实际连接验证。
- DuckDB：配置为数据库路径，空配置为内存数据库。SQL 返回原生支持类型，其他类型报 UNSUPPORTED；Parquet READ/WRITE 的 argument 为路径。SQL 为调用方提供的完整语句，不拼接未转义路径或标识符。

## .NET 与 Java

当前高层绑定提供可恢复 CsvJob 与快照路径读取。Native C API 提供完整 batch/query/plugin 能力；尚未为这些接口提供高层托管对象，不能宣称语言绑定已经覆盖全部 C API。

.NET 用 SafeHandle 持有上下文和作业，Run 接受 CancellationToken；Java 用 AutoCloseable 和生命周期锁，cancel 可在另一线程调用。错误包含原生状态码和 UTF-8 诊断。Dispose/close 先取消再等待正在执行的调用结束。

Native 库必须可被装载器找到：Windows PATH，Linux LD_LIBRARY_PATH；Java 另需 `-Djava.library.path` 指向包含 ud_java 的目录。


## 0.2.0 新增接口（代码未运行验收）

Schema / 文本计划分别需要 `ud-schema 2\n` / `ud-plan 2\n` 头；旧文件返回 Incompatible。新增 schema 标量语法为 `decimal(precision,scale)`、`date32`、`time64`、`timestamp(UTC)` 或显式 IANA 元数据。Binary / Struct / List / Map 使用 C 字段树构建；CSV 不隐式编码二进制或嵌套值。

`ud_field.children` 为 Struct 的各字段，List 的唯一 item 字段，Map 的唯一非空 entries Struct（两个 key/value 字段，key 必须非空）。`ud_value.children` 为 Struct 值或 List/Map 的元素；追加深拷贝，读取返回借用的递归视图，下一次追加或销毁使其失效。深度上限 32，List/Map offsets 为 int32。无效追加不会增加部分子行。

Decimal 使用有符号二补码 128 位系数，precision 1..38、scale -38..38；解析不经过浮点，支持 Exact、截断、HalfEven、HalfAway、Floor、Ceiling。Exact 遇到有损缩放报 Parse，超精度报 Overflow；纯系数 compare/add 要求调用方使用相同 scale。标量解析输入上限 4096 字节。时间精度为微秒；Date32 保存 epoch 天数，Time64 限于一天，Timestamp 保存 UTC 微秒。ISO 解析使用公历 0001..9999，时间点必须含 Z / ±HH:MM；时区元数据用于保留标注，不解释无时区字符串。

`ud_batch_cast` 显式转换 / 重命名，嵌套类型保持形状；`ud_compute_map` 提供映射、常量、NULL 默认值及 ASCII Trim / 大小写。UTC 时间点不会通过隐式本地时区转换，Decimal-to-float 被拒绝，二进制与 UTF8 的显式转换仍须通过 UTF8 验证。

连接器使用 `ud_connection_open`，宿主必须提供凭据解析和授权回调。凭据解析按 tenant/resource/reference 返回通过 SDK context 分配的配置；SDK 用后释放且不打印配置。关闭资源在授权撤销或取消后仍可执行。Schema 发现返回声明类型及可见权限；游标采用明确 schema 和 batch_size。Append/Upsert/Delete 能力协商独立于读取；Overwrite / CDC 目前未声明支持。

PostgreSQL 的数据点只接受 timestamptz，读取会话固定 UTC / ISO / UTF8；无时区 timestamp 明确拒绝。SQLite Decimal 目标必须为 TEXT affinity，时间目标为 INTEGER affinity，以避免 SQLite 自动转 REAL 造成精度损失。新扩展类型写入只能通过类型化连接器，不能用旧 ODBC/DuckDB/SQLite SQL 写接口猜测映射。

类型化写入把数据和 `ud_sdk_receipts_v2` 批次凭据放在同一事务。tenant/job/batch_id 标识批次，摘要覆盖目标资源、表、模式、键、游标及版本化批次内容。同一 ID 的不同摘要报 Corrupt。CommitUnknown 必须查询凭据，不能推进源位置；凭据表不可读/缺失不推测未提交。没有跨连接全局事务。

质量规则状态跨成功 apply 调用；非空、唯一、范围、主外键及 accepted-row-count 对账已提供。正则规则在没有宿主 match 回调时使用 SDK 内置 UTF8 NFA。Unique 忽略包含 NULL 的键；PrimaryKey 拒绝 NULL；ForeignKey 使用 MATCH SIMPLE。质量错误或隔离失败使实例失效；失败不推进 processed 指标。隔离回调只有在坏行及违例信息持久提交后才能返回 OK。SDK 文件隔离存储已写入；真实持久隔离及恢复尚待统一验收。

C/.NET/Java 的全能力等价绑定、质量状态恢复、DAG 持久恢复和分布式 Worker 尚未完成，不能据这些接口宣称全阶段完成。
## Structured DAG contract (development)

`ud_dag_create` consumes a strict version 2 JSON object with `nodes`, `edges` and
`parameters`. Each node has `id`, `kind` (`source`, `transform`, `quality`, `sink`),
`operation` and an object `config`. Edges have `from`, `to`, `port`. Input ports of
each consumer must be unique and contiguous. Sources have no incoming edges;
sinks have no outgoing edges. All nodes must lie on source-to-sink paths, and
cycles, undeclared dependencies and duplicate IDs are errors. `ud_dag_build`
uses the same validator for C construction. Getters borrow immutable data.

Parameter declarations are named objects with `kind` (`null`, `boolean`, `number`,
`string`, `array`, `object`), optional Boolean `required`, and optional typed
`default`. A config object containing only `$parameter` and its string name is
a reference. `ud_dag_bind` validates values, resolves references structurally and
produces a literal graph. Required missing values, wrong kinds and unknown values
are rejected; an absent optional value referenced by config needs a default,
except a `null` parameter. The `$parameter` key is reserved. Fingerprints cover
the compact versioned document; insertion order and exact number lexemes are
significant. These APIs construct/validate plans. The Run API below executes
file graphs/custom nodes; durable DAG state and CSV recovery migration remain pending.

## DAG Run execution (development)

`ud_run_create` / `ud_run_create_json` own an independent context, allocator quota,
cancel flag, bound graph and node instances. `ud_run_execute` blocks;
`ud_run_start` uses a dedicated thread. One controller manages execution, waiting
and destruction; cancellation, metrics and node-state reads may be concurrent.
Runs never share a mutable context. Destruction while running returns BUSY.

The downstream-first pull scheduler uses bounded queues per edge and round-robin
input ports. Fan-out creates independent deep copies. Custom handlers declare
their output schema at open and process DATA / INPUT_END / POLL events. Inputs
are borrowed only during callbacks, output is owned by the runtime, and every
batch must match the schema and row cap. Callback effects are never automatically
retried. An event returning BUSY is a contract error (INVALID); TRANSIENT stops
the run. Handlers must free their instances and must not wait/control/destroy runs
inside callbacks. Authorization callbacks may inspect metrics or cancel.

Built-ins are Source `csv` / `ndjson`, Transform `pass` / `cast`, and Sink `ndjson`.
Sources require regular files and absolute paths. Source config contains versioned
`schema`, `path`, optional `batch_size`; NDJSON also accepts `max_record_bytes`
and `rounding`. CSV accepts `null_marker` and Exact rounding, and currently has
the host memory quota rather than a per-record byte cap (that option is rejected).
Cast config contains the target schema and optional rounding. Pass config is
empty or has an explicit rounding mode. Sink config has `path` and optional
`max_record_bytes`. File operations require a nonempty tenant and a host grant
for actions 101 (read) / 102 (write), rechecked for each operation and publication.

NDJSON Sink writes an exclusively reserved staging file, flushes/syncs, and
publishes a new path without replacing existing data. Existing output or leftover
staging returns BUSY. There is no global transaction across multiple sinks;
cancellation/error after one publication can leave that complete sink visible.
Database/quality built-ins, durable DAG checkpoints, restart reconciliation,
partition parallelism, finite retries and CSV migration remain unfinished.
Execution fixtures have been written but have not run.

`input_rows` counts source rows emitted once before fan-out; `output_rows` counts
rows accepted by Sink callbacks, summed across sinks. They are execution counters,
not checkpoint positions or proof of publication. Failed runs may retain queued
batches until destruction. `queue_high_water` records the total queued batch peak.

## Nested JSON and local quarantine (development)

`ud_batch_from_json` / `ud_batch_to_json` exchange arrays of named row objects
against explicit recursive fields. Unknown members and duplicate JSON names are
rejected; missing nullable fields become NULL. Structs are objects; Lists and
Maps are arrays. Decimal is parsed exactly from a number or string and emitted
as a string. INT64 accepts an integer number or string. ISO temporal strings
retain UTC semantics; Binary is strict canonical Base64. All output owns its
data. `ud_json_reader_*` reads NDJSON with explicit per-record byte and batch
limits. A malformed record stops the reader; it does not silently skip it.
Offsets are reusable only for the same immutable input. JSON root parsing is a
Foundation-only dependency with depth/node/host memory limits.

`ud_quarantine_*` provides a local SDK file store usable as the quality isolation
callback. Tenant/job paths are separated by hashes and every protected operation
requires the host's authorization callback. Set a stable batch ID and source
position before apply. Stored envelopes include version, tenant/job/batch/position,
plan fingerprint, typed rows, violation indices and checksum. File locks protect
writers, and file plus publication-directory sync precede acknowledgment.
Exact replay succeeds; changed rows/rules/position for an existing ID is CORRUPT.
Read outputs are owned and explicitly released. This is local storage, with host
control of the root directory; multi-machine coordination remains PostgreSQL.
Runtime durability and crash recovery tests have not yet been executed.

When no regex callback is supplied, quality rules use the SDK UTF8 Thompson NFA.
`ud_regex_compile/test/destroy` also expose it directly. Supported syntax and
limits are documented in `include/ud/regex.h`; unsupported constructs return
UNSUPPORTED. Matching honors cancellation and host memory quotas. Regular
expression fixtures are written but not executed.
## Typed external operators (development)

`ud_query_aggregate_ndjson` partitions typed input by composite group keys and
repartitions when group state/snapshot exceeds the context quota (maximum depth
8). `ud_query_join_ndjson` uses shared key hashes and bounded block nested loops
within buckets, including skewed duplicate keys. NULL keys never join. Exact
Decimal and recursive values use the Columnar parsers; output field names must
be unambiguous. Output order is unspecified. Input schema/plan is explicit and
input files must stay immutable throughout the call. The aggregate plan must
belong to the supplied context. `block_rows`, per-record byte cap, SDK allocation
quota and a cumulative logical spill-write byte limit are explicit. Filesystem
metadata/block overhead and libc file buffers are outside these logical quotas.

Temporary files are exclusively created under a caller-provided directory, and
only invocation-owned files are removed. The staging output is flushed and
published without replacing an existing output name. Cancellation, overflow,
parse and quota failures stop publication. This is a local SDK execution path;
it does not supply distributed leases or resumable DAG state. Its fixtures have
not been run and do not yet constitute spill correctness/performance evidence.

## Language data and DAG contracts (development)

.NET 8 and Java 17 expose owned `Batch` and immutable `Dag` wrappers. The batch
methods use the same versioned schema JSON, typed JSON, canonical wire, rounding
and conversion functions as C. Bind/cast results have independent owned lifetimes.
UTF16 input is validated before UTF8 encoding. Status codes retain C numeric
values in both languages. `Run` wrappers call the same C file DAG executor,
including start/wait/cancel, native metrics, node states, independent lifetime and
authorization callbacks. Exceptions from authorization become denial. Java
attaches the native worker to the JVM only for callbacks and releases global
references after joining; .NET retains callback delegates through native release.
The new data and execution fixtures are `tests/data_parity.py` and
`tests/run_parity.py`. Connector/quality/scheduler/recovery/distributed parity and
independent installed-package consumption remain unfinished; these fixtures alone
cannot satisfy the mandatory full capability gate.

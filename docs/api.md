# 公共 API 与语言绑定

API 从 `include/ud/` 导出。固定宽度整数、长度明确的字符串值及不透明对象用于跨语言边界；文件路径是 NUL 结尾 UTF-8。返回 UD_END 表示枚举结束，其他非零状态是真实失败。详见 `status.h` 的状态码。

## Schema 与原生计划

`ud_plan_create` 接收两个带长度的 UTF-8 文本。它们是结构化计划命令，不是 SQL，不解析表达式或执行任意脚本。字段名与聚合别名使用 ASCII 标识符，禁止重复名称。Schema 每行：

```text
team utf8 required
amount int64 nullable
active bool required
```

支持 `bool/int64/float64/utf8` 与 `nullable/required`。计划每行一个命令：

```text
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

MIN/MAX 支持全部四种类型，SUM 只支持 Int64/Float64。整数及有限浮点范围溢出失败；不会转成字符串或零。字符串按 UTF-8 字节序比较，无隐式排序规则。decimal、时间、复杂类型需要显式外部转换。

## CSV

UTF-8（允许首部 BOM）、逗号、有表头、LF/CRLF、双引号字段、重复引号转义及字段内换行。表头必须与显式 Schema 名称/顺序匹配。不自动推断编码和类型，不跳过错误行。

默认 NULL 标记 `\N`，空字符串保留为空字符串；标记不能含分隔符、引号、换行或 NUL。标记比较作用于解析后的字段，包括被引号包围的字段。为避免丢失信息，输出字符串等于 NULL 标记时失败，要求宿主换标记。

错误给出文件、逻辑记录（表头为 1）、字段位置。浮点输出要求进程数值 locale 为 C；库不修改宿主全局 locale，不支持时明确报错。

默认批大小 4096，检查点 10,000 行或 5 秒，在当前批末完整记录边界提交；作业 poll 结束也提交剩余状态。配额默认 256 MiB，包括工作批次、聚合状态和序列化快照，第三方数据库内部内存不计入此配额。

## 插件

`ud_plugin_get_api` 协商 ABI 1。描述包含 id/version/roles/capabilities；initialize 创建实例，execute 执行，destroy 释放实例，shutdown 清理模块级状态。空 shutdown 表示该插件没有模块级状态。

- CSV：配置为 UTF-8 文件路径；READ 输入是 Schema batch，连续 READ 返回批次或 END；WRITE 输入为完整输出 batch。
- ABS：无配置；SCALAR 接受一个 Int64/Float64 列，保留 NULL，Int64 最小值取绝对值失败。
- SQLite：配置为数据库路径或 `:memory:`；READ 首次 argument 为只读 SQL，后续使用空 argument 继续取批，input 为显式结果 Schema；WRITE 使用参数化 SQL，input 每行绑定一次。空 batch + 无参数语句执行一次，用于 DDL。
- ODBC：配置为连接字符串（不记录）；READ/WRITE 使用显式 Schema 与 Unicode API。结果字符串上限 8191 SQLWCHAR 单元，超长失败而不截断；数值列不接受隐式窄化。驱动兼容性需实际连接验证。
- DuckDB：配置为数据库路径，空配置为内存数据库。SQL 返回原生支持类型，其他类型报 UNSUPPORTED；Parquet READ/WRITE 的 argument 为路径。SQL 为调用方提供的完整语句，不拼接未转义路径或标识符。

## .NET 与 Java

当前高层绑定提供可恢复 CsvJob 与快照路径读取。Native C API 提供完整 batch/query/plugin 能力；尚未为这些接口提供高层托管对象，不能宣称语言绑定已经覆盖全部 C API。

.NET 用 SafeHandle 持有上下文和作业，Run 接受 CancellationToken；Java 用 AutoCloseable 和生命周期锁，cancel 可在另一线程调用。错误包含原生状态码和 UTF-8 诊断。Dispose/close 先取消再等待正在执行的调用结束。

Native 库必须可被装载器找到：Windows PATH，Linux LD_LIBRARY_PATH；Java 另需 `-Djava.library.path` 指向包含 ud_java 的目录。

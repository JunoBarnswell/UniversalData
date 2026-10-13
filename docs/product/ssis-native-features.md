# PR #5：五项 SSIS 职责的原生产品契约

SDK 1.4.0 / ABI 8。已有 A0–A5、R1–R5 基础保留；本轮包含 N1–N5 全部，Linux 验收，Windows 豁免。版本与门禁唯一来源为 metadata/product.json，ABI 7 证据属于历史。

| 职责 | Keep | 新实现 | 删除/改写 |
|---|---|---|---|
| Merge / Merge Join | Compute Sort、Buffer、Pipeline | merge、mergeJoin，明确 field ID/方向/NULL 顺序/stable 契约；每行检查单调性 | 原先仅有 ordering 枚举的物理计划改为完整契约；输入需求纳入唯一 Scheduler |
| Pivot / Unpivot | Data 类型、Lineage、Aggregate kernel | 枚举绑定列，setKeys 有序跨 Batch 分组，原生重复聚合；显式同类型展开 | 不引入动态列或托管实现 |
| Sampling | 原生 Row ID、Sort、Buffer | 全局 SHA-256 优先级精确 K；seed+Row ID 百分比，两个互补端口 | 不引入分区各取 K、first K 或第二随机算法 |
| XLSX | ConnectionManager Lease、文件 stage/receipt、Data、Buffer | driver excel；C17 ZIP/Expat SAX/zlib；共享字符串 Buffer 分页，流式写 ZIP | 格式只负责读写；文件发布仍由同一 builtin 生命周期负责 |
| SCD | readonly Cache asset、Lease、Write/Receipt、Journal | 原生属性分类，六端口；四变更端口绑定一个目标；typed changeset | SQL 由提供者按类型生成；禁止分开的 expire/insert 事务与 Pipeline 拼 SQL |

输入排序由 Sort 或 Source ordering 声明传递；消费者总会检查实际行顺序。同步保持 field ID 的组件继承排序。Merge 等键稳定地先取左流；Join NULL 不相等。Merge Join INNER/LEFT/FULL，只保存当前等键组；同名右列必须 rightAliases。FULL 的 NULL 扩展结果不声明单边键全序。

Pivot 的 values 为 [{value,name}] 固定列；setKeys、pivotKey、valueColumn 使用名称、field ID 或显式列序号，绑定后只执行已解析的字段。duplicate 为 reject（默认）、sum/count/min/max（现有 Aggregate）、first/last（必须稳定排序）。errorPolicy 为 fail/redirect/ignore；redirect 是常规 Error Output。Unpivot columns 为 [{column,key}]，retain 为保留列，nulls 为 skip/include，选定列必须同值类型；需要转换时先显式 Cast。

rowSampling 的 count 是整个输入的精确 K；whenShort 为 all（默认）或 fail；K=0 全部走补集。percentageSampling 的 percentage 为 0–100，数量服从概率分布。seed 为非负整数。Row ID 由 Source 提供，调度顺序不参与百分比决策。精确采样通过已有外部 Sort 选全局最小优先级，结果以优先级输出，不声明源序。

XLSX Source 显式 schema、sheet、矩形 range、header、datePolicy（workbook/1900/1904）、formulaPolicy（fail/redirect/null）；读取缓存结果，绝不计算公式。支持 SharedStrings、inlineStr、稀疏单元格、多工作表选择、1900/1904 日期。1900 serial 60 拒绝；时间转换到 UTC 微秒。String/Decimal/Binary 用无损文本表示，Binary 为 Base64，writer 的时间使用标准 OOXML ISO 日期单元格。NaN/Infinity、.xls、宏、外部引用、ZIP64、公式重算不支持。ZIP 条目数量、压缩/解压字节、XML 深度、单字符串、单元格均受限；禁止 DTD/外部实体/路径穿越。SharedStrings 和 writer stage 共用既有 Buffer 配额。Error Output 的 payload 显式 nullable，诊断提供物理行列位置。

SCD 的 cache 必须 Error+Fail 的唯一业务键、只含 Current 维度。mapping 指定 source/target/change（key/type1/type2/fixed）；current、validFrom、validTo、surrogate、version、effectiveTime 显式绑定。事实按业务键、effectiveTime 升序稳定排序。默认 surrogate 为稳定事件 ID（UTF8），也可 surrogateSource 显式提供；interval 为 UTC [from,to)，current 的 to 为 NULL。Type 1 仅更新当前行，Type 2 新行包含最新 Type 1 属性，历史不回填；Fixed 违例 fail/redirect/ignore。早于当前区间的变更拒绝。输入已包含完整属性；不支持隐式 inferred member 或数据库自增 surrogate。

scd 端口 0 Unchanged、1 New、2 Type1Update、3 Type2Expire、4 Type2Insert、5 FixedViolation。变更携带完整 _before、_eventId、_effectiveTime、_action；1–4 必须一一直接连接 scd.destination 的 0–3。同一 Event 的 expire/insert 在同一物理 Session 的现有 Receipt 事务中提交；完整前像不匹配或当前业务键已存在返回 Fenced。一个业务键只保留一个内存 overlay，变更跨 Batch 按业务键/时间/事件/动作重排到 Buffer 外部 Sort。重放查询 Receipt，禁止 CommitUnknown 时确认 Source。

新增 C、C#、Java 功能入口均为同一 Package/Execution API；语言侧没有算法实现。公共 C Operator 与 Component、WriteOptions 的结构变更只属于 ABI 8，没有 ABI 7 兼容桥。

第三方依赖：zlib（zlib License）仅做 Deflate/CRC；Expat（MIT）仅做有界 SAX。使用系统 zlib 1.3.1 / Expat 2.8.3；设置自定义 Context 分配器、关闭参数实体、拒绝 DTD 与外部实体。没有引入业务 ETL 框架。

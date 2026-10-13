# Lookup Cache 与唯一索引

缓存定义不可变，包含 ID、Reference Schema、复合键、duplicate=first/error/all、scope 与版本。Execution 拥有声明的资产；Cache Build 是正规 Pipeline 组件。成功 seal 后只读共享，每个查询实例持有独立 reader/view，最后引用释放所有 Block。

Compute 内有一个有序索引：Reference 经现有 Sort/Buffer 形成 bounded pages，页边界与页内二分都使用 Data Compare；不新增文件格式或私有 Spill。Join、Lookup 和 FK 共用这一索引，删除旧逐行扫描参考集的 join_pull。

默认 First 按 Reference 原始稳定输入顺序选择；All 保留确定的全部匹配；Error 在 seal 时拒绝重复键。NULL Lookup 不匹配；FK 使用 MATCH SIMPLE；复合值及 Decimal、Unicode、Nested 遵守统一类型约束。Miss / 重复违规走正规端口。

跨 Pipeline 只共享已封存、定义与 snapshot fingerprint 完全一致的缓存。跨运行不隐式复用；恢复先验证 Reference 版本。内存、working view、Spill 与取消由既有根资源 owner 管理。指标包括 load/hit/miss/duplicate/spill，不能用命中率代替正确性。

Cache 定义包含 `sourceConnection`，可选 `sourceTable`，schema、keys、duplicates 和 errorPolicy。Cache Build 使用显式 ordinal `mapping` 导入 Reference；Lookup 的 probe `keys` 也是显式 ordinal。Source 与资产共享来源字段命名空间；Column ID 可显式声明。

ABI 7 删除 `lookup` 与 `UD_OPERATOR_LOOKUP`；唯一产品入口为 `cacheBuild`/`cacheLookup`，默认 First 和 miss Ignore（inner），显式 Fail/Redirect 可覆盖。Error+Fail 封存时失败；Error+Redirect 隔离重复键的后续行并保留 First；All 按稳定 Reference 顺序展开全部匹配。Join 只使用相同索引的 All 模式。

缓存的版本哈希包含 Schema、复合键、重复/错误政策和完整有序内容，不依赖入库 Batch 切分。只读资产持有参考资源的逻辑 Read 授权，撤销立即使读取失败。恢复通过正常 builder DataFlow 重建，不增加缓存水合旁路。Execution 资产只有一个 builder；事务中的 Cache Build 明确 Unsupported，不能发布未确认事务中的参考数据。

资产授权来自 Builder 的真实 Source Lease，根所有者复制其已解析定义和资源，不在 Probe 时重新解析可变变量，也不额外占用物理会话。所有查询继续对这份真实资源检查 Read 授权。

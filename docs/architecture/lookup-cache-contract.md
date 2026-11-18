# Lookup Cache 与唯一索引

缓存定义不可变，包含 ID、Reference Schema、复合键、duplicate=first/error/all、scope 与版本。Execution 拥有声明的资产；Cache Build 是正规 Pipeline 组件。成功 seal 后只读共享，每个查询实例持有独立 reader/view，最后引用释放所有 Block。

Compute 内有一个有序索引：Reference 经现有 Sort/Buffer 形成 bounded pages，页边界与页内二分都使用 Data Compare；不新增文件格式或私有 Spill。Join、Lookup 和 FK 共用这一索引，删除旧逐行扫描参考集的 join_pull。

默认 First 按 Reference 原始稳定输入顺序选择；All 保留确定的全部匹配；Error 在 seal 时拒绝重复键。NULL Lookup 不匹配；FK 使用 MATCH SIMPLE；复合值及 Decimal、Unicode、Nested 遵守统一类型约束。Miss / 重复违规走正规端口。

跨 Pipeline 只共享已封存、定义与 snapshot fingerprint 完全一致的缓存。跨运行不隐式复用；恢复先验证 Reference 版本。内存、working view、Spill 与取消由既有根资源 owner 管理。指标包括 load/hit/miss/duplicate/spill，不能用命中率代替正确性。

Cache 定义包含 `sourceConnection`，可选 `sourceTable`，schema、keys、duplicates 和 errorPolicy。Cache Build 使用显式 ordinal `mapping` 导入 Reference；Lookup 的 probe `keys` 也是显式 ordinal。Source 与资产共享来源字段命名空间；Column ID 可显式声明。

Full 使用 `cacheBuild` 和 `cacheLookup`，在执行期共享封存的引用。重复键默认取首行，`Error+Fail` 在封存时失败，`Error+Redirect` 隔离后续重复行，`All` 按稳定顺序展开。Join 使用同一索引的 All 模式。

Partial 和 None 使用 `dbLookup`，引用列必须与数据库主键一致。SQL 的引用列比较决定匹配语义，查询返回的探针序号将结果映射到原始输入。Partial 按原始探针键缓存命中和未命中，在 `maxEntries` 和执行期 `ttlMs` 范围内复用。None 对每个探针执行独立的数据库读取，重复输入与 NULL 同样发出查询。

端口 0 输出匹配后的输入和引用字段。端口 1 根据 `errorPolicy=redirect` 输出记录错误。端口 2 在 `noMatch=output` 时输出原始未匹配输入。默认 `noMatch=ignore` 时，未匹配行由 `errorPolicy` 决定，默认丢弃。NULL 参与相同的未匹配分流。

`scope=epoch` 为每个数据库 Lookup 设置只读事务视图。检查点周期确认后释放旧视图，下一周期重新读取引用数据并建立新的 Partial 索引。SQLite 使用延迟事务，首个查询固定快照。PostgreSQL 使用只读的 REPEATABLE READ。其他驱动会拒绝这个作用域。同周期恢复重放已持久化的探针观察，恢复后首次遇到的探针使用当前只读事务视图。

Full 缓存的版本哈希包含 Schema、复合键、重复/错误政策和完整有序内容，与入库 Batch 切分无关。检查点恢复使用资产持久化的封存引用。只读资产持有参考资源的逻辑 Read 授权，撤销立即使读取失败。Execution 资产只有一个 builder，事务中的 Cache Build 会拒绝执行。

资产授权来自 Builder 的真实 Source Lease，根所有者复制其已解析定义和资源，不在 Probe 时重新解析可变变量，也不额外占用物理会话。所有查询继续对这份真实资源检查 Read 授权。

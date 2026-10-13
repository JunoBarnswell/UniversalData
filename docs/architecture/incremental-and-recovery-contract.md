# 增量位置与恢复契约

增量是已有 Source 的声明：snapshot / watermark / composite，含字段 ID、typed lower bound、升序规则、来源/Schema/政策指纹与分区位置。键必须可比较且非 NULL；复合键最后的唯一性通过源声明及读取快照实际检查。时间与 Decimal 使用 Data 的规范语义，不把更新时间轮询称为 CDC。

SQL 先在正常 Lease 的一致读取范围内捕获快照，使用 Compute Sort 规范排序，并把 bounded sealed Batch 的唯一 Wire 写入同一个 Journal 所有的有界不可变块；Journal 帧只记录 SHA-256 和长度。完整快照封存前不开放目标写入；中断捕获仅在重读前缀摘要一致时继续，变化安全拒绝。恢复从同一 Journal 重放，不保留数据库物理 snapshot token 的虚假长期承诺。

文件复用不可变文件摘要与全局位置；追加文件不是本轮 CDC。每分区有独立位置，分区定义变化 Incompatible。

lastRead 与 lastConfirmed 分开。泛化 DAG 采用整个 DataFlow 的确认 Barrier：所有普通/错误目标成功提交并持久确认后才推进确认游标；部分完成时确认位置保守保留，已提交 Batch 通过 Receipt 验证安全重放。这样可处理 Filter/Join/Aggregate 和多目标，不用下游接收或线程完成顺序猜测源进度。

Overlap 必须配合显式幂等目标策略，否则提前 Unsupported。Schema、快照、游标政策不一致拒绝恢复。Diagnostics 只显示位置数量、确认状态与指纹；不泄露水位原值。

SQL Source 的默认行为为有界快照捕获：同一个 Lease/Reader 完成一致读，关闭游标并释放物理会话，保留相同解析资源的逻辑授权。读取已封存数据仍检查 Read 授权。无 State Directory 时使用根 Buffer/Spill，有 State Directory 时使用已有 Journal 的 Wire 块和摘要帧；只有完全封存后才进入目标执行。

CSV/NDJSON 始终用不可变文件摘要及原始位置生成行身份，排序保留原始身份。SQL 的复合键身份由规范 typed key 生成；无键快照由来源及快照内容指纹与位置生成。身份不取自 Execution ID。分区在 Cursor filter 之前选择，确认各自选中行；全量快照先验证全局唯一性。相同 Task/Pipeline 中的同来源分区必须给出一致快照指纹，否则在目标开始前拒绝。当前文件分区是逻辑行选择，多个 Source 会重复扫描文件，不宣称字节级分片。

Receipt source position 包含最后稳定 row ID 和 Batch 行数。中途不推进 confirmed；恢复通过相同封存快照重建输出并核验已有 Receipt。大型或聚合 DAG 的确认粒度是完整 DataFlow，不能将 6,000 个已提交行错误显示成已确认的完整 Source Cursor。

Journal 的 fenced writer 统一发布 Wire 块和索引，不缓存快照正文于内存，不在日志压缩时重编码正文。块由摘要寻址，每次读取校验长度和 SHA-256；缺失/损坏拒绝恢复。已确认日志仍持有块，保留至宿主移除对应 State Directory，不与临时 Spill 混淆。Source 游标确认记录同时包含来源身份、快照/Schema 指纹和分区策略；公共诊断只显示计数与 CommitUnknown。

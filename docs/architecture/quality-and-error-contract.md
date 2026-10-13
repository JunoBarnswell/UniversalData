# Quality 与正规错误路径

Quality 的规则由 Schema/Column ID 绑定，含稳定 ID、语义版本、类型及 fail/redirect/ignore。所有规则按定义顺序求值；一个输入行只分类一次，可产生多个 Violation。Fail 保留原始质量错误；Ignore 显式记录失败但保留输入；Redirect 将输入移出 accepted，每条违规发一个错误记录。

FK 使用明确 Reference 输入；只在 Reference End 后查询已封存的 Compute 索引，遵守 MATCH SIMPLE。Reference 通过正常 Source/Lease/Buffer/Journal，恢复必须可验证；不隐式访问在线随机参考表。Unique/Primary Key 的状态受根配额限制，资源不足返回 Resource，不吞错。

Data 原生行身份独立于 Batch 行号、线程、输出顺序和 Execution ID。源按确定的逻辑来源与位置/键生成，投影/过滤/排序保留，Join 组合双方身份，聚合按确定的分组身份生成。Wire 和 Block Store 保留身份，真实所有权计入预算。

错误协议包含 execution/task/pipeline/component、rule ID/version、column ID、source row ID、error code、policy；默认不输出敏感原行，显式 payload 配置才复制原列。每次 emit 限于宿主 Batch 大小，违规较多用既有 MORE/POLL 分批 Drain。

分类 rejected 与错误记录数分开；持久隔离仅在正常目标 commit/receipt/journal 之后计数。普通/错误目标分别记录事实，任一未知提交保持 CommitUnknown。失败和恢复不创建旁路 Quarantine 执行器。

ABI 7 配置使用 `errorPolicy` 和可覆盖的 rule `policy`；不接受旧 Abort/Reject 拼写。`includeInput` 默认 false，开启后原行放在唯一 Struct 列 `input`，避免与固定错误列重名。accepted Schema 对 Fail/Redirect 的 NotNull/PrimaryKey 列提供非 NULL 保证，供后续 Schema Binder 使用。

规则诊断的 evaluated/failed/rejected/ignored 是当前实例的分类计数；committed/acknowledged/unknown 是同一规则定义的错误记录交付计数（目标有两个，则分别计入两次）。Ignore 不创建隔离记录，不计为错误记录提交。交付事实归 Root/Task 的既有 Checkpoint owner，错误行携带规则 token，不能只按 error port 流量反推拒绝行数。

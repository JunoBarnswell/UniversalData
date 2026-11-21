# Studio 4 实施与验收追踪

<!-- Generated from metadata/product.json studio4. -->

实现、验收与可发布性分别记录；Planned 不计为已完成。

## SDK 11.0.0 候选已审查源码

`8d7b36594d8c68a8933fabd57317dc6a7318925c`。 以下证据列指向实现或测试定义，不表示运行通过。

| ID | 目标 | 实现 | 验收 | 限制 | 证据 |
|---|---|---|---|---|---|
| S07 | Versioned stream/table refresh | Planned | NotRun | Current reference lookup is not event-time as-of table lookup or generally versioned dynamic refresh. | components/compute/src/cache_lookup.c, tests/orders_payments_shops_iceberg.py |
| S08 | Bounded stream/stream join | Implemented (scoped) | NotRun | Watermark-bounded inner interval match with inclusive bound; inputs are insert-only, rows at or behind the common watermark are dropped. No general outer/retracting/distributed join. | components/compute/src/event_match.c, tests/event_match.c |
| S09 | Key TTL deduplication | Implemented (scoped) | NotRun | Keeps the first row per key until event-time TTL expires at the common watermark. Not an unbounded/global deduplication guarantee. | components/compute/src/event_match.c, tests/event_match.c |
| S11 | Microbatch time/row triggers | Partial | NotRun | Native CDC row/time options group complete source transactions; general Package trigger/scheduling is not this capability. | engines/data-flow/cdc.c |
| S13 | Native rate budgets | Planned | NotRun | Bounded queue/working/spill quotas and node limits are not a general source rate governor. | docs/architecture/stream-batch-unification.md |
| S14 | Event-window change output | Implemented (scoped) | NotRun | Keyed tumbling/sliding count, sum and average may emit insert/update/delete (update/delete include the before image as applicable) changes with checkpointed published state; session remains final-only. No generic CDC retractions. | components/compute/src/event_window.c, tests/event_window.c |
| S15 | REST write and recovery modes | Implemented (scoped) | NotRun | response/idempotent/receipt modes use one Native path; unknown commit is not safe blind replay. Includes local fixture source, not external endpoint crash/replay evidence. | connectors/rest.c, tests/advanced_formats.py |
| S16 | Iceberg v2 REST/Parquet delivery | Implemented (scoped) | NotRun | Existing Catalog-owned partition specs are used; native-created tables are unpartitioned. No remote object-store credential/maintenance guarantee or general standalone SQL DELETE. Real Trino fixture not certified by CORE. | connectors/iceberg_table.cpp, tests/iceberg_v2.py |
| S17 | Bounded cross-system streaming oracle | Implemented fixture | NotRun | PostgreSQL TLS CDC insert, Kafka refund/duplicate, current MySQL lookup, SQL/Trino net 70 and same-ID resume with one snapshot; not production-wide mutable CDC or distributed exactly-once. | tests/orders_payments_shops_iceberg.py |
| V11-01 | 跨次调度增量结算与空批续跑 | Native Catalog 用同一事务结算运行及位点，原运行使用 Journal 恢复 | NotRun | — | catalog/incremental.c, engines/package/incremental.c, tests/jobs_schedule.py |
| V11-02 | S3 Iceberg 及实时三源回读 | Native SigV4 对象传输，REST Catalog 快照确认 | NotRun | — | connectors/s3_object.c, tests/iceberg_v2.py, tests/orders_payments_shops_iceberg.py |
| V11-03 | DataJob、CLI、Designer 共用预检和发布 | Native Binder 与 POST /api/publications 统一契约 | NotRun | — | engines/data-flow/pipeline_contract.c, bindings/dotnet/UniversalData/DataClient.Execution.cs, products/designer/src/JobTemplateDialog.tsx |

## 历史 Studio 4 基础验收

历史基线 `09118e27f6cab45524c8bbeab581c1fa805a0529`。 下表保持当时的实现与结果，仅作追溯，不覆盖上方当前行。

| ID | 目标 | 实现 | 验收 | 限制 | 证据 |
|---|---|---|---|---|---|
| UI01 | Workspace tabs and history | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI02 | 200/500/1000 node browser performance | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI03 | Native properties, formula and field mapping | Implemented | Partial | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI04 | Draft CAS, close protection and tabs | Implemented | Partial | Draft/version CAS API and tabs verified; full UI conflict-resolution journey NotRun. | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI05 | Bounded Native first-N probes | Partial | Partial | Only existing explicit-debug first-N probes; rolling hot sampling remains unimplemented. | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI06 | Commands, themes, density and keyboard | Implemented | Partial | Themes/density/commands tested; full keyboard and accessibility audit NotRun. | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI07 | Read-only preview and immutable publication | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI08 | Native Catalog feed and reconnect cursor | Implemented | Partial | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI09 | Native binding errors and unsupported continuous state | Implemented | Partial | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| UI10 | Existing tenant and sample authorization | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/browser.json, docs/acceptance/evidence/studio4-linux/studio4-scale.json |
| ST01 | Bounded END | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST02 | Continuous idle executor defer | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST03 | Partition idle and minimum Watermark | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST04 | Tumbling keyed final aggregation | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST05 | Sliding and merging sessions with lateness | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST06 | FIFO drain and aligned Barrier | Implemented | Partial | Two-input frontier/idle/END verified; simultaneous two-source Barrier fault injection NotRun. | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST07 | Shared bounded queue backpressure | Implemented | Partial | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST08 | Existing projected schema evolution rejection | Implemented | Partial | Existing projected-schema rules only; no current live vendor schema-drift UAT. | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST09 | Native Journal recovery with compact window state | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST10 | CommitUnknown and receipt recovery | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST11 | Truthful multi-sink commit policy | Implemented | Partial | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST12 | Event-time expiry, quota and Buffer Store spill | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST13 | Graceful Stop distinct from Cancel/Pause | Implemented | Passed | — | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST14 | Bounded subscriptions and slow write deadlines | Implemented | Partial | Quota/write-deadline implementation and authenticated cursor replay verified; sustained slow-client UAT NotRun. | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| ST15 | One million events and one hour soak with resource metrics | Partial | Partial | 1h continuous soak NotRun; finite regression is not production capacity proof. | docs/acceptance/evidence/studio4-linux/window-performance.json |
| ST16 | C/.NET/Java single epoch and Package | Implemented | Partial | Current C/.NET/Java ABI/lifecycle passed; identical event-window package across all three hosts not separately certified. | docs/acceptance/evidence/studio4-linux/ctest.xml, docs/acceptance/evidence/studio4-linux/asan-ubsan.log |
| S07 | Versioned stream/table refresh | Planned | NotRun | Not implemented in this foundation candidate; continuous unsafe state is rejected. |  |
| S08 | Bounded stream/stream join | Planned | NotRun | Not implemented in this foundation candidate; continuous unsafe state is rejected. |  |
| S09 | Key TTL deduplication | Planned | NotRun | Not implemented in this foundation candidate; continuous unsafe state is rejected. |  |
| S11 | Microbatch time/row triggers | Planned | NotRun | Not implemented in this foundation candidate; continuous unsafe state is rejected. |  |
| S13 | Native rate budgets | Planned | NotRun | Not implemented in this foundation candidate; continuous unsafe state is rejected. |  |

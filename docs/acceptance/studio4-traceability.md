# Studio 4 实施与验收追踪

<!-- Generated from metadata/product.json studio4. -->

实现、验收与可发布性分别记录；Planned 不计为已完成。

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

# 四引擎需求与验收追踪

<!-- Generated from metadata/product.json. -->

保留全部 69 项目标；下表是附件 78 项工作项的同源投影。Implemented 与 Verified 分开，能力组合边界见权威模型 limitations。

| ID | 优先级 | 阶段 | 能力 | 代码 | 验收 | 69 项目标映射 |
|---|---|---|---|---|---|---|
| CF-01 | P0 | R2 | ExecutableDefinition | Implemented | Pending final Linux evidence sealing | G08, G09 |
| CF-02 | P0 | R2 | Task Host | Implemented | Pending final Linux evidence sealing | G07, G20 |
| CF-03 | P0 | R2 | Control Graph Binder | Implemented | Pending final Linux evidence sealing | G05, G09, G11 |
| CF-04 | P0 | R2 | Precedence Evaluator | Implemented | Pending final Linux evidence sealing | G05, G11 |
| CF-05 | P0 | R2 | Task State / Outcome | Implemented | Pending final Linux evidence sealing | G06, G45, G46 |
| CF-06 | P0 | R2 | Ready Scheduler | Implemented | Pending final Linux evidence sealing | G12, G23 |
| CF-07 | P0 | R2 | Sequence Container | Implemented | Pending final Linux evidence sealing | G08, G10, G46 |
| CF-08 | P0 | R2 | For Loop | Implemented | Pending final Linux evidence sealing | G08, G10 |
| CF-09 | P0 | R2 | Foreach Loop | Implemented | Pending final Linux evidence sealing | G10, G45 |
| CF-10 | P1 | R2 | Enumerator Registry | Implemented | Pending final Linux evidence sealing | G07, G10, G25 |
| CF-11 | P0 | R2 | Scoped Variables | Implemented | Pending final Linux evidence sealing | G02, G05, G08 |
| CF-12 | P1 | R2 | Event Handlers | Implemented | Pending final Linux evidence sealing | G12, G48 |
| CF-13 | P0 | R2 | Failure Propagation | Implemented | Pending final Linux evidence sealing | G06, G12 |
| CF-14 | P1 | R2 | Transaction Policy | Implemented | Pending final Linux evidence sealing | G25, G46 |
| CF-15 | P0 | R2 | Deadline / Cancel | Implemented | Pending final Linux evidence sealing | G12, G16, G17, G18 |
| CF-16 | P1 | R2 | Retry Semantics | Implemented | Pending final Linux evidence sealing | G20, G46 |
| CF-17 | P1 | R2 | Child Package | Implemented | Pending final Linux evidence sealing | G17, G45 |
| CF-18 | P1 | R2 | Control Diagnostics | Implemented | Pending final Linux evidence sealing | G48 |
| DF-01 | P0 | R3 | Pipeline Definition | Implemented | Pending final Linux evidence sealing | G09, G13, G21 |
| DF-02 | P0 | R3 | Component Metadata | Implemented | Pending final Linux evidence sealing | G07, G21 |
| DF-03 | P0 | R3 | Schema Binder | Implemented | Pending final Linux evidence sealing | G02, G04, G05, G21 |
| DF-04 | P0 | R3 | Physical Planner | Implemented | Pending final Linux evidence sealing | G23 |
| DF-05 | P0 | R3 | Synchronous Execution | Implemented | Pending final Linux evidence sealing | G03, G22, G32, G35 |
| DF-06 | P0 | R3 | Asynchronous Execution | Implemented | Pending final Linux evidence sealing | G23, G24 |
| DF-07 | P0 | R3 | Component Lifecycle | Implemented | Pending final Linux evidence sealing | G21, G46 |
| DF-08 | P0 | R3 | Multiport Routing | Implemented | Pending final Linux evidence sealing | G22, G34, G35, G36, G42 |
| DF-09 | P0 | R3 | Error Policies | Implemented | Pending final Linux evidence sealing | G30, G42 |
| DF-10 | P0 | R3 | Backpressure | Implemented | Pending final Linux evidence sealing | G22, G23 |
| DF-11 | P0 | R3 | Buffer Ownership | Implemented | Pending final Linux evidence sealing | G03, G23 |
| DF-12 | P1 | R3 | Buffer Layout | Implemented | Pending final Linux evidence sealing | G02, G03, G32 |
| DF-13 | P0 | R3 | Spill | Implemented | Pending final Linux evidence sealing | G23, G24, G52 |
| DF-14 | P0 | R3 | Shared Compute | Implemented | Pending final Linux evidence sealing | G24, G37, G38, G39, G40, G58 |
| DF-15 | P0 | R3 | Early Validation | Implemented | Pending final Linux evidence sealing | G02, G09, G21, G25 |
| DF-16 | P1 | R3 | Explain / Inspector | Implemented | Pending final Linux evidence sealing | G23, G48 |
| DF-17 | P1 | R3 | Detailed Metrics | Implemented | Pending final Linux evidence sealing | G48 |
| DF-18 | P1 | R3 | Recoverable Pipeline | Implemented | Pending final Linux evidence sealing | G45, G46 |
| CM-01 | P0 | R4 | Connection Definition | Implemented | Pending final Linux evidence sealing | G08, G25 |
| CM-02 | P0 | R4 | Scope Resolution | Implemented | Pending final Linux evidence sealing | G08, G17, G25 |
| CM-03 | P0 | R4 | Registry Consistency | Implemented | Pending final Linux evidence sealing | G07, G25 |
| CM-04 | P0 | R4 | Factory | Implemented | Pending final Linux evidence sealing | G25 |
| CM-05 | P0 | R4 | Lease | Implemented | Pending final Linux evidence sealing | G25, G52 |
| CM-06 | P0 | R4 | Capabilities | Implemented | Pending final Linux evidence sealing | G25, G28, G29 |
| CM-07 | P0 | R4 | Credentials | Implemented | Pending final Linux evidence sealing | G08, G25 |
| CM-08 | P0 | R4 | Authorization | Implemented | Pending final Linux evidence sealing | G15, G16, G18, G25 |
| CM-09 | P1 | R4 | Pool | Implemented | Pending final Linux evidence sealing | G25 |
| CM-10 | P0 | R4 | Session Isolation | Implemented | Pending final Linux evidence sealing | G25, G46 |
| CM-11 | P0 | R4 | Thread Affinity | Implemented | Pending final Linux evidence sealing | G07, G25 |
| CM-12 | P1 | R4 | Transaction Enlistment | Implemented | Pending final Linux evidence sealing | G25, G46 |
| CM-13 | P1 | R4 | Metadata Discovery | Implemented | Pending final Linux evidence sealing | G25, G28, G29 |
| CM-14 | P0 | R4 | Schema Evolution | Implemented | Pending final Linux evidence sealing | G02, G25, G28, G29 |
| CM-15 | P1 | R4 | Bulk Reader / Writer | Implemented | Pending final Linux evidence sealing | G25, G28, G29 |
| CM-16 | P0 | R4 | Retry / Reconnect | Implemented | Pending final Linux evidence sealing | G25, G46 |
| CM-17 | P0 | R4 | Error Normalization | Implemented | Pending final Linux evidence sealing | G06, G25, G46 |
| CM-18 | P1 | R4 | Metrics | Implemented | Pending final Linux evidence sealing | G25, G48 |
| CM-19 | P0 | R4 | Resource Unification | Implemented | Pending final Linux evidence sealing | G14, G15, G16, G18, G25 |
| CM-20 | P1 | R4 | Native Driver Plugin | Implemented | Pending final Linux evidence sealing | G07, G20, G25, G50 |
| PP-01 | P0 | R5 | Execution Tree Builder | Implemented | Pending final Linux evidence sealing | G23 |
| PP-02 | P0 | R5 | Tree Graph | Implemented | Pending final Linux evidence sealing | G22, G23 |
| PP-03 | P0 | R5 | Shared Worker Pool | Implemented | Pending final Linux evidence sealing | G12, G23 |
| PP-04 | P0 | R5 | Unified Work Scheduling | Implemented | Pending final Linux evidence sealing | G12, G23, G24, G58 |
| PP-05 | P0 | R5 | Work Item Identity | Implemented | Pending final Linux evidence sealing | G09, G23, G45, G48 |
| PP-06 | P0 | R5 | Event-driven Ready Queue | Implemented | Pending final Linux evidence sealing | G22, G23 |
| PP-07 | P0 | R5 | Cross-Tree Backpressure | Implemented | Pending final Linux evidence sealing | G22, G23 |
| PP-08 | P1 | R5 | Source Partitioning | Implemented | Pending final Linux evidence sealing | G26, G27, G45, G58 |
| PP-09 | P1 | R5 | Partition Strategies | Implemented | Pending final Linux evidence sealing | G04, G58 |
| PP-10 | P1 | R5 | Parallel Aggregate | Implemented | Pending final Linux evidence sealing | G24, G38, G58 |
| PP-11 | P1 | R5 | Parallel Join | Implemented | Pending final Linux evidence sealing | G24, G37, G58 |
| PP-12 | P1 | R5 | Parallel Sort | Implemented | Pending final Linux evidence sealing | G24, G39, G58 |
| PP-13 | P0 | R5 | Ordering Contract | Implemented | Pending final Linux evidence sealing | G04, G23, G58 |
| PP-14 | P0 | R5 | Component Concurrency | Implemented | Pending final Linux evidence sealing | G07, G21 |
| PP-15 | P0 | R5 | Driver Concurrency | Implemented | Pending final Linux evidence sealing | G07, G25, G46 |
| PP-16 | P0 | R5 | Shared Resource Budget | Implemented | Pending final Linux evidence sealing | G23, G25, G52 |
| PP-17 | P0 | R5 | Cancellation | Implemented | Pending final Linux evidence sealing | G12, G23, G52 |
| PP-18 | P1 | R5 | Checkpoint Barrier | Implemented | Pending final Linux evidence sealing | G45, G46, G58 |
| PP-19 | P0 | R5 | Deterministic Semantics | Implemented | Pending final Linux evidence sealing | G04, G24, G58 |
| PP-20 | P1 | R5 | Detailed Scheduler Metrics | Implemented | Pending final Linux evidence sealing | G23, G48 |
| PP-21 | P2 | R5 | Adaptive Scheduling | Planned | Not in P0/P1 delivery (P2) | G23, G58 |
| PP-22 | P1 | R7 | Benchmark Harness | Implemented | Pending final Linux evidence sealing | G23, G24, G52, G53, G58 |

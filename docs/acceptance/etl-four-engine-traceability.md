# 四引擎需求与验收追踪

<!-- Generated from metadata/product.json. -->

保留全部 69 项目标；下表是附件 78 项工作项的同源投影。Implemented 与 Verified 分开，能力组合边界见权威模型 limitations。

| ID | 优先级 | 阶段 | 能力 | 代码 | 验收 | 69 项目标映射 |
|---|---|---|---|---|---|---|
| CF-01 | P0 | R2 | ExecutableDefinition | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G09 |
| CF-02 | P0 | R2 | Task Host | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G20 |
| CF-03 | P0 | R2 | Control Graph Binder | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G05, G09, G11 |
| CF-04 | P0 | R2 | Precedence Evaluator | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G05, G11 |
| CF-05 | P0 | R2 | Task State / Outcome | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G06, G45, G46 |
| CF-06 | P0 | R2 | Ready Scheduler | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G23 |
| CF-07 | P0 | R2 | Sequence Container | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G10, G46 |
| CF-08 | P0 | R2 | For Loop | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G10 |
| CF-09 | P0 | R2 | Foreach Loop | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G10, G45 |
| CF-10 | P1 | R2 | Enumerator Registry | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G10, G25 |
| CF-11 | P0 | R2 | Scoped Variables | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G02, G05, G08 |
| CF-12 | P1 | R2 | Event Handlers | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G48 |
| CF-13 | P0 | R2 | Failure Propagation | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G06, G12 |
| CF-14 | P1 | R2 | Transaction Policy | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G46 |
| CF-15 | P0 | R2 | Deadline / Cancel | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G16, G17, G18 |
| CF-16 | P1 | R2 | Retry Semantics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G20, G46 |
| CF-17 | P1 | R2 | Child Package | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G17, G45 |
| CF-18 | P1 | R2 | Control Diagnostics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G48 |
| DF-01 | P0 | R3 | Pipeline Definition | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G09, G13, G21 |
| DF-02 | P0 | R3 | Component Metadata | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G21 |
| DF-03 | P0 | R3 | Schema Binder | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G02, G04, G05, G21 |
| DF-04 | P0 | R3 | Physical Planner | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23 |
| DF-05 | P0 | R3 | Synchronous Execution | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G03, G22, G32, G35 |
| DF-06 | P0 | R3 | Asynchronous Execution | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G24 |
| DF-07 | P0 | R3 | Component Lifecycle | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G21, G46 |
| DF-08 | P0 | R3 | Multiport Routing | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G22, G34, G35, G36, G42 |
| DF-09 | P0 | R3 | Error Policies | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G30, G42 |
| DF-10 | P0 | R3 | Backpressure | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G22, G23 |
| DF-11 | P0 | R3 | Buffer Ownership | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G03, G23 |
| DF-12 | P1 | R3 | Buffer Layout | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G02, G03, G32 |
| DF-13 | P0 | R3 | Spill | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G24, G52 |
| DF-14 | P0 | R3 | Shared Compute | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G24, G37, G38, G39, G40, G58 |
| DF-15 | P0 | R3 | Early Validation | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G02, G09, G21, G25 |
| DF-16 | P1 | R3 | Explain / Inspector | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G48 |
| DF-17 | P1 | R3 | Detailed Metrics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G48 |
| DF-18 | P1 | R3 | Recoverable Pipeline | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G45, G46 |
| CM-01 | P0 | R4 | Connection Definition | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G25 |
| CM-02 | P0 | R4 | Scope Resolution | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G17, G25 |
| CM-03 | P0 | R4 | Registry Consistency | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G25 |
| CM-04 | P0 | R4 | Factory | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25 |
| CM-05 | P0 | R4 | Lease | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G52 |
| CM-06 | P0 | R4 | Capabilities | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G28, G29 |
| CM-07 | P0 | R4 | Credentials | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G08, G25 |
| CM-08 | P0 | R4 | Authorization | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G15, G16, G18, G25 |
| CM-09 | P1 | R4 | Pool | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25 |
| CM-10 | P0 | R4 | Session Isolation | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G46 |
| CM-11 | P0 | R4 | Thread Affinity | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G25 |
| CM-12 | P1 | R4 | Transaction Enlistment | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G46 |
| CM-13 | P1 | R4 | Metadata Discovery | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G28, G29 |
| CM-14 | P0 | R4 | Schema Evolution | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G02, G25, G28, G29 |
| CM-15 | P1 | R4 | Bulk Reader / Writer | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G28, G29 |
| CM-16 | P0 | R4 | Retry / Reconnect | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G46 |
| CM-17 | P0 | R4 | Error Normalization | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G06, G25, G46 |
| CM-18 | P1 | R4 | Metrics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G25, G48 |
| CM-19 | P0 | R4 | Resource Unification | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G14, G15, G16, G18, G25 |
| CM-20 | P1 | R4 | Native Driver Plugin | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G20, G25, G50 |
| PP-01 | P0 | R5 | Execution Tree Builder | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23 |
| PP-02 | P0 | R5 | Tree Graph | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G22, G23 |
| PP-03 | P0 | R5 | Shared Worker Pool | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G23 |
| PP-04 | P0 | R5 | Unified Work Scheduling | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G23, G24, G58 |
| PP-05 | P0 | R5 | Work Item Identity | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G09, G23, G45, G48 |
| PP-06 | P0 | R5 | Event-driven Ready Queue | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G22, G23 |
| PP-07 | P0 | R5 | Cross-Tree Backpressure | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G22, G23 |
| PP-08 | P1 | R5 | Source Partitioning | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G26, G27, G45, G58 |
| PP-09 | P1 | R5 | Partition Strategies | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G04, G58 |
| PP-10 | P1 | R5 | Parallel Aggregate | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G24, G38, G58 |
| PP-11 | P1 | R5 | Parallel Join | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G24, G37, G58 |
| PP-12 | P1 | R5 | Parallel Sort | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G24, G39, G58 |
| PP-13 | P0 | R5 | Ordering Contract | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G04, G23, G58 |
| PP-14 | P0 | R5 | Component Concurrency | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G21 |
| PP-15 | P0 | R5 | Driver Concurrency | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G07, G25, G46 |
| PP-16 | P0 | R5 | Shared Resource Budget | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G25, G52 |
| PP-17 | P0 | R5 | Cancellation | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G12, G23, G52 |
| PP-18 | P1 | R5 | Checkpoint Barrier | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G45, G46, G58 |
| PP-19 | P0 | R5 | Deterministic Semantics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G04, G24, G58 |
| PP-20 | P1 | R5 | Detailed Scheduler Metrics | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G48 |
| PP-21 | P2 | R5 | Adaptive Scheduling | Planned | P2 — not in R1–R5 scope | G23, G58 |
| PP-22 | P1 | R7 | Benchmark Harness | Implemented | Verified by current ABI 7 Linux regression within documented capability boundaries | G23, G24, G52, G53, G58 |

# 协议 3

Worker：u32 little-endian 帧长度（最多 64 MiB）+ Package UTF8 JSON。每帧独立 Execution，上游不能在帧中提供权限或密钥。回复同格式帧，JSON `{"version":3,"state":5,"result":0}`；状态值来自公共 Execution enum。旧 query/plugin 二进制操作格式已删除。

Batch wire：显式 version 3，little-endian、长度前缀、递归 Schema/Value；不含指针/struct padding。调用 ud_batch_deserialize 完整拒绝尾随数据。新恢复协议设计见 [transactions-and-recovery](architecture/transactions-and-recovery.md)，实际 capability/验证见 ssis-status.json。

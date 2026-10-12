# UniversalData 1.0 产品定位

状态：Planned；基线 main ed3237a9852ac29831d85d93c03fc8b173cdbf6f。此设计提交不代表运行完成。

产品是可嵌入的原生数据集成运行时和组件 SDK。嵌入式产品团队通过 C ABI、.NET 或 Java 提交同一 Package；数据工程师通过 CLI 执行；组件作者只实现驱动或算子。宿主提供身份、凭据、进程隔离和部署政策。

```mermaid
flowchart LR
Host[宿主/CLI/Worker] --> Package[Package Definition]
Package --> Runtime[唯一 Package Runtime]
Runtime --> Control[Control Flow]
Control --> Pipeline[Data Flow Task / Pipeline]
Pipeline --> Drivers[Connection Managers / Drivers]
```

契约：Package 是版本化执行单元，业务结果由 Package Runtime 判定；单个数据库事务只覆盖该连接，不保证多个文件/数据库原子提交。默认拒绝外部资源访问。例：宿主授权 input/output 后，嵌套 Sequence 的循环执行 Pipeline，事件记录任务/组件 ID，输出只有目标提交后可见。拒绝授权时 Failed/UD_AUTH，取消时 Cancelled，提交结果未知时 RecoveryRequired。

不做 Web 平台、微软专有代码复制、未验证的 dtsx/COM/SSISDB 兼容、默认全局 exactly-once、隐式云凭据。主代码归属 engines/package、engines/control-flow、engines/data-flow、connectors。Windows 特定 WMI/SMO/OLE DB 由可选插件承载。扩展不足返回 UD_UNSUPPORTED，目录中的 Planned 项不伪装成可用组件。

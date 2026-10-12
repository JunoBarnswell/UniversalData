# 四引擎产品模型

本次需求：2026-10-13 用户明确要求在 PR #4 完成附件 R0–R7 的全部 P0/P1，先设计、整批实现、最终集中 Linux 验收；Windows 豁免。附件的旧平台偏好不适用。实施基线 `6686597e5a51d6b0827816400faea028e065ad14`。

用户是 SDK 宿主开发者、ETL 流程设计者、连接器/组件作者与运维人员。产品对象是不可变 Project/Package/Environment/Deployment、词法变量、控制任务/容器/约束、Pipeline/Component/Port/Path、Connection Definition。运行对象是一个 Root Execution 及其 Activation 树。定义和执行分开，循环/子包不会修改定义。

设计器以原生 Registry discovery 与 Execution explain 为事实来源，读取配置 schema、端口、canonical schema/identity/lineage、生命周期、ordering/concurrency/recovery 能力。不能通过猜测组件名称决定阻塞、分区或连接能力。Headless API 是本轮交付，不要求 Web UI。

运行界面区分执行完成、目标提交、持久确认、恢复待核验；展示 task/tree/partition、行数、队列等待、Buffer resident/working/spill、租约与池结果。Skipped 不显示为成功；CommitUnknown 明确需要回执核验，禁止提供盲重试按钮。错误输出是同一 Pipeline 中的正规 Path，经过同一目标和 Journal。

产品不承诺 Microsoft .dtsx/COM/SSISDB/MSDTC 二进制兼容，也不承诺任意多资源全局 ACID/exactly-once。SSIS 的职责模型与本产品的硬配额、原生分区、回执、纯 C ABI 分别说明。平台证据必须绑定源码和实际命令；69 项既有目标及 CF/DF/CM/PP 条目统一存于 `metadata/product.json`，其他状态文件由它生成。

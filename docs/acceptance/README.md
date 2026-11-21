# 当前核心验收

[`metadata/product.json`](../../metadata/product.json) 是版本、能力边界、候选源码、门禁和产物的事实来源。`python scripts/product_model.py generate` 生成 JSON、进度、组件目录和流处理追踪等投影；`python scripts/product_model.py check` 验证投影。不要直接编辑生成文件。

- [当前进度](progress.md) · [发布门禁](gates.json) · [52 项审查清单](completion-goal.json)
- [流处理与 Designer 源码/验收追踪](studio4-traceability.md) · [组件目录](../product/component-catalog.md) · [SSIS 严格对照](ssis-strict-parity.md)

每条结论区分**源码可见、测试是否执行、验收范围、能否发布**。已审查源码的精确提交与对应 CI 记录在产品模型；历史 ABI、基准工件、旧账单阻塞与旧平台豁免不计入 SDK 10 验收。Linux 与 Windows 都是正式交付必验平台。

CORE 只证明实际执行的测试；真实提供方集成、SSIS Golden、完整 PRODUCT、独立安装消费者、真实浏览器恢复以及长时间运行，均按各自执行结果记录。跳过、等待中、外部环境缺失以及源码测试夹具存在，都不代表通过。没有完整当前证据，不挂发布通过章。

旧计划、`requirements.json`、`cases.json` 和 `evidence/` 保留供追溯；历史专项表保留当时事实，不用历史结果替代当前验收。

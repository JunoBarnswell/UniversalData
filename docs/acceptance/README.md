# 当前核心验收

metadata/product.json 是版本、组件范围、候选源码、门禁和产物的唯一事实来源。product_model.py 生成 progress.json、ssis-status.json、completion-goal.json、gates.json、组件清单及语言版本文件。人工编辑生成文件会被门禁拒绝。

按用户约定，整批源码与调用方迁移完成后集中编译和关键验收。本轮 Linux 执行构建、内存、并发、事务恢复及独立 SDK 消费；Windows 明确豁免。扩展保持真实 Planned/Partial。

基线 main 的合并、当前分支的实现、当前验收、当前产物分别记录。历史 ABI 4 的通过记录仅作历史证据，不能作为 ABI 6 的通过证明。当前源码未正式提交或合并时，mergeEvidence 为空。

门禁覆盖 Registry 所有权和合法类别、resident 无 codec、嵌套/投影去重、working quota/lease/fault cleanup、实际 Execution Tree 并发/容量/取消、分段 Journal 索引/快照/缺段/损坏/崩溃恢复、逻辑与持久 task facts、统一产品事实，以及安装后 C/NuGet/Maven/JNI 的同一 Package 和 typed wire。

GitHub Actions 运行 38270005891 的四个 job 均在启动前被账户账单/额度问题拒绝，实际步骤为 0；这是外部 Blocked，不能作为代码编译失败或通过记录。

requirements.json、cases.json 和 evidence 下的旧资料显式标为历史规划/证据；当前 69 项目标、核心门禁与扩展状态以产品模型为准。

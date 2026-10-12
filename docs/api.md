# API 3

公共对象、所有权与线程契约见 [public-c-abi](architecture/public-c-abi.md)，模型见 [product-model](product/product-model.md)，实际头文件在 include/ud。Package/Execution 取代全部 Job/Dag/Run 执行 API，不提供兼容层。

Schema 文档 version 3；Compute 文本 Schema/Plan 必须以 `ud-schema 3` / `ud-plan 3` 换行头开始。绑定失败或旧版本明确拒绝；UD_END 只用于游标结束。组件 callback 用户数据需存活到 Execution 销毁。

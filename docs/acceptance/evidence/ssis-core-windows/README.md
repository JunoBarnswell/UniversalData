# 本轮 Windows 核心验收证据

2026-10-12，源码 fingerprint 见 gates.json。Native 31/31、MSVC ASan 31/31，零编译告警；真实 C/NuGet/Maven/JNI 安装消费和三语言一致性通过。长原始日志保留于本机临时目录 UniversalData-ssis-20261012，提交摘要及哈希。旧 evidence 子目录属于此前提交及 ABI，不用于当前完成判断。

Linux 本轮免测，保留相同 C17 算子和 POSIX 平台代码。MSVC 没有 UBSan/LSan/TSan 的本轮结果；allocation/配额/故障清理有独立实际用例。扩展整体未完成，见 completion-goal.json。

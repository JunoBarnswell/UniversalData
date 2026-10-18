# Studio 4 前端与观测设计

唯一 PackageDocument + useHistory 是编辑事实；工作台标签只管理编辑器的身份与存活，不复制文档存储。Graph 编辑沿 graphEditing 的纯变更；Inspector、通用 Schema 属性、表达式、字段映射和预览保持 Native 驱动。命令面板调用同一动作闭包，键盘与按钮不维护两份逻辑。

features/workspace 管理标签与关闭；features/graph 管理画布呈现与选择；features/observability 管理事实订阅、时间轴和路径指标；shared/design-system 管理语义 Token/主题/密度。迁移原实现，不保留 wrapper 包裹另一个 Editor。API 层统一 Cookie、CSRF、AbortSignal、lossless JSON 与会话失效。

执行订阅采用 fetch SSE，保留已有认证和 CSRF头；不得另建匿名 EventSource 入口。订阅失败切换到同权限的快照+有限轮询，重连携带明确 revision。Native sequence/Catalog revision 使用字符串或 bigint，不能经 JS double 缩窄。事件缺口显式显示；刷新快照不生成虚拟时间轴事件。终态关闭订阅。

界面主题变量使用 surface/text/border/accent/status 语义，深浅主题同一 DOM 和焦点规则。舒适/紧凑只改变尺寸，不改变字段语义。窗口时间轴只说明配置边界，不在 TS 执行窗口算法。能力不支持显示明确原因，不能在表单中临时降级或改写用户配置。

错误场景：409 草稿冲突保留本地编辑并比较远程；发布/执行沿原授权确认；会话过期中止订阅；权限撤销关闭流；慢客户端/配额溢出显示缺口并补快照；Worker 不可达显示观测不可用，不能推断业务已失败。Preview 有明确源行/样本限制，Live 采样有真实执行身份。

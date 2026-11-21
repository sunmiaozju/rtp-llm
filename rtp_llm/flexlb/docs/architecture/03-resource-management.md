# Resource Management

容量约束分为三层：

1. V2 主队列用 `scheduler.maxQueuedRequests` 限制 Generation 排队总量；
   V2 计入主队列与 Prefill 子队列中等待交付的请求。
   PRIORITY 模式中，达到 `scheduler.ordering.highPriorityThreshold` 的请求可在队列满时
   抢占当前未在规划、优先级严格低于自己的排队请求。容量与阈值配置更新后对后续入队生效。
2. `ResourceMeasure` 在路由时判断单个 worker 是否可选。
3. `PrefillEndpoint` / `DecodeEndpoint` 用本地预留账本保护尚未反映到引擎
   `WorkerStatus` 中的调度决策。

主要代码：`balance/resource/`、`balance/endpoint/`、`DefaultRouter` 和
`PriorityScheduler`。

## EndpointRegistry

`EndpointRegistry` 按角色和 `ip:port` 维护 Prefill、Decode、P/D Fusion 与 VIT endpoint。
Encoder 有独立的 `EncoderEndpoint`，不会与同地址的 VIT 或 Generation endpoint 共用任务账本。
选中 Encoder 后，endpoint 记录尚未在 WorkerStatus 中出现的请求及预计未命中编码长度；
首次观察到任务前按 `seq_len - encoder_cache_hit_len` 记账。WorkerStatus 出现 waiting/running 任务后，
用该组去重后的未命中合成输入 `input_length` 替换预测值，直到任务结束；不扣减 `prefix_length`。
全命中组可能只上报 finished，其 `input_length` 是原始 prompt 长度，不计入编码负载；
finished、取消或失活后按请求 ID 清除本地记录。
同一地址的 `WorkerStatus` 代际变化时，registry 原子替换 endpoint 并关闭旧实例；
过期 worker 只能用当时观察到的 `WorkerStatus` 对象条件删除，避免删掉同地址的
新代际。

### 同地址重新注册的设计约定

同一 `ip:port` 要等旧 Worker 的下线清理流程结束后才能重新注册；新旧 Worker 不共用本地账本。
Worker 摘流后不参与新请求的选路；已选好 Worker 的 Decode 和 Encoder 请求在优雅下线窗口内仍按原地址下发。
Master 摘流后直接清理该 Worker 的本地记录，不等待选路线程。

## Prefill 可用性与队列

Prefill 选择先排除缺失可路由 rank 的 DP 组，DP 组识别方式与 Decode 相同：用
WorkerStatus 的 `dp_master_address` 分组，没有该地址的 worker 独立参与选择。排除
在缓存匹配之前完成，保证缓存匹配结果只覆盖 DP 组完整的合法 P 集合。

`PrefillResourceMeasure` 要求 endpoint 存活，且引擎观测到的有效待处理请求低于
`router.roles.prefill.availability.maxPendingRequests`（默认 64）。上下线切换使用
`router.availabilityHysteresisPercent`（默认 15）做滞回，避免在阈值附近抖动。

请求选中 Prefill 后进入该节点的 `PrefillQueue`。等待请求占全局与共享子队列名额，
不占已发布请求的 Prefill 并发名额。
如果选中的 Prefill 在入队前摘流，请求回到主队列重新选点，保留原顺序和超时期限。

## Decode 容量与本地预留

`DecodeResourceMeasure` 要求 endpoint 存活，并同时检查：

- 引擎可见负载未超过可选的
  `router.roles.decode.availability.maxEngineRequests`。
- 真实 KV 使用率未超过 `maxKvUsagePercent`（默认 90）；同样使用
  `availabilityHysteresisPercent` 做滞回。

Decode 选择先排除缺失可路由 rank 的 DP 组，再对剩余 endpoint 计算容量和选择成本。
DP 组使用 WorkerStatus 的 `dp_master_address` 识别；没有该地址的 worker 独立参与选择。

`DecodeEndpoint.reserve()` 在对外发布前建立 shadow 预留，记录并发槽位、硬 KV
和预期 KV。请求离开 Prefill 队列进入 Decode 调度之后，才选择 Decode 并预留。

WorkerStatus 到达后，`DecodeEndpoint.calibrate()` 用引擎已确认状态对账。对于取消或
发布结果不确定的请求，Engine fence 在权威终态到达前持有相应账本，防止
KV 或并发容量被提前释放并二次分配。

## 策略层使用

`ResourceMeasureFactory` 按指标注册 Prefill / Decode measure。`DefaultRouter` 及其策略先
排除不可用 endpoint，再根据等待时间、KV 负载、cache 亲和性和配置的候选集
选择 worker。QUEUE 路径成功后，endpoint 账本由 `PriorityScheduler` 负责提交、
回滚和终态释放。

## 观测

`BatchSchedulerReporter` 上报 endpoint 队列深度、等待时间、batch 大小、inflight、
Decode 负载和本地 KV 预留。`PrioritySchedulerReporter` 上报优先级准入、抢占、
取消和分层 Decode 负载。观测失败不得改变调度结果或资源所有权。

# 可观测性

## 观测数据产生在什么时刻

一次 Generation 请求可以先在 P 排队，再占着 P 等 D，最后拿到地址。要解释这条请求，至少要区分三个时刻：**选点时**只能预测耗时和缓存命中；**调度结束时**才知道最终返回码和累计等待；**引擎上报后**才知道实际命中。把三者都记成“请求完成”，就无法区分 FlexLB 返回慢和引擎执行慢。

| 数据从哪里来 | 代表什么 | 在哪个线程做什么 |
|---|---|---|
| `Schedule` 入口、选路、队列重试 | 单次调用的阶段时间戳、候选与选中理由、各类阻塞次数。重试三次不是失败三个请求。 | **请求主链路**，包括调度队列的消费线程。需要当场记下决策用的值；过后重新读节点，负载可能已经变了。新增请求级观测只累计必要字段。 |
| 调度 Future 完成、RPC 响应收尾 | main/P/D 最终等待时长、成功或拒绝原因；RPC 层另有返回码、服务端耗时和请求 PV。调度已发布地址与客户端已收到响应是两个时刻。 | **请求完成路径**。V2 在 Future 完成时提交异步指标任务；RPC 在响应收尾处记录交付结果。两者没有共用一个可以代表所有完成的 `finally`。 |
| WorkerStatus 轮询与反馈 | 引擎已观察到的运行或完成任务、实际命中长度。地址返回时拿不到这些值。 | **后台轮询路径**。在对应 worker 的反馈到达后，按请求 ID、角色、实例代际关联预测；观测回调在 worker 状态锁外执行。不让 Schedule 等待反馈。 |
| 周期采样、健康探测与选举事件 | 当前各队列人数、节点在途数、存活状态、KVCM 来源或主从变化。即使没有请求完成，系统状态也可能改变。 | **后台路径**。读取当前值或记录状态变化，不依赖任何请求的完成回调。采样点之间的短暂峰值可能看不见。 |

### 请求主链路上的采集与异步上报

以“等 D 是因为哪类容量不足”为例：每次尝试都必须在 D 消费线程记录当时的阻塞原因，但没必要让这个线程每次都调用监控后端。请求结束后，[`V2QueueReporter.completed`](../../../flexlb-sync/src/main/java/org/flexlb/service/monitor/V2QueueReporter.java) 读取本次请求的阶段、优先级和终态，交给 [`AsyncRequestMetrics`](../../../flexlb-sync/src/main/java/org/flexlb/service/monitor/AsyncRequestMetrics.java)。上报线程分别统计尝试次数和去重后的阻塞数。上报队列有界；满时丢弃观测任务，以 `schedule_v2.metrics_report.dropped{reason=queue_full}` 记录丢弃量，不在请求线程补报。过载期间指标可能少样本，不代表流量真的下降。

**排队计时到调度结束，响应计时到交付结束。** 新增指标时，前者在调度 Future 完成时提交；后者在响应回调的 `finally` 中记录结束时间和结果，再异步提交。成功、拒绝、异常、取消和超时都要考虑，每个适用出口只报一次。队列满可以丢指标，不能丢请求清理或容量归还。

**现有实现没有全部异步化。** [`FlexlbServiceImpl.completeSchedule`](../../../flexlb-api/src/main/java/org/flexlb/httpserver/FlexlbServiceImpl.java) 在调用响应 observer 后进入 `finally`：本地记录服务端耗时，调用存量 reporter，并在符合条件时构造、序列化 PV。Logback 异步写文件，并不会把构造 PV 和 JSON 序列化移出完成线程。`finally` 保证进入这段代码后即使交付抛错也能收尾；它既不切线程，也不保证每个入口请求都会走到这里。

### 排查入口

- **请求仍没返回地址：** 看 `schedule_v2.queue.depth` 的 `queue` 标签定位积压阶段，用 `schedule_v2.queue.oldest_wait_ms` 看该阶段最老请求已等多久。再对照已结束请求的 `schedule_v2.queue.wait_ms` 和 `schedule_v2.queue.blocked_attempt.qps` 解释趋势。旧 `batchQueue` 深度不是 V2 三层队列的深度；仍在等的请求也还没有最终等待时长样本。
- **P 命中更多却没被选中：** 按请求 ID 和 `phase` 找决策副本的 PV，看 `routingDecisions` 里的有效命中、已有负载耗时和新请求预计耗时。`kvcm.log` 只有查询结果，没有最终选点理由。
- **地址已返回但占用还没降：** 对照逻辑节点的 `schedule_v2.prefill.inflight`、worker 上报和[容量释放条件](../../design/scheduling/02-capacity-and-release.md)。RPC 成功说明已返回路由结果，不是引擎 Finished。

## 日志

`logback-spring.xml` 定义 application、PV、sync、sync-consistency 和 FlexLB 文件输出。
日志路径由 `FLEXLB_LOG_PATH` / `FLEXLB_APP_LOG_PATH` 控制，AsyncAppender queue
由 `FLEXLB_LOG_ASYNC_QUEUE_SIZE` 控制。

logger group `flexlb` 包含 `org.flexlb`、`flexlbLogger`、`syncLogger`、
`syncConsistencyLogger`。`FlexlbLogManager` 监听配置快照：

- `observability.logging.level` 热更新整个 group；
- `observability.logging.stdoutEnabled` 动态挂载或卸载 root/PV 的
  `CONSOLE-async`，文件输出始终保留。

`/flexlb/update_log_level` 继续提供显式 HTTP 调级入口。

`kvcm.log` 通过独立 `kvcmLogger` 记录每次收到的 KVCM 缓存查询结果，仅包含 requestId
和 `hosts` 列表；每个节点保留 `hostIpPort` 及原始 `local/global` 命中 block 数。
包含零命中、重复和空名称节点，不做选路过滤。日志位于
`flexlb.log.path`，沿用异步写入和按天/大小滚动：50MB、保留 5 天、总量 4GB。
未收到响应的传输异常沿用现有异常处理和监控。

### 请求 PV 与回放

gRPC Schedule 先记录 arrival，再读取配置并构建调度上下文。
请求 ID 优先从非空白的 `request_id_str` 读取，否则将 int64 `request_id` 转为字符串；
两个字段均未提供时使用 int64 的 proto3 默认值 0，解析结果为 `"0"`。
业务请求、调度回调和转发补偿复用解析结果。
已构建上下文的入口错误可以记录 `ENTRY_ERROR` PV；`enableFallback` 提前返回或上下文构建失败时，没有该上下文，也就没有这份请求 PV。
同一业务请求先调度 Encoder、再调度 Generation 时，Schedule PV 的 `phase` 区分两次决策，
`server_status.role` 保留实际选中的角色。Encoder 选点失败和 WorkerStatus 生命周期异常
沿用请求错误码与状态日志。调度摘要日志输出可选的 `encoder_cache_hit_len`，Schedule PV 输出
可选的 `encoderCacheHitLen`；缺失值与显式 0 可区分。

本地 Schedule 的调度 Future 有自己的完成回调。若该回调先取得 `completeOnce` 的完成标志，就进入响应交付和上述 PV 收尾；若入站 gRPC 取消监听器先取得标志，它会尝试取消本地调度，后到的完成回调只清理监听器，不再向已取消的 RPC 发送响应，也不经过该响应 PV 路径。因此不能用 PV 条数反推所有 RPC 取消次数。
PV 顶层与嵌套 `response` 的成功标识、错误码和错误消息表达同一终态；准入拒绝原因仅在
确有准入拒绝时记录，成功、取消、超时和其他无准入拒绝原因的结果省略该字段。

`totalUs` 是入口到记录 PV 前的单调时钟耗时；`arrivalMs` 是服务入口时间减调用方
`requestTimeMs`，受两端时钟偏差影响。gRPC 路径记录收到的
`requestMessageBytes`（gRPC 接收的未压缩 protobuf 字节数，不含帧头）。transport tracer
累计已读取/解压的字节，并通过 gRPC Context 传到入口；调度线程不为监控额外遍历 PB。
`app.request.message.bytes` 保留该 Message 口径；绕过 transport 的直接调用不制造 0 样本。

每条请求有自己的 `RoutingTelemetryState`。选路各阶段依次记录尝试次数和原因，写 PV 时才生成不可变的 `RoutingTelemetry` 快照。

`routingDecisions` 是选点时的记录。Prefill 候选明细先放选中节点，再按遍历顺序补到最多 5 个；发生截断时，最快或命中最多的节点未必在明细里。核对亲和阈值应看同一条 PV 的 `cacheAffinity`，不要只用这几行反推所有候选。候选耗时以毫秒计，未知的估计省略，不记为零。

单条请求查看 Prefill 或 PDFusion 的 `routingDecisions.cacheAffinityDecision`；看整体分布则按 `app.cache.affinity.decision.qps` 的 `decision` 标签分组。每个值的触发条件、零命中的四种区别，以及字段缺失的含义见 [Prefill 缓存亲和决策原因](../../design/routing/02-generation-routing-decisions.md#prefill-缓存亲和决策原因cacheaffinitydecision)。

启用缓存亲和且存在已建模 TTFT 时，PV 的 `cacheAffinity` 保存决策时的最快 TTFT、
`maxExtraTtftMs`、`minPrefixHitPercent` 和 TTFT 上限。对于有效命中高于最快节点、且满足
最低命中率的候选，记录其中 TTFT 最短的节点地址、TTFT 和命中 Tokens。`OVER_CAP` 的
`overCapMs` 等于该候选 TTFT 减去上限，表示至少还需增加多少毫秒预算才能使用缓存亲和。
没有已建模的更高命中候选时省略超出量。`app.cache.affinity.over.cap.ms` 以 TIMER
记录 `OVER_CAP` 决策的超出量。
Prometheus 导出为 `flexlb_app_cache_affinity_over_cap_ms_seconds` 的分布系列，值单位为秒；
展示毫秒时乘以 1000。选路重试及放弃的选择不产生该指标样本。

缓存反馈以请求 ID、角色和 Worker 实例代次关联路由预测，`worker` 使用完整的
`ip:port@engineIndex` 逻辑身份；`prefill_worker_status` 同时记录 `workerIp` 和 `engineIndex`。
首次有效的 RUNNING 命中反馈或 FINISHED 反馈触发统一上报：理论命中、策略所选命中、
KVCM 预测与本地/global 匹配、Local Standby 预测，以及实际命中。

比较**选中节点的本地命中率**与 **KVCM 返回节点中的最高本地命中率**时，按相同角色和时间窗口取 Counter 增量之和：

| 指标 | 每条请求累计的 token 数 |
|---|---|
| `app.cache.kvcm.selected.local.match.tokens` | 最终选中节点的 KVCM 本地命中 |
| `app.cache.kvcm.selected.global.match.tokens` | 最终选中节点的 KVCM global 命中（已包含本地命中） |
| `app.cache.kvcm.best.local.match.tokens` | 同一次 KVCM 查询中单个节点最多能在本地命中的 token 数，最多计入本次输入长度 |
| `app.cache.kvcm.selected.input.tokens` | 同一批反馈样本的输入 token 数，作为上述三个指标的共同分母 |

逐请求对照时，看 `cache_hit_comparison` PV 的 `kvcmPrediction.localPredictionTokens`（选中节点）和 `kvcmPrediction.bestLocalMatchTokens`（本次查询最高值）。`bestLocalMatchTokens` 为 `0` 表示没有本地命中，为 `-1` 表示没有可用的 KVCM 本地最高值。

例如输入 1,000 tokens，两个节点的 global 命中都是 900，但本地命中分别是 400 和 800。选中前者时，选中节点的本地命中率是 40%，最佳本地命中率是 80%。跨请求先汇总 token 再相除；这个差距反映本地缓存利用机会，不能单独归因于 `OVER_CAP`。

每项独立判断是否可用；有效的空匹配产生 0，查询失败不产生预测数据点。
Engine 的 `prefixLengthValid` 表示实际命中值有效；有效的 0 表示零命中。
FINISHED 缺少有效实际值时仍上报可用的理论和预测数据，实际值及其输入 Tokens Counter 不上报。
比较事件中不可用的实际命中为 `-1`，不可用的预测和理论命中为 `null`；差值要求两侧均有效。
每个关联记录最多生成一次 `cache_hit_comparison` 和一次 `prefill_worker_status`。
实际命中 Counter 及输入 Tokens Counter 用于计算实际全局命中率。
比较事件包含理论命中、实际命中、路由预测、KVCM 本地匹配、KVCM 本地加远端的 global 总匹配，
以及 Local Standby 预测；差值统一为实际值减预测值。
RUNNING 反馈后保留关联，使之后的首 Token 或 FINISHED 反馈仍能补充状态 PV；
比较事件和状态 PV 分别通过原子标记最多生成一次。
FINISHED 在 `finally` 中按请求 key 与该次读到的关联对象条件删除，即使实际命中无效或反馈处理抛出异常也会清理，
且不会误删同 key 后续写入的新关联。已取得关联对象的并发回调仍共享原子标记，删除不影响已启动的异步对照。
`CacheHitFeedbackTracker` 是独立的 Spring Bean，注入 `ConfigService` 以初始化并热更新关联表容量和超时，
`CacheAwareService` 注入同一个 Tracker 处理预测与反馈。
预测关联上限由 `observability.cacheHit.feedbackMaximumEntries` 配置，默认 100,000 条；
未收到 FINISHED 的记录按 `observability.cacheHit.feedbackTimeoutMs` 判断反馈静默超时，默认 60,000 ms（1 分钟）。
持续收到该请求的活跃状态时保持关联；超时后的迟到反馈无法再关联。理论与 Local Standby 对照异步完成，
反馈阶段各自最多等待一秒；不可用的项省略，其余指标正常输出。观测回调在 Worker 状态锁外执行。

`tools/pv_request_replay/build_workbook.py` 支持 `routingDecisions` 和
`shortestTtftDecisions` 两种日志结构，以及包含两种结构的日志窗口。Requests 展示请求与缓存
证据；Routing Decisions 展示 CostBased 候选、策略参数、拒绝统计和决策组；Decision Snapshot
Top5 展示 `shortestTtftDecisions` 的 token-work 估计。预测耗时与 Engine 实测耗时分别展示。
空值表示未记录，零表示已记录且数值为零。HTML 回放通过工作簿读取这些数据，按请求展示候选
及缓存对照。回归测试验证原始 PV 到工作簿、HTML 的字段传递、单位、角色和实例隔离。

## 指标

`FlexMonitor` 提供 GAUGE、COUNTER、QPS 与优先级窗口抽象。opensource 默认使用
`NoOpFlexMonitor`；internal profile 可启用 KMonitor/Prometheus provider。

指标名集中在 `MetricConstant`，主要覆盖：

- engine health、worker status 与状态转换时延；
- routing、queue、dispatch、forward-to-master；
- cache hit、KVCM retry/failure、Local Standby capacity/fallback/comparison；
- 线程池、graceful lifecycle；
- request payload、optimizer trace 与 PV decision 数据。

新增请求级指标在选路、重试和返回响应的主链路上只做本地记录，请求结束后统一异步上报。
响应不等待指标，队列满时也不回退到请求线程执行。已有同步上报按链路逐步迁移。

`app.engine.balancing.master.all.qps` 与 `app.engine.balancing.master.all.rt` 使用相同的
`phase`、`code`、`priority` 标签，分别记录调度完成速率与包含排队的调度响应耗时（毫秒）。
`phase=ENCODER` 表示 Encoder 独立调度，`GENERATION` 表示 Prefill、Decode 或 PDFusion 调度，
`VIT` 表示只返回 ViT 地址的选路调用。每次完成按实际返回码上报；已转发到 Master 并取得响应的
调用由决策 Master 上报，转发节点只记录自身的转发指标。正常 KMonitor 聚合窗口为 20 秒。

`auto_tpm.request.count{phase,priority}` 在调度上下文构造完成后记录接入速率。同一业务请求可以
产生多次 Encoder 和 Generation 调度调用，各阶段 QPS 表示调度调用次数。不同阶段的平均耗时
分别展示；跨阶段汇总使用耗时总和除以样本数。

V2 同时上报主队列的配置上限、主队列与 Prefill 子队列的合计等待人数，以及各角色的
共享子队列占用、容量和逻辑端点数。分阶段的等待人数和等待时长见 V2 队列指标。

V2 的 Decode 预占发生在 Prefill 出队之后。区分本次受阻与最终请求失败时，按 `DECODE_WAITING` 阶段查看 V2 阻塞原因和尝试次数。

`app.flexlb.queue.subqueue.blocked{role,reason=SHARED_SUBQUEUE_FULL}` 是入所选
子队列失败的尝试 QPS：共享 C 满且本机低优等待项也未替换成功时记录一次；同一请求
多次重试可产生多次样本，不表示最终客户端失败。`app.flexlb.queue.global.residence.ms`
和 `app.flexlb.queue.subqueue.residence.ms{role}` 在请求实际离开各队列时记录
本次入队到出队的单调时钟时长，包含交付、取消、抢占、停机等出口；监控按 Timer
样本计算平均值和分位数。撤回后重入按新的一次停留计时，旧的
`routing.queue.wait.time.ms` 保持兼容口径。仍在等待的请求不产生 Timer 样本，
使用 `app.flexlb.queue.global.oldest.wait.ms` 和
`app.flexlb.queue.subqueue.oldest.wait.ms{role,engineIp}` 查看最老等待年龄，空队列报 0。
队列深度与容量计数来自不同并发点，采样时允许短暂不一致；只有持续偏差才值得排查漏账。

`app.engine.health.check.engine.worker.number.service.discovery.result{role}` 统计所有已配置 endpoint
成功返回的原始 Host 数量；空返回上报 0，即使健康检查继续沿用缓存。某 endpoint 查询失败时
不以缓存或 0 伪装完整的发现结果，本轮跳过总数上报，失败由 discovery error/timeout 指标记录。
`app.engine.health.check.engine.{prefill,decode,encoder}.worker.number` 则统计 WorkerDirectory
中已展开的逻辑 Worker 数。两者均使用公共 BIZ_NAME 与 Master 标签定位部署，不含 model 标签。
两种数量含义不同，不能互相替代。

Block Size 保留 `app.cache.block.size{role}`：同一角色的 Worker 共享相同配置，
每 2 秒读取该角色首个大于 0 的 WorkerStatus 值，覆盖 PREFILL、DECODE、PDFUSION、ENCODER。
首次状态到达前 EngineObservation 已初始化，blockSize=0，不上报未就绪值。
该指标不增加 engineIp 标签；大盘按 BIZ_NAME 与 role 展示，删除旧 model 筛选。

正常请求、缓存、排队和派发的 QPS/Counter，以及逐请求或逐 Step 的耗时与 Tokens 指标，Master 公共身份仍保留。健康检查失败、状态更新超时和
在途 TTL 清理作为节点定位例外保留地址。逐端点当前占用、队列深度和 KV 水位不属于高频事件计数。
旧 batchQueue 长度每轮按 role/priority 跨端点求和，消失的优先级不额外补零。
空端点队列不汇报优先级队列长度，不生成 `priority=0、depth=0` 回退，也不保存上一轮优先级状态。

WorkerStatus/cache 的成功轮询周期、RPC 耗时、运行负载、完成任务表大小和 cache key 数按 role 汇总样本；活跃任务数每 2 秒按 role 汇总当前 WorkerStatus 活跃任务表。
KV 已用、可用和使用率保留裸 IP，单机总容量按 role 共享，要求同部署同角色容量一致。
健康检查失败保留引擎地址和错误码。缓存预测对照保留 role/group/taskState/cacheMatchSource，
不携带引擎地址；PV 和路由/cache 内部继续使用完整 logical identity。
对照的绝对 Token 差值不依赖分母，输入 Token 数缺失或非正时仍可记录；Counter 和比例仅在
输入 Token 数为正时报告，不能将绝对差值的样本数量当作加权命中率的分母。

`app.engine.balancing.master.select.detail` 保留原有 `role/success/code` 标签；独立的
`app.engine.balancing.master.worker.select.detail` 增加有限枚举 `reason`。
`app.engine.worker.info.step.latency.var` 与 `app.engine.worker.info.running.query.len.var`
按 role 上报逻辑 endpoint 方差，不含 model。后者 Prefill 使用 work-ms，Decode 和状态角色使用
活动任务数，方差单位分别为 work-ms² 与 count²，不能跨角色混合比较数值。
Step 延迟方差单位为 ms²。上述指标保留原有名称，通过标签聚合和面板说明表达实际口径。
`app.cache.hit.count` 和 `app.cache.input.tokens` 是按 role 汇总的 Token Counter，
全局命中率使用同一窗口内的命中 Token 增量除以输入 Token 增量，不能求单请求命中率的平均值。
`app.engine.zk.master.event` 保留按事件类型上报 `1.0` 的既有 Gauge 口径；
独立的 `app.engine.zk.master.event.time.ms` 用 epoch-ms Gauge 展示最近事件时间。
`app.engine.worker.status.scheduler.to.running.ms` 中的 scheduler 是 Engine 调度器；该值
与 `app.engine.worker.status.engine.waiting.to.running.ms` 相同，均取 Engine 的
`running_entered_time_ms - waiting_entered_time_ms`。前者保留已有展示口径，后者显式标明观测来源；
不能将二者相加或作为 Master 与 Engine 的两端耗时比较。
`remote_kv_wait_ms` 是引擎报告的时长，0ms 可以表示没有远程 KV 等待，仍作为有效样本。
phase 时间戳的 0 表示未知，缺少时间戳时不报告相减得到的耗时。

Engine received→waiting 和 waiting→running 使用 WorkerStatus 中 Engine 自己记录的
阶段时间戳计算。轮询间隔影响 Master 收到样本的时间，不用探测到状态的时间估算阶段耗时；
完成任务的时间戳完整时，无需在轮询中逐个观察到这些阶段。

gRPC 服务端执行器与批次发送执行器初始化后立即通过 `FlexMonitor` 上报一次状态，之后每 2 秒上报忙碌线程数、总线程数和
排队任务数；gRPC 服务端另报最大线程数、累计拒绝任务数。它们与其它线程池使用相同的
provider：KMonitor 部署上报 `whale-lb.grpc.server.executor.*` 和
`whale-lb.dispatch.executor.*`，无需单独接入 Micrometer 采集。
忙碌线程、排队数、总线程数、最大线程数使用 GAUGE，scrape 读取最近一次上报的快照，
最多滞后 2 秒；不能用这些快照捕获短于采样间隔的峰值。瞬时指标保留原名。
拒绝任务数使用 `grpc.server.executor.caller.runs` COUNTER。每次拒绝发生时直接上报 1，
由监控 provider 累加，不维护本地累计值或上次上报位置。初始化及每 2 秒上报 0，
使没有拒绝事件时指标仍可查询；零上报不增加计数。Master 重启后计数重新开始。拒绝数包含队列或线程饱和、
线程池关闭后提交触发的拒绝。保留 `grpc.server.executor.caller.runs` 上报名，
面板显示“任务拒绝数”，使用 `increase(...[1m])` 查看窗口拒绝数。
已初始化的空闲线程池持续上报忙碌线程数和排队任务数为 0，总线程数仍包含空闲线程。

`JvmGcMetricsReporter` 接收 JVM 的 GC 通知，每次记录一次回收和本次暂停毫秒数，
通过 `FlexMonitor` 的 NORMAL COUNTER（20 秒聚合）上报 `app.jvm.gc.collection.count` 与
`app.jvm.gc.pause.total.ms`。按 `gc`、`collector`、`pid` 区分进程与收集器：
G1 的 `young` 包含 Mixed，`full` 对应 `G1 Old Generation`，`concurrent` 只统计
并发周期中的暂停阶段，不表示整个并发标记周期。其它收集器标为 `other`。
每秒上报零增量，保持未发生 GC 的序列可查询；关闭组件时移除 GC listener。
面板展示统计窗口两端的计数差值，以及暂停毫秒数差值除以次数差值；
例如窗口内两次暂停为 10 ms、30 ms，展示 2 次和平均 20 ms。
没有 GC 的窗口次数为 0，平均耗时为空。底层计数器用于差分，不作为累计趋势展示；
PID 隔离重启前后的计数器，缺少窗口边界样本时保留无数据。

Encoder 的 worker 数由周期指标上报 `app.engine.health.check.engine.encoder.worker.number`；
WorkerStatus 成功轮询上报 `app.flexlb.encoder.pending.request.count` 和
`app.flexlb.encoder.selection.load`、`app.flexlb.encoder.uncached.token.load`。
三项按 `engineIp`、`role=ENCODER` 标记，分别表示尚未在 WorkerStatus
看到的本地选点数、`running + waiting + pending` 并发数和在途编码工作量代理值。
最后一项在首次 WorkerStatus 前采用 Client 的 MM token 预测值，之后采用活动任务的合成输入
`input_length`；两者口径可能略有差异，finished 中的长度不参与该指标。
通用 KV 指标仍上报，但不参与 Encoder 选点。
`app.flexlb.tracked.request.count{role=ENCODER,engineIp=scheduler}` 统计 Encoder 请求表。
Generation 分阶段指标见 V2 队列指标。Decode `app.flexlb.inflight.request.count` 继续统计
本地未确认预留数减去 Master 排队数；收到 `KV_ALLOCATED/RUNNING` 后不再计入。
Decode 总负载仍使用 `app.flexlb.decode.total.load`（已确认请求加本地预留，含排队）。
`/rtp_llm/inflight_status` 的 `scheduler_tracked` 返回两个阶段正在跟踪的请求总数，包含排队请求；
`scheduler_inflight` 保留为该值的兼容别名，供现有清账和测试工具使用。它不表示节点侧的未确认在途数。
Decode 预留字段 `inputKvTokens = max(0, seqLen)`，
`inputAndMaxOutputKvTokens = inputKvTokens + max(0, maxNewTokens)`（溢出时饱和）。
两者分别表示输入预留和输入加最大输出预留，不是输出长度预测，后者包含前者。
引擎确认后的容量使用引擎上报的实际 KV 数，不再使用请求上限。
监控分别使用 `app.flexlb.decode.inflight.hard.kv.reserved.tokens` 与
`app.flexlb.decode.inflight.kv.reserved.tokens`，包含排队预留；中文图例使用上述计算口径。
周期统计复用 Prefill 快照，调度器一次遍历计算数量和最大年龄；Decode admission
上报仅收集数值，不构造请求明细。

`auto_tpm.preemption.target_invalid.count` 统计 Decode 抢占目标校验失败的尝试次数，
使用 QPS + NORMAL（20 秒聚合）上报。一次计划即使包含多个目标也只计一次，标签仅有
`mode=return` 和固定的 `reason`：`victim_state_changed`（预留或派发状态变化）、
`victim_already_claimed`（目标已被其他抢占占用）、`priority_not_preemptible`（目标优先级不允许抢占）。
容量不足不计入此指标。
未发生事件时可能没有时间序列，不能据此断言上报链路正常。
`auto_tpm.request.lifecycle.failures.total{stage}` 是 NORMAL COUNTER（20 秒聚合），
`stage=registration` 记录注册异常，`stage=terminal_cleanup` 记录资源清理失败或请求最终状态提交异常。
初始化及周期采样对这两个 stage 报告 0，尚未发生异常时也能显示已初始化的计数。
这些异常保留 request_id 日志供定位，指标标签不包含请求 ID 或错误文本。
旧 `auto_tpm.inflight_settle_miss.count{kind}` 随独立 inflight 台账的清理路径一起停用：
当前注册、资源跟踪和结束记录由同一个 RequestSlot 保持，迟到或重复回调无需找回旧台账。
它不映射为抢占目标校验失败。旧竞态告警应退役，另启用新的生命周期异常增量告警。

上报器分布在 common、grpc、cache、sync 模块。新增指标应复用现有 reporter ownership，
不要恢复已删除的旧监控层。

## Multi-engine endpoint 与观测

`MODEL_SERVICE_CONFIG` 的 endpoint 支持 `multi_engine_num`（默认 1）。显式
`worker_status_port` 必须处于 `[1, 65535]`；N>1 必须指定该端口，并保证
`worker_status_port + N - 1 <= 65535`。每个 index 使用独立的 status gRPC 端口，
同一物理地址上的逻辑 worker 仍共用业务 HTTP/gRPC 地址。`protocol` 只用于解释服务发现返回的 worker 端口。
N=1 沿用 discovery 的 legacy gRPC port，因而兼容未配置 `worker_status_port` 的 RTP-LLM。

逻辑 worker identity 为 `ip:http_port@engineIndex`，包括 N=1 的 `@0`。
Encoder 和保留逐端点的记账指标在 N=1 使用 physical `ip:http_port`，
在 N>1 使用 logical identity `ip:http_port@engineIndex`；step、阶段耗时和缓存预测对照不携带引擎 IP。
网络连接仍使用
物理地址。schedule 在 N=1 时省略 `engine_index`，内部 identity 仍保留 index 0。

### Scheduler step 采样

WorkerStatus 的 `last_step_metrics` 表示最近完成的非空 scheduler step，包含 step ID、完成时间、
总调度 Token 数、Prefill 请求数、Prefill Token 数、Token 预算及预算填充率。
字段缺失表示引擎尚无可用 step 观测；纯 Decode step 的 Prefill 请求数和 Token 数为 0。
FlexLB 按逻辑 Worker 和 step ID 去重后，通过 `app.engine.worker.step.*` 上报五项数值。
`phase=prefill` 表示该 step 含 Prefill，`phase=decode` 表示纯 Decode；角色、组和
phase 标签保留，跨引擎汇总采样。预算填充率为总调度 Token 数 / Token 预算，包含 Decode Token。

这些指标是 WorkerStatus 轮询采样，轮询之间完成的中间 step 不会全部保留。KMonitor 使用
GAUGE + SUMMARY 聚合实际采样值；Micrometer provider 只暴露最近上报值，不提供逐 step 分布。
指标标签不包含 step ID 或完成时间。凑批比较应筛选 `phase=prefill`，避免纯 Decode step
稀释 Prefill 预算填充率。Turbo 转发后的指标前缀为 `dashscope_turbo_backend_flexlb_app_engine_worker_step_`。

### Prefill 非末块 Token 数

完成 Prefill 请求后，`app.engine.worker.status.prefill.nonfinal.chunk.min.tokens` 和
`app.engine.worker.status.prefill.nonfinal.chunk.max.tokens` 分别上报该请求内非末块 Chunk 的
最小、最大 Token 数。末块可能不足一个完整 Chunk，不参与统计；没有非末块样本的请求不向
这两个指标报告 0，仍正常报告 `app.engine.worker.status.prefill.step.count`。
例如分为 8192、8192、616 Tokens 的请求，Step 数为 3，非末块最小和最大值均为 8192。

两个指标保留请求内统计口径，以 GAUGE + TRIVIAL 注册，KMonitor 每 60 秒发送一次聚合结果，
不生成额外的 SUMMARY 分位指标；显式配置的 `FLEXLB_MONITOR_PRIORITY` 可覆盖此周期。
曲线均值分别是有非末块样本请求的最小值均值、最大值均值，不是所有 Chunk 的均值或窗口极值。
业务调用只更新本地统计，不逐请求发送网络数据，也不额外抽样。指标名以单位 `tokens` 结尾，
请求内最小、最大值由 `min` / `max` 中间段区分。

监控迁移配置在 `tools/monitoring/metric_migration.json`；
`python3 tools/monitoring/migrate_metrics.py --input dashboard.json --output dashboard-migrated.json`
读取 Grafana 导出的 JSON，保留现有指标名称与已正确的查询，仅修正失效标签筛选、
累计 Counter 查询与展示口径，并补充缺失面板和独立告警规则；不会写入线上 Grafana。
在对应版本代码发布时使用生成的配置；保留导出原件以便整体回退。

### Prefill 准入占用监控

`NON_BATCH_V2` 每轮采样完成后间隔 20 秒，按已创建的 Prefill 队列上报以下 GAUGE，`endpoint` 标识逻辑端点，
Master 身份由公共监控标签区分：

- `schedule_v2.prefill.inflight`：引擎活跃请求与 Master 本地在途名额按请求 ID 去重后的数量，
  与准入判断共用同一快照计数逻辑。已离开 Prefill 子队列、正在等待 Decode 的请求计入。
- `schedule_v2.prefill.inflight_limit`：当前 `dispatcher.maxInflightPerPrefillWorker`。

占用量保留 endpoint，队列移除后的下一次采样补零；上限每个 Master 上报一份。使用率按 Master 身份将各 endpoint 占用数匹配到共享上限计算，
仅使用上限大于零的序列。采样和监控聚合可能平滑瞬时占用；阻塞频率结合
`PREFILL_INFLIGHT_FULL` 原因指标判断。

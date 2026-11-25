# 配置参考

## 配置边界

FlexLB 保留两个职责独立的配置文档：

- `FLEXLB_CONFIG`：唯一的 FlexLB 行为配置，包含调度、分发、路由、worker registry、
  observability、服务发现运行参数、cache matching、Optimizer 和一致性/主选举。
- `MODEL_SERVICE_CONFIG`：只保存模型、endpoint、KVCM、Optimizer 的地址和服务发现定位信息。

`MODEL_SERVICE_CONFIG` 不属于动态 `FlexlbConfig` 快照，也不会被 UniConfig 或 Nacos
的动态更新覆盖。UniConfig 开关、Nacos 连接、部署标识、日志路径、Spring/OTEL 等
基础设施环境变量仍各自独立；它们不构成
FlexLB 行为配置的别名。

Spring 按标准规则读取环境变量，例如 `SERVER_PORT` 对应 `server.port`，
`FLEXLB_MONITOR_PROVIDER` 对应 `flexlb.monitor.provider`。内源部署通过
`FLEXLB_MONITOR_PROVIDER=kmonitor` 启用 KMonitor 指标上报。

容器启动脚本以物理内存和 cgroup 限额的较小值计算 JVM 内存预算；至少 16GiB 的容器
保留 2GiB direct memory 上限，小规格继续按比例分配。默认堆同时受 direct memory、
metaspace、code cache 以及容器内存的 1/8 native 余量约束。显式堆覆盖也校验初始堆不超过最大堆，
且上述预算总和不超过限额；超配时启动脚本报错。该校验约束 JVM 内存池配置，线程栈和
native allocation 的实际占用仍取决于负载。

显式堆大小必须携带 `k/m/g` 单位，例如 `2048m` 或 `2g`；不接受无单位数值，避免将业务希望
配置的 MB 数按 JVM 的字节语义使用。推导默认 GC 线程数时优先采用有效的正整数 CPU 配额；无效或缺失时
读取系统 CPU 数，仍不可用时使用 1，确保 G1 的并行 GC 线程数至少为 1。

仅在 JDK 21+ 且最终 Xmx 为 48GiB 或 64GiB 时启用大堆 G1 调优（等价的 `m/k` 单位也支持）。
其他堆大小保留原有年轻代 20% 下限、GC 线程数和日志配置。所有规格使用 32MiB region
和初始 IHOP 40，IHOP 后续自适应调整。
大堆配置使用默认 5% 年轻代下限，设置 `G1ReservePercent=20` 为对象转移保留堆内余量，
并启用 `AlwaysPreTouch` 预触初始堆，以启动耗时和更早提交物理内存换取运行期间更少的堆缺页。
大堆并发 GC 线程数按 JDK 21 的 `(ParallelGCThreads + 2) / 4` 取整，至少为 1：
24 核使用 18/5 个并行/并发线程，32 核使用 23/6；原有规格分别保持 18/4、23/5。
使用 `FLEXLB_JVM_HEAP_SIZE=48g` 或 `64g` 可同时设置 Xms/Xmx，仍需通过容器预算校验。
大堆 `gc.log` 同时记录 GC 和 safepoint 时间，按 20MiB 轮转并保留 5 个归档文件。

以下环境变量在 JVM 启动时优先覆盖对应的 G1 参数，值使用十进制整数。未设置或留空时使用默认行为。

| 环境变量 | JVM 参数 | 默认行为 |
|---|---|---|
| `FLEXLB_JVM_G1_MAX_NEW_SIZE_PERCENT` | `G1MaxNewSizePercent` | 由 JVM 决定年轻代上限 |
| `FLEXLB_JVM_MAX_GC_PAUSE_MILLIS` | `MaxGCPauseMillis` | 80 ms 暂停软目标 |
| `FLEXLB_JVM_PARALLEL_GC_THREADS` | `ParallelGCThreads` | 按容器可见 CPU 数推导 |
| `FLEXLB_JVM_CONC_GC_THREADS` | `ConcGCThreads` | 按实际使用的并行 GC 线程数推导，保留上述大堆取整规则 |

KVCM 的缓存和集群信息查询使用独立的 gRPC 客户端 I/O 线程组，不与 `GetWorkerStatus` 等引擎 RPC
共用；Master 转发仍使用引擎客户端的 I/O 线程组。两个客户端 I/O 线程组、客户端共享回调线程池和
服务端 I/O 工作线程组的线程数，各自在启动时取 JVM 的 `Runtime.getRuntime().availableProcessors()`，
不属于 `FLEXLB_CONFIG`。

## FlexlbConfig 加载与动态更新

`ConfigService` 是统一读取入口。`ConfigSourceSelection` 在启动时根据 FlexLB 进程的
环境变量选择唯一的行为配置字符串来源，先判断 UniConfig，再判断 Nacos：

| 条件（按顺序判断） | 字符串来源 | 动态更新方式 |
|---|---|---|
| `FLEXLB_UNICONF_ENABLE=true` | `UniConfigConfigSource` | 轮询本机 Turbo UniConfig HTTP 接口 |
| 否则，`FLEXLB_NACOS_SERVER_ADDR` 非空 | `NacosConfigSource` | Nacos listener |
| 否则 | `EnvironmentConfigSource` | 启动时读取 `FLEXLB_CONFIG` |

`true` 忽略大小写和两端空白。开启 UniConfig 后，即使配置了 Nacos 地址，也不会
创建 Nacos 客户端或 listener。使用外部来源时，环境变量中的 `FLEXLB_CONFIG` 不参与
校验或合并；初始文档中省略的行为字段使用对应格式解析器和 `FlexlbConfig` 的默认值。
`EnvironmentConfigSource` 仍负责读取独立的 `MODEL_SERVICE_CONFIG` 启动拓扑。
来源选择和连接参数在启动时确定，修改它们需要重启进程。

三种来源都返回原始字符串，复用 `ConfigDocumentParserResolver` 和 v0/v3 解析器：
优先使用文档内的 `schemaVersion`，否则使用 `FLEXLB_CONFIG_SCHEMA_VERSION`（默认 `0`）。
UniConfig 和 Nacos 的内容直接使用同一种配置 JSON，不增加包装层。

`FlexlbConfigMerger` 按以下规则合并更新：

- **只改一个字段：** 对象逐字段合并，数组和标量整体替换。从外部配置删掉字段，本机仍保留当前值。
- **切换模式：** `consistency`、`scheduler.ordering`、`cacheMatching` 的 `type` 改变时，通常替换整个分支。例外是 `KVCM ↔ LOCAL_STANDBY`：只改 `type` 会保留超时、健康阈值和 standby 参数。`scheduler` 和 `dispatcher` 自身的 `type` 按普通字段合并。
- **空更新：** `{"schemaVersion":3}` 不改变当前配置；`{}` 没有指定版本，默认按 v0 解析。

每次合并后都使用与 `FLEXLB_CONFIG` 相同的严格解析和跨字段校验：

- 仅忽略兼容名单中已删除的字段；拒绝其他未知字段，校验保留字段的 `null`、类型和取值范围；
- 拒绝重复 key、数值枚举和尾随 JSON；
- 拒绝违反 scheduler / dispatcher / router 等组合约束的配置。

选定的外部来源初次读取或校验失败会阻止应用启动，不会自动切换到低优先级来源。
运行时读取或校验失败，继续使用上一份有效配置；下一份配置校验通过后，才整体替换 `FlexlbConfig` 并通知监听器。

`scheduler.type` 仅支持 `QUEUE`，`dispatcher.type` 仅支持 `NON_BATCH_V2`。
显式配置 `NON_BATCH` 或 `BATCH` 会报错；已删除的 `scheduler.decision` 和 `scheduler.scanBudgetMultiplier` 被忽略。
启动时配置错误会阻止启动，
非法热更新会保留上次有效快照。主队列和 Decode 消费线程数仍在启动时确定。
当前 `NON_BATCH_V2` Decode 消费模型按单一流量 group 使用，不提供跨 group 的消费者隔离或公平调度保证。

`router.roles.decode.availability.maxEngineRequests` 和 `maxKvUsagePercent` 随配置快照更新。
新请求进入 Master 时保存当前配置，Decode 选路、排队重试和派发都沿用该请求的快照。
请求数上限、KV 百分比预算和请求 KV 需求保持到预留与派发；已派发的请求继续执行。
环境变量来源只在启动时读取配置。

`dispatcher.maxInflightPerPrefillWorker` 不用于淘汰准备阶段的候选。NON_BATCH_V2 在 Prefill 子队列
出队前读取当前配置，检查并占用在途名额。
`dispatcher.prefillSubqueueLimitMultiplier` 默认 4；当前路由形态的 Prefill 子队列共享
`min(scheduler.maxQueuedRequests, 当前形态的逻辑端点数 × maxInflightPerPrefillWorker × 倍数)` 个待交付名额。
PREFILL 与 PDFUSION 的端点和待交付名额不合并计算。
共享名额满时，已选 Prefill 的请求由主队列消费者持有，不在有序队列中，但仍计入全局排队数。
消费者按 `dispatcher.sharedSlotRetryMs` 等待后重试申请名额，默认 5 ms，取值为正整数，支持 Nacos 热更新。
如果发现所选 Prefill 已摘流，请求退回主队列重选。
`scheduler.maxQueuedRequests` 限制尚未发布的 Generation 请求总数，包括准备中、消费者持有、
等待 Prefill 或 Decode 的请求。调低上限不会撤销已有占位；排队数降到新上限以下后才有空位可用于准入。

旧的字段级行为变量 `BLOCK_HASH_STRATEGY`、`FLEXLB_LOG_LEVEL`、
`ENABLE_STDOUT_LOG`、`ENABLE_FALLBACK` 不再覆盖 JSON。行为配置只认
所选来源的 `FLEXLB_CONFIG` 文档，避免嵌套字段到环境变量名的隐式转换。

### UniConfig 连接

Spectrum 部署的扩展配置中启用 Turbo UniConfig：

```json
{
  "turbo": {
    "env": {
      "UNICONF_ENABLE": "true"
    }
  }
}
```

同时需要在部署的 worker 环境变量中显式配置，让 FlexLB Java 进程读取到：

```text
FLEXLB_UNICONF_ENABLE=true
```

两个开关分别控制不同进程，启用 UniConfig 时需要同时配置：Turbo 的
`turbo.env.UNICONF_ENABLE=true` 开启本机配置服务；Java 的
`FLEXLB_UNICONF_ENABLE=true` 选择 UniConfig 配置来源。Java 只读取带 `FLEXLB_` 前缀的
开关，未设置或不为 `true` 时继续按 Nacos 地址、环境变量的顺序选择来源。
部署标识沿用 `DeploymentIdentity`，要求设置 `SPECTRUM_WORKSPACE_ID`、
`SPECTRUM_APPLICATION_NAME` 和 `SPECTRUM_DEPLOYMENT_NAME`。

`UniConfigConfigSource` 读取以下部署级 key 的 HTTP 响应正文：

```text
GET http://127.0.0.1:18080/v2/configs/modelstudio.spectrum.deployment.<workspace>.<deployment>.runtime.meta
```

正文就是完整的配置 JSON 字符串，与 Nacos 文档格式一致。连接和读取超时各为 3 秒。
启动时立即读取初始配置，后续每次请求完成后等待 30 秒再轮询，仅在正文变化时发布更新。
Turbo 侧通常有约 1 分钟缓存，采用 30 秒轮询可兼顾本机请求频率和变化发现延迟；缓存刷新后，
Java 还可能等待接近 30 秒才发现新内容。因此两段等待叠加时，生效延迟可能接近 90 秒，
另加请求耗时，不能将轮询间隔理解为端到端生效时间保证。
启动阶段读取配置失败时，每隔 1 秒持续重试，没有次数上限；连接失败、非 HTTP 200
（包括 key 不存在时的 404）和非法 JSON 都会触发重试。文档既没有 `consistency`，
也没有 `flexlbSyncConsistencyConfig` 时同样重试。
线程被中断后停止等待并启动失败。文档读取成功后，业务配置校验失败仍会阻止启动。
启动前应先在部署 UniConfig 页面保存合法配置。
运行时 HTTP 异常保留有效快照并继续按 30 秒间隔重试。

### Decode 抢占交付方式

允许 `DECODE_ENGINE_OWNED` 抢占时，V2 将待抢占请求的字符串 ID 随 Decode 路由返回，
由调用方处理；FlexLB 不在选路链路发送 Cancel RPC。
旧部署配置可保留 `scheduler.ordering.preemption.engineCancellation` 和 `scheduler.ordering.preemption.timeoutMs`，这两个字段会被忽略。

### Prefill 缓存工作量权重

`localWeight` 和 `remoteWeight` 用来估计**新请求自身**能少算多少 token，不用于计算缓存亲和命中率，也不改变节点已有负载。抵扣量为 `本节点本地命中 × localWeight + 远端增量 × remoteWeight`，至少留一个块的输入用于计算（不足一块则保留全部输入）。KVCM 的远端增量取本次响应中最大的 global 命中减去本节点本地命中；其余来源取本节点自己的远端增量。单位和算例见[缓存命中率与节点负载](../../design/routing/03-cache-matching.md#选-p-的两个维度请求的缓存命中率与节点的已有负载)。

### Nacos 连接

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `FLEXLB_NACOS_SERVER_ADDR` | 无 | 仅在 UniConfig 未开启且地址非空时启用 Nacos |
| `FLEXLB_NACOS_DATA_ID` | 部署标识 | 显式 DataId |
| `FLEXLB_NACOS_GROUP` | `DEFAULT_GROUP` | Nacos group |
| `FLEXLB_NACOS_NAMESPACE` | 空 | Nacos namespace |

未显式配置 DataId 时，部署标识优先使用
`SPECTRUM_WORKSPACE_ID`、`SPECTRUM_APPLICATION_NAME`、
`SPECTRUM_DEPLOYMENT_NAME` 组成
`spectrum:<workspace>:<application>:<deployment>`。Spectrum 三元组不完整时，
要求 `BIZ_NAME`、`DEPLOYMENT_NAME`、`ZONE_NAME` 全部非空，依次以冒号拼接为
`<bizName>:<deploymentName>:<zoneName>`，例如 `dash_pd:ea118_RTX_PRO_5000_72GB:master`。
变量值会去除首尾空白。两组三元组均不完整时启动失败，错误信息列出六个字段及其值；
`null` 表示变量未设置或只有空白。使用第二组三元组时，部署身份不属于 Spectrum，不能用于 UniConfig。
部署标识同时用于 ZooKeeper 选举和主节点变更通知；
显式设置 `FLEXLB_NACOS_DATA_ID` 只覆盖 Nacos DataId。

选中 Nacos 配置来源时，启动阶段必须读取到非空配置正文。配置不存在、正文为空或只有空白、
读取异常均导致启动失败。配置缺失或空白的错误包含 DataId、group 和 namespace；读取异常保留原始原因。
启动过程不回退其他 DataId、环境变量配置或默认配置。

UniConfig / Nacos 的 v3 部分更新示例：

```json
{
  "schemaVersion": 3,
  "scheduler": {
    "mainQueueMaxAddAttempts": 4
  },
  "router": {
    "availabilityHysteresisPercent": 12
  },
  "observability": {
    "logging": {
      "level": "warn",
      "stdoutEnabled": true
    }
  }
}
```

## FLEXLB_CONFIG 结构

公共 schema 当前为 version 3，按责任分区：

- `scheduler`：`type` 取 `QUEUE`（配置 `DIRECT` 时校验失败）；QUEUE 拥有 ordering、capacity 和 lifecycle。
  `mainQueueMaxAddAttempts` 控制 `NON_BATCH_V2` 主队列一次入队的总尝试次数（含首次），默认 `3`，
  必须是正整数。最后一次申请名额失败后仍可抢占；发起抢占的请求不再重试，释放的名额可由其他请求使用。
  热更新后，新发起的入队操作使用新值，已开始的入队操作沿用原值。
- `dispatcher`：`NON_BATCH_V2`（默认且唯一模式）。可用
  `maxInflightPerEncoderWorker` 限制每台 Encoder 的在途请求数；未配置时不设额外上限，
  不影响未配置 Encoder 的模型。
- `router`：角色 availability、execution estimator、selector、cache affinity 和
  group selector。
- `workerRegistry`：worker health 与 cache-status 刷新策略。
  `health.statusPollFailureThreshold` 控制 WorkerStatus 连续探测失败的摘除次数，默认 `10`，必须是正整数。
  Nacos 热更新在下一次处理失败时生效，不清零已有计数；探测成功仍会清零。
  `health.statusStaleAfterMs` 独立生效，可能在达到失败次数阈值前摘除 Worker。
- `observability.cacheHit`：recent-key window、指标和理论命中日志。理论命中查询、历史池更新、
  池容量指标与请求日志由独立单线程按入队顺序处理；请求线程仅提交任务。
  理论命中 Tokens 保存在请求的异步结果中，随 worker status 反馈统一上报。
  后台任务直接读取路由完成后保持不变的请求字段；日志与指标开关在执行时从配置服务读取。
  等待队列最多容纳 100,000 个任务，满时丢弃统计样本并汇总告警，不阻塞请求线程。
  `feedbackMaximumEntries` 默认 `100000`，必须为正整数，限制尚未收到 FINISHED 的预测关联数量。
  Nacos 更新该值会直接调整现有关联表的容量，保留表内记录；调小容量时超出的记录可能被淘汰，
  被淘汰请求后续的反馈无法关联，因此应按未完成请求数预留余量。例如：

  ```json
  {"observability":{"cacheHit":{"feedbackMaximumEntries":200000}}}
  ```

  `feedbackTimeoutMs` 默认 `60000`（1 分钟），必须为正整数，独立于 WorkerStatus 的超时参数。
  它从关联登记或最近一次包含该请求的状态反馈开始计算；PENDING、RECEIVED、KV_ALLOCATED、RUNNING
  均会续期，其他请求的状态不会续期。超过该时长没有匹配反馈时关联过期，后台回收；FINISHED 立即条件删除。
  预测在选路阶段登记，等待首次反馈的排队时间也包含在内，排队较长的部署应相应调大该值。
  Nacos 热更新直接调整现有关联的过期时长，按最近一次访问时间计算，不重建关联表；
  调小时已超过新时长的记录会过期。

  `recentKeyWindow.maxKeyOccurrences` 默认 `1000000`，限制保留的唯一 key 数量；
  `durationMs` 默认 `1800000`。实际容量取配置上限与 `capacityMultiplier × Prefill 数量 × (单台总 KV Token / blockSize)`
  的较小值，`capacityMultiplier` 默认 `10`。Prefill 的最大 KV Cache 和 blockSize 相同，只读取第一台的容量；
  该台上报容量后，后台线程才分配历史池；容量尚未产生时不记录样本。历史池使用固定数组，不存储 KV 内容；
  默认上限对应约 49 MiB JVM 堆，容量估算较小时实际占用更低。
  `durationMs`、`maxKeyOccurrences` 与 `capacityMultiplier` 通过配置更新热生效：任一值变化后，
  后台线程在下一次统计时按新值创建新历史池并释放旧池，累计历史清空，理论命中率随后逐步恢复；
  三个值都不变时保持原池。重建时 Prefill 容量尚未可得则继续使用旧池。
  池容量按创建时的 Prefill 数量确定，Prefill 扩缩容不会触发重建。
  理论命中监控使用 `app.cache.theory.hit.count`、`app.cache.theory.total.count` 与
  `app.cache.theory.hit.ratio`。前两个指标是 Counter，每个请求分别上报理论命中 Tokens 和
  输入 Tokens；命中率是该请求的理论命中 Tokens 与输入 Tokens 的比值。历史记录有效期只影响
  本次请求是否匹配，不影响已经上报的 Counter。
- `observability.logging`：FlexLB logger group 级别与 root/PV stdout 开关。
- `serviceDiscovery`：connect/read timeout、poll interval 与连接池运行参数。
  持续空结果保留已有地址，不按空结果次数或持续时间撤销 worker。
- `cacheMatching`：默认 `KVCM`，要求 `MODEL_SERVICE_CONFIG.kvcm` 提供定位信息。
  支持 `LOCAL_SYNC` / `KVCM` / `LOCAL_STANDBY`；KVCM 和 LOCAL_STANDBY 共用查询、健康、远端命中
  （`medium` / `topKHostCount` / `backendTypes`）和 Local Standby 参数。
  使用 Worker 本地同步缓存时显式设置 `cacheMatching.type=LOCAL_SYNC`。
  仅按本机成功调度的历史匹配时，设置 `cacheMatching.type=LOCAL_STANDBY` 即可，可省略 `MODEL_SERVICE_CONFIG.kvcm`。
  Java 配置接口通过 `getType()` 提供模式；`isKvcmAndStandbyEnabled()` 对 KVCM 和 LOCAL_STANDBY
  返回 true，`getKvcmAndStandbyConfig()` 提供这两个模式共用的参数。
- `optimizer`：启用开关和服务发现轮询间隔。
- `consistency`：`NONE` / `ZOOKEEPER` tagged union；ZooKeeper 分支拥有连接和 master
  刷新参数。
- `enableFallback`：默认 `false`；每次 Schedule 在入口读取当前快照，启用时在构建调度上下文、
  校验 batch 载荷、主从判断和转发之前返回 `success=false`、`code=8600`、`enqueued_by_master=false`，
  不返回选路或 lifecycle，由客户端执行 fallback。适用于 Encoder 和 Generation。
  UniConfig/Nacos 热更新生效后，新请求使用新值；关闭时恢复正常调度，已进入调度的请求继续原流程。
  热更新关闭必须显式设置 `enableFallback=false`，删除字段会保留当前值。
- `fallbackBatchTokenCapacity`：默认 `1048576`；Engine 未声明
  `max_batch_tokens_size` 和 `max_seq_len` 时使用的最终 batch token 容量兜底值。
  优先使用 Engine 上报的 `max_batch_tokens_size`，其次使用 `max_seq_len`。
- `internalRuntime`：代码内部设置，不接受公共 JSON 输入。

可选配置应省略，不能写 `null`。完整示例和 selector
矩阵见根目录 [README](../../../README.md)。

### KVCM 查询参数热更新

KVCM 模式下，Nacos v3 配置可设置并热更新查询参数：

```json
{
  "schemaVersion": 3,
  "cacheMatching": {
    "type": "KVCM",
    "queryTimeoutMs": 100,
    "topKHostCount": 3,
    "queryChannelsPerLeader": 8,
    "backendTypes": []
  }
}
```

`queryTimeoutMs` 是 `GetHostCacheState` 的总超时预算（含重试），必须为正整数，默认 100 ms；
超时后，本次请求走本地 Standby。`requestTimeoutMs` 仅用于 Leader 探测的 `GetClusterInfo`，默认仍为 500 ms。
升级时，如果要保留原先通过 `requestTimeoutMs` 设置的缓存查询超时，请把该值显式填到 `queryTimeoutMs`。

`queryType` 接受 KVCM 协议中 `QueryType` 的枚举名称字符串，配置后覆盖所有 role/group 的
worker 状态推导值。启动时省略该字段则继续从 worker 状态推导。
`topKHostCount` 为非负整数，默认 3；0 只计算本地命中。
`queryChannelsPerLeader` 为正整数，默认 8，限制对当前 KVCM leader 的查询连接数。Nacos 更新后，
新查询从新容量内随机选槽，超出容量的连接退出池，已发出的 RPC 可以完成。
`leaderRefreshIntervalMs` 默认 30000 毫秒，启动时读取。
`backendTypes` 默认空列表，只接受 `ST_TAIRMEMPOOL` 和 `ST_EVENT_REPORT_L2`，
例如 `["ST_TAIRMEMPOOL", "ST_EVENT_REPORT_L2"]`。空列表原样发送给 KVCM。
运行时省略字段保留当前值；显式配置 `[]` 清空来源列表。非法值或 JSON `null`
会拒绝整次更新，保留上一份有效配置。旧 `globalKvsHostCount` / `enableP2p` 不再接受。

Nacos 监听更新通过 `ConfigService` 校验并发布到 `CacheMatchConfiguration`；
下一次查询使用新配置，同一次查询的重试继续使用原配置快照和原绝对 Deadline。
查询线程只读取内存快照，不访问 Nacos。

启动时已配置 `MODEL_SERVICE_CONFIG.kvcm` 的实例支持 `KVCM` 与 `LOCAL_STANDBY` 双向热切换，切换时保留未指定的参数。
以下片段合并到现有配置后固定使用本地 Standby：

```json
{
  "schemaVersion": 3,
  "cacheMatching": {"type": "LOCAL_STANDBY"}
}
```

将 type 设置为 `KVCM` 恢复 KVCM 优先策略，并遵循现有健康检查和 `localStandby.autoSwitch`。
显式选择 `LOCAL_STANDBY` 时，请求只查本地。配置了 KVCM 定位信息时仍会后台探测 KVCM；省略时不启动探测。
配置到达各副本后立即更新选源，已发起的缓存查询继续完成。`configuredMode` 返回配置模式，
`effectiveSource` 返回实际选源。与 `LOCAL_SYNC` 之间切换需要重启。
环境变量只在启动时读取；运行时切换通过 Nacos 或 UniConfig 配置来源下发。

## MODEL_SERVICE_CONFIG

每个 FlexLB 进程只服务一个模型，独立反序列化为唯一的 `ServiceRoute`。
`ModelMetaConfig` 保存该路由及所需角色，按 role 读取各 group 的 endpoint；
无需配置模型标识，旧配置中的 `service_id` 会被忽略。
配置包含：

- `role_endpoints`；
- endpoint 的 `address`、`protocol`、`path`、`worker_status_port`、`discovery`；
- discovery 定位字段：类型 `static-env`、`vipserver`、`dashscope`，以及可选
  `base_url` / `hosts`；
- 可选 KVCM 定位字段 `address`、`namespace`、`port`、`discovery`；
- 可选 Optimizer 定位字段 `address`、`port`、`path`、`discovery`。

旧的 `load_balance`、KVCM/Optimizer `enabled`、discovery timeout/poll、KVCM 健康与
Local Standby 等行为字段会被拒绝，并提示迁移到 `FLEXLB_CONFIG`。该拓扑在相关 Spring
Bean 创建时使用；动态 `FLEXLB_CONFIG` 更新不会改变模型拓扑。服务发现 provider 仍通过
`DiscoveryConfig` 的运行参数 getter 读取当前 `FlexlbConfig.serviceDiscovery`，因此不需要
把运行参数复制回 topology JSON。

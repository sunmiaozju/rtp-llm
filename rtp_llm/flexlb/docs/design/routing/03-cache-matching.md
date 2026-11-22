# Prefill 缓存匹配

选 Prefill（P）时，FlexLB 要估计：**同一段输入发给 P1 或 P2，各能复用多长的 KV 前缀？** 三种模式对“能复用”的判断依据不同：`LOCAL_SYNC` 看引擎最近一次报告有哪些块；`KVCM` 问外部缓存管理器，每台 P 本地有多少、连同远端又能拿到多少；`LOCAL_STANDBY` 只看这台 FlexLB 以前把哪些块发往哪台 P。三者都只提供选路前的预测，实际命中要等引擎处理请求后才知道。

下面始终用同一个例子：请求有 70 个 token，每块 16 个 token，客户端传来四个有序的 `block_cache_keys`：`[A, B, C, D]`。末尾 6 个 token 不构成完整块。客户端按目标引擎的哈希和 lookahead 规则生成 keys；非空 keys 必须带正数 `cache_key_block_size`。FlexLB 不从 token 重新算哈希。没有 keys 时按零命中继续选路，不必查询 KVCM。

假设 P1、P2 都是目标流量 group 中已注册、可路由的 `PREFILL`（合一部署则是 `PDFUSION`，沿用 P 的策略）。即使 P1 正在排队，也要参与匹配。

## 选 P 的两个维度：请求的缓存命中率与节点的已有负载

**缓存命中率看这条输入。** 选路比较的是每台 P 对当前请求的预测有效命中率，不是节点历史请求的平均命中率。以 70 个输入 token 为例：若 P1 本地命中 2 块，连同远端共命中 3 块，原始本地命中率是 `32/70`；假设远端按一半折算，有效命中率是 `(32 + 16 × 0.5) / 70 ≈ 57%`。远端只计多出来的那一块。若折算后覆盖整个输入，则至少留一块不抵扣；该节点没有记录时，它自身的有效命中率为零，仍是候选。

**已有负载看这台 P 手上还有多少工作，以毫秒计。** 调度器累计 P 队列里等待的工作和已出队在途请求的预计剩余工作。同一请求从等待转为在途只算一次，引擎反馈可修正剩余量；不是简单数请求，也不是看空闲 KV。若有无法估时的引擎任务，预计耗时记为未知，不按空载处理。

最终比较的是 `预计首 token 时间（TTFT）= 节点已有工作时间 + 新请求自身计算时间`。例如 P1 已有 30 ms 工作，新请求因命中较多只需 8 ms；P2 已有 5 ms，新请求需 20 ms。P1 命中更多，P2 却预计更快（25 ms 对 38 ms）。新请求的计算时间有独立的本地、远端抵扣权重：KVCM 取整次查询的最大 global 命中估算可用远端工作量，因此某台 P 的自身命中率为零，不等于耗时模型的远端抵扣一定为零。多等多久值得换取更高命中率，见 [Generation 选路决策](02-generation-routing-decisions.md)。

## LOCAL_SYNC：查引擎最近上报的块

在这种模式下，FlexLB 把每台 P 的缓存块清单同步到本机，请求到来时直接查内存，不必临时向每台 P 发 RPC。为此要维护两种方向的索引：`EngineLocalView` 按节点保存上次的块集合，方便处理更新；`GlobalCacheIndex` 按块保存拥有哪些副本的节点，方便一次查询多台 P。这里的 `Global` 是指这张表覆盖多个节点，不代表某台 P 能读取远端 KV。

1. **同步。** `GetCacheStatus` 带上该实例已成功写入索引的版本；尚未建好索引时发 `-1`。收到新的块集合后，与该节点旧集合做差，只把新增和移除的映射更新到索引。节点下线后清理映射；同地址重新注册，旧轮询回调不能给新实例写索引。
2. **匹配。** 假设 P1 最近上报 `{A, B, D}`，P2 上报 `{A}`。从 A 开始，两台都命中；到 B，只有 P1 继续；到 C，P1 也停止。虽然 D 在 P1 上，缺了 C 就不能把 D 接到可复用的前缀后面。
3. **返回。** P1 命中 2 块，即 32 个 token；P2 命中 1 块，即 16 个 token。该模式只报告本地命中，因此返回的 `(local, global)` 分别是 `(2, 2)` 和 `(1, 1)`，单位是块。

这个设计用后台同步换掉了请求路径上的网络等待，代价是两轮同步之间看不到引擎变化。例如 P1 刚淘汰 B 时，查询仍可能给它算 2 块。FlexLB 根据最近同步中块集合的变化量调整轮询间隔：变化大就更勤地拉取，变化小就放慢。状态响应到达与索引写入成功是两件事；后者失败时不推进索引版本，以便下轮重试。

源码：[缓存轮询与版本检查](../../../flexlb-sync/src/main/java/org/flexlb/sync/runner/GrpcCacheStatusCheckRunner.java)、[两张索引的更新](../../../flexlb-cache/src/main/java/org/flexlb/cache/match/localsync/KvCacheManager.java)、[连续前缀匹配](../../../flexlb-cache/src/main/java/org/flexlb/cache/match/localsync/GlobalCacheIndex.java)。

## KVCM：区分本地已有和远端可取的前缀

FlexLB 将同一组 `[A, B, C, D]` 发给 KVCM leader 的 `GetHostCacheState`。查询必须落到正确的引擎实例：优先用配置的 namespace，否则取对应 worker 的 `deploymentName`，再拼上块大小，例如 `model_a_16`。显式配置的 `queryType` 优先；否则根据 worker 上报的布局选择普通前缀、Mamba 或滑窗注意力（SWA）匹配。无法确定必要元数据是查询失败，不是引擎报告了零命中。

假设返回以下两条记录。`local` 是该节点本地已有的连续前缀块数；`global` 是结合本地、其他节点可提供的 KV（P2P）和配置允许的远端存储后，可复用的前缀总长。**`global` 已经包含 `local`。**

| 候选 | KVCM 返回（块） | 对这条 70-token 输入意味着什么 |
|---|---|---|
| P1 | `local=2, global=3` | 本地有前 32 个 token，加上远端可到 48；远端多提供 16 个。 |
| P2 | `local=1, global=4` | 本地只有前 16 个 token，加上远端可到 64；不能把 16 和 64 相加。 |

P2 的总前缀更长，但其中 48 个 token 依赖远端。FlexLB 保留两个数给评分逻辑，而不是把拉取 KV 当成本地显存命中。`topKHostCount` 控制 KVCM 为本地命中靠前的多少台主机计算联合命中，**不是候选数或返回地址数的上限**；设为 0 时只算本地。介质、后端类型和查询参数见[配置参考](../../architecture/reference/01-configuration.md)。

**响应中的主机不自动成为候选。** FlexLB 按已注册候选的逻辑地址 `ip:httpPort@engineIndex` 查响应。仅当来源是 KVCM、一个前端只有一个逻辑引擎且没有逻辑地址记录时，才兼容旧的 `ip:httpPort` 键。多引擎不能这样回退，否则会把同一前端下两个引擎的缓存算到一起。跨机 TP 同理：非零 rank 可以出现在 KVCM 的缓存记录里，Schedule 仍只能返回服务发现暴露的 [TP rank 0 入口](../../architecture/workers/01-discovery-and-registration.md#跨机-tp只有-tp-rank-0-是调度入口)。

**KVCM 返回零命中时用零；查询失败时，这一条请求才改查本机 `LOCAL_STANDBY`。** 后续请求是否也直接查 standby，由本副本的健康判断和配置决定。阈值、恢复条件，以及 FlexLB 同时换主时会用哪份索引，见[容灾与降级](../runtime/03-failover-and-degradation.md)。

源码：[查询参数、重试与截止时间](../../../flexlb-grpc/src/main/java/org/flexlb/engine/grpc/client/KvcmGrpcClient.java)、[namespace 和缓存布局](../../../flexlb-grpc/src/main/java/org/flexlb/engine/grpc/client/KvcmWorkerMetadataResolver.java)、[按候选地址读取结果](../../../flexlb-cache/src/main/java/org/flexlb/cache/domain/CacheMatchResult.java)。

## LOCAL_STANDBY：用本机路由历史估计命中

它和 `LOCAL_SYNC` 都在内存里匹配，但**一条索引记录的含义不同**。在 `LOCAL_SYNC` 中，`B → P1` 表示“P1 最近上报它有 B”；在 standby 中，只表示“这台 FlexLB 最近把含 B 的请求路由到了 P1”。因此即使 KVCM 一直健康，FlexLB 也要持续维护 standby；等故障发生才开始记录，第一条兜底请求就没有历史可查。

1. **写入路由结果。** 本机成功选出 `PREFILL` 或 `PDFUSION` 后，异步记录请求的每个块、选中的逻辑地址和更新时间，不等待引擎执行。写入排队失败不影响已成功的调度。只转发给 master 的 follower 不写；它在本地降级调度成功才写自己的索引。副本之间不复制这份历史。
2. **读取还有效的前缀。** 假设历史中 P1 曾接过 `[A, B, C]`，P2 曾接过 `[A]`，记录都未过期。现在查询 `[A, B, C, D]`，先得到 3 块和 1 块。若 P1 上报 `cacheMatchRollbackBlocks=1`，P2 为 0，分别扣除引擎需要重算的尾块，最终返回 `(2, 2)` 和 `(1, 1)`；最多扣到零。这里没有远端增量。
3. **让旧记录失效。** 引擎可能根本没执行那条请求，也可能已经淘汰 B；反过来，另一台 FlexLB 发来的请求可能已在 P1 上留下了本机不知道的块。standby 用 TTL 限制旧预测保留的时间，不能用 TTL 证明引擎仍有该块。容量按存活 worker 的显存块数估算，并受配置上限约束；用量升高时缩短 TTL，到达上限拒绝新映射，已有映射仍可刷新。

显式配置 `LOCAL_STANDBY` 时，请求直接同步查本机索引，KVCM 恢复健康也不会把配置改回去。`KVCM` 和 `LOCAL_STANDBY` 共用已启动的组件，支持热切换；与 `LOCAL_SYNC` 互切则需重启。平时用 KVCM 选路，可以异步计算同一请求在 standby 下的预测，再用**实际选中的节点**和引擎反馈核对误差。一次成功查询得到零块是有效样本；查询失败不是零命中样本。对照如何关联请求、哪些反馈有效，见[缓存预测与引擎实际命中的对照](../../architecture/workers/02-status-and-feedback.md#缓存预测与引擎实际命中的对照)。

源码：[路由成功后的异步写入](../../../flexlb-cache/src/main/java/org/flexlb/cache/match/localstandby/LocalStandbyCacheMatchProvider.java)、[前缀匹配与回退块数](../../../flexlb-cache/src/main/java/org/flexlb/cache/match/localstandby/LocalStandbyCacheManager.java)、[索引过期与容量限制](../../../flexlb-cache/src/main/java/org/flexlb/cache/match/localstandby/LocalStandbyCacheIndex.java)。

# 角色路由：请求走哪条路径

FlexLB 按角色选择 worker。Generation 由 `RequestSchedulerV2` 分阶段调度；`DefaultRouter`
解析目标流量 group，并提供 Encoder 选点和 VIT 选点、校验入口，不串行选择所有角色。

主要代码：`flexlb-sync/src/main/java/org/flexlb/balance/` 下的 `scheduler/`、`schedulev2/` 和 `strategy/`。

ViT 与 Encoder 属于不同引擎的不同角色，不是一次请求中前后相接的两个阶段，也不会在同一部署中同时出现。

| 推理引擎 | 对应角色（部署该角色时） |
|---|---|
| RTP-LLM | ViT |
| SGLang、vLLM | Encoder |

## Schedule 入口

每套 FlexLB 部署只支持一个流量 group，不跨 group 回退。Worker 的 DP group 用于检查 rank 是否完整，
与流量 group 无关。

`Schedule` 接收 `schedule_roles`（Encoder、Prefill、Decode、PDFusion）。显式角色去重后按
`ModelMetaConfig.getRequiredRoles()` 返回的部署角色校验；未部署的角色或非法枚举值返回 `INVALID_REQUEST`。
未传角色时使用部署配置。EPD 客户端分别请求 Encoder 与 Generation，混合角色不在已验证链路内。
当前 V2 即使只请求 Decode，也仍须先选 Prefill，成功时返回 P/D 两个地址。
VIT 不属于 `schedule_roles`；VIT-only 请求走独立的 `VitCacheSelector` 路径。

## RTP-LLM：ViT 优先复用同一份媒体缓存

调用方设置 `vit_route_only=true`，并通过 `cache_affinity_keys` 提供要匹配的媒体标识。FlexLB 此时只返回 ViT 地址，不进入 Generation 队列。这里匹配的是单个媒体标识，不是 Prefill 的连续 KV 前缀：例如请求带两张图，节点只缓存了第二张图，仍可计一次命中。

1. **比较已知缓存。** 对去重后的 keys，先比较各候选命中的 hash 数；相同时比较有 embedding 的数量（CPU 和 GPU 合计），再比较其中位于 GPU 的数量。
2. **处理尚未上报的新 key。** 例如图片第一次被选到 V1，V1 还没来得及上报。FlexLB 会暂时记住这次选择；相同图片再次到达时，若前面三项都打平，优先选最近分到这个 key 的候选。临时记录不等于引擎已缓存；四项都相同才随机选。没有候选返回 `NO_VIT_WORKER`。
3. **后续完整调度校验原节点。** 调用方可在后续请求携带 `selected_vit`。建立新的调度记录前，FlexLB 核对角色、地址、group 和注册代际。同地址若已换成新实例，返回 `VIT_ROUTE_STALE`，不把旧选择悄悄绑定到新实例。

源码：[ViT 命中统计](../../../flexlb-sync/src/main/java/org/flexlb/service/VitCacheDirectory.java)、[比较与临时记录](../../../flexlb-sync/src/main/java/org/flexlb/service/VitCacheSelector.java)、[完整调度前的校验](../../../flexlb-sync/src/main/java/org/flexlb/service/RouteService.java)。

## SGLang / vLLM：Encoder 比较节点已有的编码工作

Encoder 请求进入独立队列。选点先看已发布、存活、属于目标 group 的 Encoder，再按 `引擎运行数 + 引擎等待数 + 本机已分配但尚未观察到的请求数` 估计并发。达到 `dispatcher.maxInflightPerEncoderWorker` 的节点不参加；暂时都满时，等待容量释放后重试。Encoder 不用 Prefill 的 KV 容量或命中率给节点排名。

假设两台节点都未满，E1 有 2 条请求、估计还有 800 个未命中编码 token，E2 有 3 条、估计共 300 个。**未提供 `encoder_cache_hit_len` 时选 E1**，因为请求更少；**提供了该字段时选 E2**，因为已有编码工作更少，工作量相同时才比较并发。显式传 `0` 也算提供了估计。这里比较的是每台节点手上的工作，不是去两台节点查新请求能命中多少。

在引擎上报前，本机按已分配请求的未命中 token 估计记账；收到活跃任务后，改用引擎上报的编码输入长度，同一请求不重复相加。选好后，队列在同一 `EncoderEndpoint` 上登记并返回地址，由调用方下发，不调用 `EnqueueBatch`。选点读取的是当时的负载，并发请求仍可能选到同一节点。

源码：[Encoder 候选与排序](../../../flexlb-sync/src/main/java/org/flexlb/balance/strategy/EncoderStrategy.java)、[节点上的工作量记账](../../../flexlb-sync/src/main/java/org/flexlb/balance/endpoint/EncoderEndpoint.java)。

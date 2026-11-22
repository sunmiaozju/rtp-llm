# 一次 Schedule 的生命周期

## 请求主链路

```
gRPC FlexlbService.Schedule (FlexlbGrpcServer, HTTP 端口 + 100)
    ↓ 若 enableFallback=true → 直接返回业务码 8600，由客户端选路
    ↓ 构建请求上下文，校验 block_cache_keys 与 cache_key_block_size
    ↓ 若启用一致性且本机非 master → 尝试转发；符合降级条件时才走本地路由
RouteService.route()
    ├─ RTP-LLM 的 ViT 请求：查询缓存或选择 ViT 端点
    ├─ SGLang / vLLM 的 Encoder 请求：Encoder 队列选点并返回地址
    └─ Generation（生成阶段）：V2 主队列按 Prefill 策略选点
           ├─ P/D 分离的常规请求：进入 Prefill 队列，再进入 Decode 队列，一次返回 P/D 两个地址
           └─ PD 合一：PDFusion 节点按 P 的策略匹配缓存、比较负载并排队，返回一个合一地址
```

Schedule 入口读取当前配置快照；`enableFallback=true` 时，在构建调度上下文、主从判断和转发之前返回 `success=false`、`code=8600`、`enqueued_by_master=false`，不进入调度或派发。
开关关闭时将请求字段转换为 `BalanceContext` / `Request`，本地路由、排队和转发完成回调使用内部上下文。
原始 `FlexlbScheduleRequestPB` 仅用于入口解析和向 Master 发起 gRPC 转发；转发 RPC 回调使用提取出的 request ID。
哪些转发失败允许本地决策，见[选主与请求转发](runtime/01-master-and-forwarding.md)。

## 地址发布与引擎执行

Generation 发布地址即完成本次调度，调用方按返回地址下发。未发布时取消或超时，调度器归还排队名额，消费者归还本次未发布的预占；发布后 P/D 分别根据引擎上报或状态更新超时清理。见[队列流转](../design/scheduling/01-autotpm.md)和[容量与释放](../design/scheduling/02-capacity-and-release.md)。

## Generation 未能发布地址的三个出口

- **拒绝。** 例如请求了未部署的角色，在入队前即可返回 `INVALID_REQUEST`；主队列满且本请求未获名额时也会返回失败。两者都没有 P/D 地址。某轮尝试因 D 容量不足而继续等待，不是一次对调用方的拒绝。
- **取消。** 显式 `Cancel` 可以结束仍在等待的调度；若客户端先断开本地 Schedule RPC，取消监听器也会尝试结束这条调度。取消先于发布成功时，调度器清理已经取得的资源。调用方如何区分找到记录与真正取消成功，见 [Cancel 契约](reference/02-api-contracts.md#cancel取消尚未发布的调度)。
- **排队超时。** 请求进入 V2 Generation 调度表后，调度器按原截止时间设置定时任务；主队列重试或从 P 转去等 D 不重新计时。截止时间先到则以超时原因结束，返回该优先级对应的排队拒绝码，并走发布前清理。它与发布后等待引擎状态上报的超时是两个不同的时间窗口。

定位是“仍在某个阶段等待”还是“已经结束但引擎尚未反馈”，见[可观测性：排查入口](reference/03-observability.md#排查入口)。

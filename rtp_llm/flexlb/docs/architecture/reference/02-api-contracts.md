# 接口契约

## gRPC Schedule 响应

`FlexlbService.Schedule` 的 `FlexlbServerStatusPB` 使用 `optional int32 engine_index = 9`。
字段号 5、7 保持 reserved。N=1 不设置字段；N>1 设置所选 index，包含显式 0，调用方应通过
字段 presence 区分这两种情况。`FlexlbServiceImpl` 从内部 `ServerStatus` 转换时保留该语义；
Master 转发透传 protobuf 响应。物理 `server_ip/http_port/grpc_port` 和既有 service path 不变，
旧客户端可忽略新增字段。Decode 抢占时响应包含 `preempt_request_ids`，由调用方处理。

## 请求 ID 协议

Worker Status 的共享 PB 协议使用 `int64 TaskInfoPB.request_id = 1` 与
`string TaskInfoPB.request_id_str = 34`。RTP-LLM 用 `request_id` 上报 `int64` 请求 ID；
vLLM 和 SGLang 用 `request_id_str` 上报原始字符串 ID，数字字段保持默认值 0。
Java、C++ 和 Python 使用同一字段定义生成代码。`RequestId.parse` 优先读取非空白的
`request_id_str`；该字段为空时将 `request_id` 转为字符串。proto3 的 int64
默认值为 0，因此两个字段均未提供时解析得到字符串 `"0"`。
Running 和 Finished 任务表均以解析后的 ID 为键，缓存反馈与调度请求使用同一 ID 关联。
Master 的请求模型、调度、取消、状态查询、资源预留和 tracing 接口均只接收字符串 ID。
Schedule、Cancel、GetRequestState 请求 PB 均使用编号 1 的 int64 `request_id`，
并提供 `request_id_str` 传输字符串 ID。当前前端的 Schedule 和 Cancel 只填写 int64 字段；
Rust MasterClient 的 Schedule 和 Cancel 填写业务字符串字段，数字字段保持默认值 0。
Schedule 的 session-affinity ID、bucket ID、type 分别使用字段编号 22、23、24，
与编号 21 的字符串请求 ID 独立；Follower 转发保留这些字段。
FlexLB 收到请求后优先读取非空白的 `request_id_str`，否则读取 int64 字段。
生命周期记录保存 Schedule 请求中的 int64 字段值；字段未填写时该值为 0。生命周期响应
填写解析后的 `request_id_str` 和原始 int64 `request_id`。
例如请求带 `request_id=7, request_id_str="007"` 时，响应分别返回 `7` 和 `"007"`；
只带 `request_id_str="007"` 时，响应填写字符串字段，int64 字段保持默认值 0。
HTTP JSON 的数字 ID 在反序列化请求模型时转换成字符串。
Engine 的生成、取消、结果拉取和 KV 传输协议仍使用现有 int64 字段；
相关转换仅发生在对接这些 Engine RPC 的边界，不用于 Master 内部请求标识。
`GenerateInputPB`、`EnqueueBatchSuccessPB` 和 `EnqueueBatchErrorPB` 的 ID 仍是 int64，
对应的读取重载直接转换该字段；字段未设置时按 PB 默认值返回字符串 `"0"`。
Master 只返回逐请求路由；Generation 和 Encoder 的 Schedule 请求不需要 Engine 生成 payload。
对当前 Engine 的取消 RPC 单独要求规范 int64 字符串，拒绝 `"007"` 被归一化后误取消 ID `"7"`。

## Cancel：取消尚未发布的调度

`FlexlbService.Cancel` 处理的是 FlexLB 的调度记录，不是让引擎停止推理的 RPC。沿用 Schedule 的业务请求 ID；`role=ENCODER` 指向编码阶段，省略角色或指定 P、D、PDFusion 都指向 Generation 阶段。指定 D 不是“只释放 D”。`batch_id=0` 不限定批次，非零时必须匹配。响应 `found=true` 表示找到了匹配记录，`lifecycle` 给出处理后的状态；它不保证取消赢过了并发的地址发布。

| Generation 请求所在位置 | 调用 Cancel 的效果 |
|---|---|
| 等待主队列或在 P 队列里尚未出队 | 取消先成功则结束调度，清理等待项，归还已经取得的排队名额；尚未取得的节点资源无需释放。 |
| 已占 P、正在等 D | 取消先成功则不再发布地址，归还排队名额和 P 预占。D 若刚取得尚未交付的预占，由 D 消费线程撤销，不由 Cancel 调用线程直接扣计数。 |
| 地址已发布 | 取消不能撤回这次地址发布。Generation 调度记录清理后返回 `found=false`，不表示任务已经从引擎退出。后续 P/D 容量按各自的状态反馈或确认超时清理。 |

每个副本先尝试本地取消。未找到时，只有首次接到该 RPC 且需要转发的 follower 才向当前 master 转发一次。找不到 master 地址且尚未发出 RPC，返回本地的未找到结果；已尝试转发却拿不到可靠应答，则返回 RPC 错误，不把未知结果当作 `found=false`，也不再本地重做。

实现：[Cancel 入口与转发](../../../flexlb-api/src/main/java/org/flexlb/httpserver/FlexlbServiceImpl.java)、[Generation 请求表与取消](../../../flexlb-sync/src/main/java/org/flexlb/balance/schedulev2/RequestSchedulerV2.java)。

## HTTP 与端口

- 主服务：7001。
- 管理端口：7002。
- `/health` 与 lifecycle hook。
- `/flexlb/update_log_level`。
- `/flexlb/cache_match/status`：查询当前配置模式、实际缓存源和 KVCM 健康状态。
- `POST /rtp_llm/notify_master` 通知主节点变化；`GET /rtp_llm/queue_snapshot` 查询排队快照。
- gRPC 调度入口及 follower-to-master forwarding。

Spring profile 与日志、监控 provider 的具体默认值见
`flexlb-api/src/main/resources/application.yml`。

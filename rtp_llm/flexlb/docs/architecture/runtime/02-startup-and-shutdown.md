# 启动与停机

FlexLB 的上线由同机 HTTP hook 驱动；下线由 HTTP hook 或 Spring 容器关闭事件驱动。
高可用由 ZooKeeper LeaderSelector 主选举与 follower 请求转发实现。

主要代码：`flexlb-api/src/main/java/org/flexlb/service/grace/ApplicationLifecycle.java`、
`flexlb-common/.../listener/ApplicationWarmupState.java`，`flexlb-api/.../AppStateHookServer.java`、
`HealthCheckServer.java`、`FlexlbGrpcServer.java`，以及 `flexlb-sync/.../consistency/`。

## 生命周期 Hook

### 预热状态

`flexlb-common` 中的 `ApplicationWarmupState` 是独立 Spring Bean，使用 volatile 字段
共享预热状态。`ApplicationLifecycle` 通过 `setWarmupFinished()` 更新状态，
KVCM 客户端通过 `isWarmupFinished()` 读取状态。该状态类不依赖 gRPC 服务、调度器
或生命周期编排器。

`ApplicationLifecycle` 在上线开始时将状态置为 false，给后台首次轮询预留 3 秒，然后置为 true；这一步不等某个 worker 注册成功或返回状态。
健康检查读取同一状态，并同时检查下线标志。`KvcmGrpcClient` 注入
`ApplicationWarmupState`，在预热完成前不累计心跳和查询失败次数，避免提前触发容灾切换。
生命周期编排器依赖 `FlexlbGrpcServer` 完成请求排空；KVCM 客户端只依赖独立状态 Bean，
不反向依赖生命周期编排器。

### 编排与触发

以下 HTTP 端点由 `AppStateHookServer` 提供，仅允许 loopback 或本机地址访问：

| 端点 | 行为 |
|---|---|
| `GET /hook/process_ok` | `ApplicationReadyEvent` 后返回 200，否则 503 |
| `GET /hook/after_start` | 同步执行 `ApplicationLifecycle.online()`，上报 `online_complete` |
| `GET /hook/pre_stop` | 在 boundedElastic 上执行 `ApplicationLifecycle.offline()`，排空完成后返回 200 |

**上线顺序**（`test` profile 下跳过）：

1. 重置下线标志和预热状态，调用 `LBStatusConsistencyService.start()` 启动主选举。
2. 固定等待 3 秒，然后标记预热完成；等待被中断时恢复线程中断标志，
   预热完成标志仍在 finally 中置位。

**下线顺序**：

1. 置下线标志，使健康检查返回不可用。
2. 调用 `LBStatusConsistencyService.offline()` 下线并让主。
3. 调用 `FlexlbGrpcServer.drain()`，等待没有新 Schedule 请求的静默期；期间仍处理迟到请求。
   静默期由 `grpcServer.shutdownQuietPeriodMs` 控制，默认 5 秒。
4. 关闭 gRPC 新请求入口，等待已接受的 RPC 完成，再允许 Spring 销毁服务资源。
   强制终止期限由平台管理。

`ApplicationLifecycle` 以最高优先级处理所属 Spring 容器的 `ContextClosedEvent`，
同步执行下线流程；子容器的关闭事件不触发服务下线。成功完成后的重复下线调用直接返回。

### 健康检查

`GET /health` 通过 `ApplicationLifecycle.isHealthy()` 判断状态：
预热完成且未收到下线信号时返回 200，否则返回 404。**200 表示 FlexLB 副本完成预热，不保证已有可路由的 P/D。** 例如发现了 P，但其 DP 组的 rank 还没到齐，健康检查可以通过，这组 P 仍不能接新请求。

### 指标

`GracefulLifecycleReporter`：gauge `graceful.lifecycle.event`，tag `type`
（`process_ok`/`zk_node_online`/`warmer_complete`/`online_complete`/`health_check_offline`/
`zk_node_offline`/`shutdown_complete`/`shutdown_timeout`）+ `duration_ms`，值为时间戳。

## 容器启动超时

`FLEXLB_START_TIMEOUT_SECONDS` 设置 Java 启动到 `/hook/process_ok` 健康检查成功的等待上限，
默认 300 秒，必须为正整数。健康检查使用剩余启动时间作为请求超时。
到期后启动脚本发送 SIGTERM，等待最多 30 秒，进程仍存活则发送 SIGKILL；
清理 Java 进程和 PID 文件后以状态码 4 退出。容器重新拉起由部署平台的重启策略决定。

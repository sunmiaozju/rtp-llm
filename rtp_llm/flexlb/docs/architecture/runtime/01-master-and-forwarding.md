# 选主与请求转发

## 主选举与一致性

### 配置

一致性行为已收拢到 `FLEXLB_CONFIG.consistency` tagged union。默认
`{"type":"NONE"}`，相关 start/offline/destroy 都是 no-op、`isMaster()` 恒 false；启用时使用：

```json
{
  "consistency": {
    "type": "ZOOKEEPER",
    "connectString": "zk-1:2181,zk-2:2181",
    "sessionTimeoutMs": 30000,
    "connectionTimeoutMs": 30000,
    "masterRefreshIntervalMs": 5000
  }
}
```

一致性组件在 Bean 初始化时取得配置，因此 Nacos 可以保存和替换这部分字段，但当前进程
是否启用一致性及 ZooKeeper 客户端参数在重启后生效。选举路径和主节点变更通知使用
`DeploymentIdentity`：Spectrum 三元组齐全时为 `spectrum:<workspace>:<application>:<deployment>`；
Spectrum 三元组不完整时使用完整的 `BIZ_NAME:DEPLOYMENT_NAME:ZONE_NAME` 对应值的冒号拼接。
两组三元组均不完整时启动失败。端口取
Spring `server.port`，再回退 JVM `-Dserver.port` 和默认 7001（假定所有副本同端口）。

### ZookeeperMasterElectService

- Curator recipe：**LeaderSelector**（非 LeaderLatch），namespace `whale-master`，路径
  `/master_lb_leader/<deploymentId>`，`setId(本机IP)`，`autoRequeue()`，重试
  `ExponentialBackoffRetry(1000, 3)`。
- `takeLeadership()`：置 `isMaster=true`，**主动 HTTP 通知所有非 leader 参与者**
  `POST http://<ip>:<port>/rtp_llm/notify_master`（1s 超时）；然后阻塞在 CountDownLatch 上
  保持领导权。
- `stateChanged()`：`SUSPENDED`/`LOST` → 抛 `CancelLeadershipException` 放弃领导权
  （LOST 同时清空 master 缓存）。
- master 缓存：每 5s `updateLatestMaster()` 从 `leaderSelector.getLeader().getId()` 刷新
  `cachedMasterHostIp`；每 1s 上报 master 节点指标。
- **优雅让主**：`offline()` 停止重新参选；若自己是 master，则释放 latch。多节点时每秒检查一次，发现 leader 已不是本机就返回；连续 30 轮仍是本机也会结束等待。`pre_stop` 返回不保证已有新 master 接手。

### LBStatusConsistencyService（Spring 门面，实现 MasterElectService）

- `handleMasterChange(req)`：`/rtp_llm/notify_master` 的接收端——校验 `roleId` 匹配后
  `refreshMasterHost(true)` 强刷缓存。
- `getMasterHostIpPort()`：master IP + 本机 serverPort。
- `syncLBStatusFromMaster`（每 500ms 调度）与 `dumpLBStatus()` 目前是 **TODO 空实现**
  （`/rtp_llm/schedule_snapshot` 恒返回成功占位）。

### Master 转发与本地降级

**Leader（master）和 follower 都轮询 worker，区别在于谁调度、谁记账。** 每个副本都运行 [MasterEngineSynchronizer](../../../flexlb-sync/src/main/java/org/flexlb/sync/synchronizer/MasterEngineSynchronizer.java)，独立发现节点、发起 `GetWorkerStatus`，按自己收到的最新上报更新负载。正常情况下 leader 选 P/D，并在本机记录已分配的名额和 KV；follower 只转发这条请求，不复制这笔预占。follower 自己降级调度的请求则记在自己这里。

例如 leader 刚返回 P1/D1，引擎还没在 `WorkerStatus` 中报告该请求。leader 已将它算作在途，follower 的快照和本地账本里却都没有它。此时换主，原 follower 能根据已有的 worker 快照继续选路，但可能低估 P1/D1 的占用；等引擎上报到达，它才能在自己的视图里看到这条请求。这与缓存是否命中无关。

`FlexlbServiceImpl` 在启用一致性且本节点非 Master 时通过 gRPC 转发调度请求。
Master 缺位、明确返回未接管请求的 `NOT_MASTER`，或转发发生网络 I/O 故障时，
本节点使用已有的路由与调度流程执行本地决策，PV 日志记录 `LOCAL_FALLBACK`。

网络故障通过异常因果链识别：gRPC `UNAVAILABLE` 或 `UNKNOWN` 携带
`IOException`（包括连接拒绝、连接重置、连接关闭、DNS 解析失败）或
`UnresolvedAddressException` 时允许本地决策。只有状态码而没有网络异常证据的失败、
业务拒绝、RPC 超时和取消不触发本地降级。已过期或取消的请求不再进入本地调度。

本地降级优先保证可用性，不依赖 ZooKeeper 选主完成。连接断开不能证明原 Master
未接收请求，因此该模式不提供跨 Master 的严格单次调度保证；本地调用仍沿用原请求 ID
和截止时间。换主期间如果 KVCM 也不可用，两个降级条件分别判断，见[容灾与降级](../../design/runtime/03-failover-and-degradation.md)。

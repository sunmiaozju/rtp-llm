package org.flexlb.balance.schedulev2.prefill;

import org.flexlb.balance.estimate.WorkView;
import org.flexlb.balance.schedulev2.request.Request;

/**
 * 已从 Prefill 队列交给 Decode 调度、尚未完成 Prefill 的请求跟踪记录。
 * <p>
 * 进入 Decode 队列时创建，用于负载预测，不代表占用 Prefill 活跃名额。
 * 最终发布名额由 PrefillActiveTasks 单独管理。引擎上报后改成 ENGINE_QUEUED 或 ENGINE_RUNNING。
 * 引擎上报过、后来从活跃列表消失，或上报 Finished 时移除记录。请求失败或取消时也会移除。
 * 请求发布或最近一次引擎上报后超过配置时长，记录和占用也会被清理。
 */
public final class DispatchedRequest {

    private final Request request;
    /**
     * 这个请求在引擎上要新占的 KV token 数（输入 token 数减去命中缓存的部分）。
     */
    private final long kvTokens;
    /**
     * 初次预测的 Prefill 工作量；引擎上报有效缓存命中或剩余 token 进度后替换为纠偏后的工作量。
     */
    private volatile long predictedMs;
    /**
     * 最近一次包含该请求的引擎上报时间；未上报时为零。
     */
    private volatile long lastStatusUpdatedAtMs;
    private volatile WorkView.Phase phase = WorkView.Phase.COMMITTED;

    public DispatchedRequest(Request request, long kvTokens, long predictedMs) {
        this.request = request;
        this.kvTokens = kvTokens;
        this.predictedMs = predictedMs;
    }

    public Request request() {
        return request;
    }

    public long kvTokens() {
        return kvTokens;
    }

    public long predictedMs() {
        return predictedMs;
    }

    /**
     * 由 PrefillQueue 在记录锁内更新，与节点累计工作量的差值更新一起完成。
     */
    void updatePredictedMs(long predictedMs) {
        this.predictedMs = predictedMs;
    }

    public WorkView.Phase phase() {
        return phase;
    }

    public void updateStatus(WorkView.Phase phase, long nowMs) {
        this.phase = phase;
        lastStatusUpdatedAtMs = nowMs;
    }

    /**
     * 引擎是否已经上报过这个请求。
     */
    public boolean seenByEngine() {
        return lastStatusUpdatedAtMs > 0L;
    }

    public boolean statusExpired(long nowMs, long timeoutMs) {
        long publishedAtMs = request.publishedAtMs();
        if (publishedAtMs <= 0L) {
            return false;
        }
        long lastUpdateAtMs = Math.max(publishedAtMs, lastStatusUpdatedAtMs);
        return nowMs - lastUpdateAtMs >= timeoutMs;
    }
}

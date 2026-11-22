package org.flexlb.balance.schedulev2.prefill;

import lombok.Data;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.dao.master.WorkerStatus;

import java.util.Objects;

/**
 * 已从 Prefill 队列交给 Decode 调度、尚未完成 Prefill 的请求跟踪记录。
 * <p>
 * 尝试占 Prefill 在途名额时创建，用于负载预测。名额由 PrefillActiveTasks 单独管理。
 * 引擎上报后改成 ENGINE_QUEUED 或 ENGINE_RUNNING。
 * 引擎上报过、后来从活跃列表消失，或上报 Finished 时移除记录。请求失败或取消时也会移除。
 * 请求发布或最近一次引擎上报后超过配置时长，记录和占用也会被清理。
 */
@Data
public final class DispatchedRequest {

    public enum Phase {
        COMMITTED,
        ENGINE_QUEUED,
        ENGINE_RUNNING
    }

    private final Request request;
    /**
     * 初次预测的 Prefill 工作量；引擎确认缓存命中后，随实际命中或剩余 token 变化更新。
     */
    private volatile long predictedMs;

    /**
     * 按有效实际命中纠正的总耗时；-1 表示尚未计算
     */
    private long correctedTotalMs = -1L;

    private volatile WorkerStatus.TaskObservation workerStatus;

    /**
     * 最近一次包含该请求的引擎上报时间；未上报时为零。
     */
    private volatile long lastStatusUpdatedAtMs;
    private volatile Phase phase = Phase.COMMITTED;

    public DispatchedRequest(Request request, long predictedMs) {
        this.request = request;
        this.predictedMs = predictedMs;
    }

    public Request request() {
        return request;
    }

    public long predictedMs() {
        return predictedMs;
    }

    boolean isWorkLoadChanged(WorkerStatus.TaskObservation newWorkerStatus) {
        WorkerStatus.TaskObservation oldWorkerStatus = workerStatus;
        return oldWorkerStatus == null || oldWorkerStatus.prefixLengthValid() != newWorkerStatus.prefixLengthValid()
                || oldWorkerStatus.prefixLength() != newWorkerStatus.prefixLength()
                || !Objects.equals(oldWorkerStatus.remainingPrefillTokens(), newWorkerStatus.remainingPrefillTokens());
    }

    /**
     * 由 PrefillQueue 在记录锁内更新，与节点累计工作量的差值更新一起完成。
     */
    void updatePredictedMs(long predictedMs) {
        this.predictedMs = predictedMs;
    }

    public Phase phase() {
        return phase;
    }

    public void updateStatus(Phase phase, long nowMs) {
        this.phase = phase;
        lastStatusUpdatedAtMs = nowMs;
    }

    void updateWorkerStatus(WorkerStatus.TaskObservation workerStatus) {
        this.workerStatus = workerStatus;
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

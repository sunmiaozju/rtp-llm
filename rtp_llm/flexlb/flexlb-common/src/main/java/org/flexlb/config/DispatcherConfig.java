package org.flexlb.config;

import lombok.Getter;
import lombok.Setter;

/**
 * Per-worker concurrency limits for dispatch decisions.
 */
@Getter
@Setter
public final class DispatcherConfig {

    public static final int DEFAULT_MAX_INFLIGHT_PER_PREFILL_WORKER = 2;
    public static final int DEFAULT_PREFILL_SUBQUEUE_LIMIT_MULTIPLIER = 4;
    public static final int DEFAULT_MAX_INFLIGHT_PER_ENCODER_WORKER = Integer.MAX_VALUE;
    public static final long DEFAULT_DISPATCH_STARVATION_THRESHOLD_MS = 500L;

    public enum Type {
        /**
         * 新的非 batch 链路：先选 Prefill，Prefill 出队后再选 Decode。
         */
        NON_BATCH_V2
    }

    private Type type = Type.NON_BATCH_V2;
    private int decodeQueueThreads = Runtime.getRuntime().availableProcessors();
    private int mainQueueConsumerThreads = Runtime.getRuntime().availableProcessors();
    private int requestPrepareThreads = 2 * Runtime.getRuntime().availableProcessors();
    private int requestPrepareMaxThreads = 1_000;
    private int maxInflightPerPrefillWorker = DEFAULT_MAX_INFLIGHT_PER_PREFILL_WORKER;
    /** Shared waiting capacity = endpoint count * in-flight limit * this multiplier. */
    private int prefillSubqueueLimitMultiplier = DEFAULT_PREFILL_SUBQUEUE_LIMIT_MULTIPLIER;
    private long sharedSlotRetryMs = 5L;
    private int maxInflightPerEncoderWorker = DEFAULT_MAX_INFLIGHT_PER_ENCODER_WORKER;
    /**
     * 仅用于兼容旧配置，不再影响出队顺序。
     */
    private long dispatchStarvationThresholdMs = DEFAULT_DISPATCH_STARVATION_THRESHOLD_MS;
    /**
     * NON_BATCH_V2：缓存匹配查询结果的复用有效期，随匹配结果计算的 Prefill 耗时一起缓存。
     * 默认 30 秒，支持热更新；候选节点变化时立即失效。
     */
    private long cacheMatchResultTtlMs = 30_000L;

    public String typeName() {
        return type.name();
    }

    void validate() {
        if (type == null) {
            throw new ConfigValidationException("dispatcher.type", "is required");
        }
        if (decodeQueueThreads <= 0) {
            throw new ConfigValidationException("dispatcher.decodeQueueThreads", "must be greater than zero");
        }
        if (mainQueueConsumerThreads <= 0) {
            throw new ConfigValidationException("dispatcher.mainQueueConsumerThreads", "must be greater than zero");
        }
        if (requestPrepareThreads <= 0) {
            throw new ConfigValidationException("dispatcher.requestPrepareThreads", "must be greater than zero");
        }
        if (requestPrepareMaxThreads < requestPrepareThreads) {
            throw new ConfigValidationException("dispatcher.requestPrepareMaxThreads",
                    "must be greater than or equal to dispatcher.requestPrepareThreads");
        }
        if (maxInflightPerPrefillWorker <= 0) {
            throw new ConfigValidationException("dispatcher.maxInflightPerPrefillWorker",
                    "must be greater than zero");
        }
        if (prefillSubqueueLimitMultiplier <= 0) {
            throw new ConfigValidationException("dispatcher.prefillSubqueueLimitMultiplier",
                    "must be greater than zero");
        }
        if (sharedSlotRetryMs <= 0L) {
            throw new ConfigValidationException("dispatcher.sharedSlotRetryMs", "must be greater than zero");
        }
        if (maxInflightPerEncoderWorker <= 0) {
            throw new ConfigValidationException("dispatcher.maxInflightPerEncoderWorker",
                    "must be greater than zero");
        }
        if (dispatchStarvationThresholdMs < 0L) {
            throw new ConfigValidationException("dispatcher.dispatchStarvationThresholdMs", "must not be negative");
        }
        if (cacheMatchResultTtlMs <= 0L) {
            throw new ConfigValidationException("dispatcher.cacheMatchResultTtlMs", "must be greater than zero");
        }
    }
}

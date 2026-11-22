package org.flexlb.dao;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public final class BalanceMetrics {

    public record QueueBlockMetricKey(String queue, String reason) {
    }

    public record PrefillRouteMetrics(long selectedHitTokens,
                                      long candidateMaxHitTokens,
                                      String affinityDecision,
                                      Long overCapMs) {
    }

    @Getter
    public static final class BlockCounts {
        private final AtomicLong attemptCount = new AtomicLong();
        private final AtomicLong deduplicatedBlockCount = new AtomicLong();
    }

    private final ConcurrentHashMap<QueueBlockMetricKey, BlockCounts> blockedCounts = new ConcurrentHashMap<>();

    private int prepareSubmitRejectedCount;

    @Setter
    private volatile Long lastCacheMatchUs;

    @Setter
    private volatile Long estimatedPrefillComputeTokens;

    @Setter
    private PrefillRouteMetrics prefillRouteMetrics;

    public void recordPrepareSubmitRejected() {
        prepareSubmitRejectedCount++;
    }
}

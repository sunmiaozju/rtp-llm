package org.flexlb.dao;

import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public final class BalanceMetrics {

    public record QueueBlockMetricKey(String queue, String reason) {
    }

    @Getter
    public static final class BlockCounts {
        private final AtomicLong attemptCount = new AtomicLong();
        private final AtomicLong deduplicatedBlockCount = new AtomicLong();
    }

    private final ConcurrentHashMap<QueueBlockMetricKey, BlockCounts> blockedCounts = new ConcurrentHashMap<>();
}

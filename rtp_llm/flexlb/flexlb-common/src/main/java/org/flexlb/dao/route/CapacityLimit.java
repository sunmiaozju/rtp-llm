package org.flexlb.dao.route;

import org.flexlb.config.RoutingConfig;
import org.flexlb.util.CommonUtils;

public record CapacityLimit(long maxEngineRequests, long maxKvUsagePercent) {

    public CapacityLimit {
        if (maxEngineRequests < 0L || maxKvUsagePercent <= 0L || maxKvUsagePercent > RoutingConfig.PERCENTAGE_SCALE) {
            throw new IllegalArgumentException("Decode admission limits are outside their domain");
        }
    }

    public CapacityDeficit evaluate(CapacityUsage usage, long requiredKvTokens, long kvBudgetTokens) {
        return evaluate(usage, requiredKvTokens, kvBudgetTokens, CapacityRelease.NONE);
    }

    /**
     * Use the same occupancy scope for the observation and every exact victim release.
     */
    public CapacityDeficit evaluate(CapacityUsage usage,
                                    long requiredKvTokens,
                                    long kvBudgetTokens,
                                    CapacityRelease release) {
        validateDemand(requiredKvTokens, kvBudgetTokens);
        long requests = maxEngineRequests == 0L
                ? 0L
                : shortfall(Math.max(0L, usage.occupiedRequests() - release.requests()), 1L, maxEngineRequests, 0L);
        if (usage.totalKvTokens() == 0L) {
            return new CapacityDeficit(requests, 0L, 0L);
        }
        return new CapacityDeficit(requests,
                shortfall(usage.reservedKvTokens(), requiredKvTokens, usage.availableKvTokens(),
                        release.requiredKvTokens()),
                shortfall(Math.max(0L, usage.kvBudgetUsedTokens() - release.kvBudgetTokens()), kvBudgetTokens,
                        kvBudget(usage.totalKvTokens()), 0L));
    }

    private static void validateDemand(long requiredKvTokens, long kvBudgetTokens) {
        if (requiredKvTokens < 0L || kvBudgetTokens < requiredKvTokens) {
            throw new IllegalArgumentException("Decode demand must satisfy KV budget >= required KV >= 0");
        }
    }

    /**
     * Integer arithmetic never rounds the configured KV budget up.
     */
    public long kvBudget(long totalKv) {
        if (totalKv < 0L) {
            throw new IllegalArgumentException("negative KV capacity");
        }
        return totalKv / 100L * maxKvUsagePercent + totalKv % 100L * maxKvUsagePercent / 100L;
    }

    private static long shortfall(long used, long incoming, long capacity, long released) {
        long remainingUsed = Math.max(0L, used - released);
        long remainingCapacity = CommonUtils.saturatedAddNonNegative(capacity, Math.max(0L, released - used));
        return remainingUsed > remainingCapacity
                ? CommonUtils.saturatedAddNonNegative(remainingUsed - remainingCapacity, incoming)
                : Math.max(0L, incoming - (remainingCapacity - remainingUsed));
    }
}

package org.flexlb.dao.route;

import org.flexlb.util.CommonUtils;

/**
 * Capacity released by removing a request: before confirmation, input KV
 * and input plus max output; after confirmation, both use engine-reported KV.
 */
public record CapacityRelease(long requests, long requiredKvTokens, long kvBudgetTokens) {
    public static final CapacityRelease NONE = new CapacityRelease(0L, 0L, 0L);

    public CapacityRelease {
        if (requests < 0L || requiredKvTokens < 0L || kvBudgetTokens < requiredKvTokens) {
            throw new IllegalArgumentException("invalid Decode capacity release");
        }
    }

    public CapacityRelease plus(CapacityRelease other) {
        return new CapacityRelease(
                CommonUtils.saturatedAddNonNegative(requests, other.requests),
                CommonUtils.saturatedAddNonNegative(requiredKvTokens, other.requiredKvTokens),
                CommonUtils.saturatedAddNonNegative(kvBudgetTokens, other.kvBudgetTokens)
        );
    }
}

package org.flexlb.dao.route;

/**
 * Remaining shortages against available KV and the configured KV budget.
 */
public record CapacityDeficit(long requests, long requiredKvTokens, long kvBudgetTokens) {
    public boolean hasCapacity() {
        return requests == 0L && !needsKv();
    }

    public boolean needsKv() {
        return requiredKvTokens > 0L || kvBudgetTokens > 0L;
    }

    public long kvTokens() {
        return Math.max(requiredKvTokens, kvBudgetTokens);
    }
}

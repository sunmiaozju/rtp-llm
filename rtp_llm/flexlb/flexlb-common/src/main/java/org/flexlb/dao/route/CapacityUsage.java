package org.flexlb.dao.route;

public record CapacityUsage(long occupiedRequests,
                            long totalKvTokens,
                            long availableKvTokens,
                            long reservedKvTokens,
                            long kvBudgetUsedTokens) {
    public CapacityUsage {
        if (occupiedRequests < 0L || totalKvTokens < 0L || availableKvTokens < 0L || reservedKvTokens < 0L
                || kvBudgetUsedTokens < 0L) {
            throw new IllegalArgumentException("Decode occupancy must be non-negative");
        }
    }

    public long availableKvAfterReservations() {
        return Math.max(0L, availableKvTokens - reservedKvTokens);
    }
}

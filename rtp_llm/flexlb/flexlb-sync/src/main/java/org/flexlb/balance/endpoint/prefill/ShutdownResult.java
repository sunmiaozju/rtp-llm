package org.flexlb.balance.endpoint.prefill;

import org.flexlb.balance.scheduler.ScheduledRequest;

import java.util.List;

public record ShutdownResult(List<ScheduledRequest> removedItems, Throwable checkFailure) {
    public ShutdownResult {
        removedItems = List.copyOf(removedItems);
    }
}

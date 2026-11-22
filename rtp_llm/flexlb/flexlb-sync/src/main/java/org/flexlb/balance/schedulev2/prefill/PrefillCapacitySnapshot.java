package org.flexlb.balance.schedulev2.prefill;

import org.flexlb.balance.schedulev2.QueueBlockReason;

import java.util.Map;
import java.util.Set;

/**
 * 这台 Prefill 的 Worker 活跃请求和 Master 本地在途名额。
 */
record PrefillCapacitySnapshot(Map<String, DispatchedRequest> localPublishReservations,
                               Set<String> workerActiveRequestIds) {

    long inflightCount() {
        long count = workerActiveRequestIds.size();
        for (String requestId : localPublishReservations.keySet()) {
            if (!workerActiveRequestIds.contains(requestId)) {
                count++;
            }
        }
        return count;
    }

    QueueBlockReason blockReason(int limit) {
        return inflightCount() >= limit ? QueueBlockReason.PREFILL_INFLIGHT_FULL : null;
    }
}

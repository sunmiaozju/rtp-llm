package org.flexlb.balance.endpoint.prefill;

import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.enums.PriorityPreemptionProgress;

import java.util.Objects;

public record FinishReport(String requestId, long errorCode, PriorityPreemptionProgress preemptionProgress) {
    public static FinishReport from(WorkerStatus.TaskObservation task) {
        return new FinishReport(task.requestId(), task.errorCode(), task.priorityPreemptionProgress());
    }

    public FinishReport merge(FinishReport other) {
        if (!Objects.equals(requestId, other.requestId)) {
            throw new IllegalArgumentException("cannot merge finish reports of different requests");
        }
        long mergedError = errorCode != 0L ? errorCode : other.errorCode;
        return new FinishReport(requestId, mergedError,
                PriorityPreemptionProgress.merge(preemptionProgress, other.preemptionProgress));
    }
}

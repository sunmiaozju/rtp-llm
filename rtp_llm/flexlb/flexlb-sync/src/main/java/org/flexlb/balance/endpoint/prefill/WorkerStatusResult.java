package org.flexlb.balance.endpoint.prefill;

import org.flexlb.balance.endpoint.prefill.batch.FinishedBatch;

import java.util.List;

public record WorkerStatusResult(List<WorkerStatusFact> schedulerFacts,
                                 List<FinishedBatch> finishedBatches,
                                 Throwable publicationFailure) {
}

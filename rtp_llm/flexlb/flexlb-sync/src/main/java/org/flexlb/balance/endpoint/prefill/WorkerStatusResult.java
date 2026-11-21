package org.flexlb.balance.endpoint.prefill;

import java.util.List;

public record WorkerStatusResult(List<WorkerStatusFact> schedulerFacts, Throwable publicationFailure) {
}

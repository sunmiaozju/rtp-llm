package org.flexlb.mockengine;

import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for production endpoint selection in the mock-engine loop. */
class ProductionRoutingDeadlineE2ETest {

    private static final int BASE_PORT = 63300;

    @Test
    @Timeout(10)
    void requestDeadlineIsNotMisclassifiedAsAPlacementCapacityMiss()
            throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT, 1, 1, "1", 1.0, false)) {
            h.startAutoPump(5L);

            long nearDeadlineId = 88_001L;
            long secondRequestId = 88_002L;
            BalanceContext nearDeadline = h.context(
                    Long.toString(nearDeadlineId), 50, 10_000L, 8);
            nearDeadline.setSchedulingMetadata(SchedulingMetadata.explicit(
                    50, System.currentTimeMillis() + 2_000L));

            CompletableFuture<Response> first = h.submitAndGenerate(nearDeadline);
            CompletableFuture<Response> second = h.submitAndGenerate(
                    h.context(Long.toString(secondRequestId), 50, 10_000L, 8));
            Response firstResponse = first.get(1, TimeUnit.SECONDS);
            Response secondResponse = second.get(1, TimeUnit.SECONDS);

            assertTrue(firstResponse.isSuccess(),
                    "production routing must admit serviceable work before its deadline: "
                            + firstResponse.getCode() + ": "
                            + firstResponse.getErrorMessage());
            assertTrue(secondResponse.isSuccess(),
                    "second request must also receive a route");
            AutoTpmE2EHarness.await(
                    () -> h.engineArrivalOrder.contains(nearDeadlineId)
                            && h.engineArrivalOrder.contains(secondRequestId),
                    1_000L,
                    "both admitted requests must reach the mock engine");
        }
    }
}

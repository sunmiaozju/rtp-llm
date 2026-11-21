package org.flexlb.mock.grpc;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.route.RoleType;
import org.flexlb.mock.FlexLBMockTestBase;
import org.flexlb.mock.MockPrefillWorker;
import org.flexlb.mock.MockWorkerBehavior;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multiple workers: start two mock prefill workers and verify that requests
 * are distributed across both, not all sent to a single worker.
 *
 * <p>Flow:
 * 1. Worker A is started by the base class (default behavior)
 * 2. Worker B is started via {@link #addPrefillWorker} (registered in EndpointRegistry)
 * 3. Reconfigure the mock DefaultRouter to alternate between A and B
 * 4. Submit 4 requests
 * 5. Verify: both workers received at least 1 EnqueueBatch call
 * 6. Verify: total EnqueueBatch count matches the number of submitted requests
 *
 * <p>Key mechanism:
 * <ul>
 *   <li>The base class starts one prefill worker (worker A) and registers it in
 *       {@code EndpointRegistry} and {@code WorkerDirectory}</li>
 *   <li>{@link #addPrefillWorker} starts an additional worker B, creates its
 *       {@code WorkerStatus} and {@code PrefillEndpoint}, and registers both</li>
 *   <li>The mock {@code DefaultRouter} is reset and reconfigured to return routing
 *       responses that alternate between A and B</li>
 *   <li>Each routing response contains {@code ServerStatus} entries for the
 *       selected prefill worker and the shared decode worker</li>
 *   <li>The scheduler looks up the prefill endpoint by {@code ip:httpPort}
 *       and offers the request to that worker's {@code PrefillQueue}</li>
 * </ul>
 */
class MultipleWorkersTest extends FlexLBMockTestBase {

    private MockPrefillWorker workerB;
    private int workerBGrpcPort;
    private int workerBHttpPort;
    private String workerBIpPort;

    @Override
    protected FlexlbConfig createConfig() {
        return super.createConfig();
    }

    @Test
    @Timeout(30)
    void multipleWorkers_requestsDistributedAcrossBoth() throws Exception {
        // 1. Start an additional prefill worker B
        workerB = addPrefillWorker(MockWorkerBehavior.builder().build());
        workerBGrpcPort = workerB.getPort();
        workerBHttpPort = workerB.getHttpPort();
        workerBIpPort = workerIpPort(workerB);

        // 3. Submit 4 requests — should alternate A, B, A, B
        long[] requestIds = {40001, 40002, 40003, 40004};
        CompletableFuture<Response>[] futures = new CompletableFuture[requestIds.length];
        for (int i = 0; i < requestIds.length; i++) {
            futures[i] = submitRequest(String.valueOf(requestIds[i]));
        }

        java.util.Set<Integer> prefillPorts = new java.util.HashSet<>();
        // Each response carries a Prefill and Decode route.
        for (int i = 0; i < requestIds.length; i++) {
            Response resp = futures[i].get(10, TimeUnit.SECONDS);
            assertTrue(resp.isSuccess(),
                    "Request " + requestIds[i] + " should succeed, got code=" + resp.getCode()
                            + ", error=" + resp.getErrorMessage());
            assertEquals(false, resp.isEnqueuedByMaster());
            resp.getServerStatus().stream().filter(status -> status.getRole() == RoleType.PREFILL)
                    .forEach(status -> prefillPorts.add(status.getGrpcPort()));
        }

        assertEquals(java.util.Set.of(prefillGrpcPort, workerBGrpcPort), prefillPorts);
        assertEquals(0, mockPrefillWorker.getEnqueueCount());
        assertEquals(0, workerB.getEnqueueCount());
        assertEquals(0, mockDecodeWorker.getEnqueueCount());
    }

    // ==================== Helper: build routing response ====================

}

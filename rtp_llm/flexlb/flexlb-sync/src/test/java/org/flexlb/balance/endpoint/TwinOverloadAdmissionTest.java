package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Twin overload protection at real FlexLB endpoint state machines. WorkerStatus
 * replaces the external engine; there is no mock performance model or GPU here.
 * Batch-limit assertions exercise deliveries owned by this Master. They do not
 * require unowned engine batches to consume local batch delivery capacity.
 */
class TwinOverloadAdmissionTest {

    private final List<WorkerEndpoint> endpoints = new ArrayList<>();

    @AfterEach
    void closeEndpoints() {
        endpoints.forEach(WorkerEndpoint::close);
    }

    @Test
    void nonBatchPreservesUnknownEngineBacklogUntilAnAuthoritativeUpdate() {
        FlexlbConfig config = config(null);
        config.setDispatcher(DispatcherConfig.nonBatch());
        PrefillEndpoint endpoint = prefill(config);
        applyStatus(endpoint, tasks(101L, 8, 70L, TaskPhase.RUNNING), Map.of());
        assertEquals(8, endpoint.observedRequestCount());
        assertTrue(endpoint.captureLoadView().work().totalRemainingWorkMs().isEmpty());
        applyStatus(endpoint, Map.of(), tasks(101L, 8, 70L, TaskPhase.RUNNING));
        assertEquals(0, endpoint.observedRequestCount());
        assertEquals(0L, endpoint.captureLoadView().work().totalRemainingWorkMs().orElseThrow());
    }

    @Test
    void decodeRequestCapMustBlockEvenWithAlmostEmptyKvPool() {
        DecodeEndpoint endpoint = new DecodeEndpoint(
                EndpointTestSupport.workerStatus(RoleType.DECODE, "127.0.0.2", 8080, 8090),
                EndpointTestSupport.noopEventSink());
        endpoints.add(endpoint);
        applyStatus(endpoint, Map.of(), Map.of());
        for (long id = 1; id <= 9; id++) {
            try (WorkerEndpoint.EndpointHold hold = endpoint.tryHold()) {
                assertNotNull(hold);
                assertNotNull(endpoint.reserve(hold, Long.toString(id), 128L, 256L, 50));
            }
        }
        for (long id = 1; id <= 8; id++) {
            DecodeEndpoint.EngineDispatchPermitAcquisition acquisition = endpoint.acquireDispatchPermit(
                    endpoint.reservationHandle(Long.toString(id)), new DecodeEndpoint.CapacityLimit(8L, 90L));
            assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.SUCCESS, acquisition.status());
            assertEquals(DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED,
                    acquisition.permit().dispatch());
        }
        assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.CAPACITY_FULL, endpoint
                .acquireDispatchPermit(endpoint.reservationHandle("9"), new DecodeEndpoint.CapacityLimit(8L, 90L))
                .status());

        applyStatus(endpoint, tasks(2L, 7, 10L, TaskPhase.RUNNING),
                tasks(1L, 1, 10L, TaskPhase.RUNNING));
        DecodeEndpoint.EngineDispatchPermitAcquisition resumed = endpoint
                .acquireDispatchPermit(endpoint.reservationHandle("9"), new DecodeEndpoint.CapacityLimit(8L, 90L));
        assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.SUCCESS, resumed.status());
        assertTrue(resumed.permit().release());
    }

    private PrefillEndpoint prefill(FlexlbConfig config) {
        EndpointTestSupport.TestRequestRuntime runtime = EndpointTestSupport.requestRuntime();
        PrefillEndpoint endpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090),
                () -> config, EndpointTestSupport.routeStrategy(runtime), runtime.events(),
                mock(BatchSchedulerReporter.class),
                new org.flexlb.balance.scheduler.PlacementAvailability());
        endpoints.add(endpoint);
        endpoint.startGeneration();
        return endpoint;
    }

    private static FlexlbConfig config(Integer batchLimit) {
        FlexlbConfig config = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        config.fixedWindowDecision().setMaxRequests(8);
        config.queueScheduler().setQueueTimeoutMs(3_600_000L);
        config.fixedWindowDecision().setMaxCollectionWaitMs(400L);
        if (batchLimit != null) {
            config.getDispatcher().setMaxInflightPerPrefillWorker(batchLimit);
        }
        return config;
    }

    private static ScheduledRequest item(PrefillEndpoint endpoint, FlexlbConfig config, long id) {
        Request request = new Request();
        request.setRequestId(Long.toString(id));
        request.setSeqLen(128L);
        BalanceContext context = new BalanceContext(config);
        context.setRequest(request);
        ServerStatus selected = new ServerStatus();
        selected.setRole(RoleType.PREFILL);
        selected.setServerIp("127.0.0.1");
        selected.setHttpPort(8080);
        selected.setGrpcPort(8090);
        return new ScheduledRequest(context, null, null, selected, null,
                endpoint, null, null, System.currentTimeMillis());
    }

    private static Map<String, TaskInfoPB.Builder> tasks(long firstId, int count, long batchId, TaskPhase phase) {
        Map<String, TaskInfoPB.Builder> result = new LinkedHashMap<>();
        for (long id = firstId; id < firstId + count; id++) {
            TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
            task.setRequestIdStr(Long.toString(id));
            task.setBatchId(batchId);
            WorkerStatusPBs.phase(task, phase);
            task.setInputLength(128L);
            result.put(Long.toString(id), task);
        }
        return result;
    }

    private static void applyStatus(WorkerEndpoint endpoint,
                                    Map<String, TaskInfoPB.Builder> running,
                                    Map<String, TaskInfoPB.Builder> finished) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setRunningTasks(response, running);
        WorkerStatusPBs.setFinishedTasks(response, finished);
        response.setTotalKvCache(22_000_000L);
        response.setAvailableKvCache(21_900_000L);
        EndpointTestSupport.applyStatus(endpoint, response).run();
    }
}

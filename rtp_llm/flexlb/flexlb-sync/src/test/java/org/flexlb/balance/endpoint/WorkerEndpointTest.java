package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerEndpointTest {

    private WorkerStatus status;
    private PrefillEndpoint endpoint;

    @BeforeEach
    void setUp() {
        status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL,
                "group-x",
                "10.0.0.1",
                8080,
                8081,
                "site-x");
        FlexlbConfig config = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator()
                .setExpression("sum(computeTokens)");
        EndpointTestSupport.TestRequestRuntime requestRuntime =
                EndpointTestSupport.requestRuntime();
        endpoint = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                () -> config,
                EndpointTestSupport.routeStrategy(requestRuntime),
                requestRuntime.events(),
                Mockito.mock(BatchSchedulerReporter.class),
                new org.flexlb.balance.scheduler.PlacementAvailability());
        endpoint.startGeneration();
    }

    @AfterEach
    void tearDown() {
        endpoint.close();
    }

    @Test
    void ipPort_format() {
        assertEquals("10.0.0.1:8080@0", endpoint.ipPort());
    }

    @Test
    void endpointHoldCanBeReleasedByCompletionThread() {
        WorkerEndpoint.EndpointHold hold = endpoint.tryHold();

        CompletableFuture.runAsync(hold::close).join();

        assertThrows(IllegalArgumentException.class, () -> endpoint.checkHold(hold));
    }

    // ==================== getStatus() returns live reference ====================

    @Test
    void getStatus_returns_live_reference() {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setAlive(true);
        response.setAvailableConcurrency(42);
        response.setDpRank(3);
        EndpointTestSupport.applyStatus(endpoint, response);

        WorkerStatus liveStatus = endpoint.getStatus();
        assertSame(status, liveStatus);
        assertTrue(liveStatus.pollHealth().reportedAlive());
        assertEquals(42L, (long) liveStatus.getAvailableConcurrency());
        assertEquals(3L, liveStatus.getDpRank());
    }

    // ==================== WorkerStatus response transaction ====================

    @Test
    void responseFieldsAndAppliedCursorsHaveSeparateCommitBoundaries() {
        WorkerStatusPB.Builder resp = WorkerStatusPB.newBuilder();
        resp.setRoleType(WorkerStatusPBs.role(RoleType.DECODE));
        resp.setAlive(true);
        resp.setAvailableConcurrency(8);
        resp.setStepLatencyMs(25.0);
        resp.setIterateCount(100);
        resp.setDpSize(4);
        resp.setTpSize(2);
        resp.setDpRank(1);
        resp.setMaxSeqLen(131072L);
        resp.setMaxBatchTokensSize(262144L);
        resp.setAvailableKvCache(10000L);
        resp.setStatusVersion(5L);
        resp.setLatestFinishedVersion(3L);

        WorkerStatus.PreparedStatus prepared;
        status.lock.lock();
        try {
            prepared = status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, resp.build()));
        } finally {
            status.lock.unlock();
        }

        assertEquals(RoleType.PREFILL, status.getRole());
        assertEquals(-1L, status.appliedStatusCursor().statusVersion());
        assertEquals(-1L,
                status.appliedStatusCursor().latestFinishedTaskVersion());

        status.lock.lock();
        try {
            status.publishPreparedStatus(prepared);
            status.recordSuccessfulPoll(true);
        } finally {
            status.lock.unlock();
        }
        assertEquals(RoleType.DECODE, status.getRole());
        assertTrue(status.pollHealth().reportedAlive());
        assertEquals(8L, (long) status.getAvailableConcurrency());
        assertEquals(25.0, status.getStepLatencyMs(), 0.001);
        assertEquals(100L, status.getIterateCount());
        assertEquals(4L, status.getDpSize());
        assertEquals(2L, status.getTpSize());
        assertEquals(1L, status.getDpRank());
        assertEquals(131072L, status.getMaxSeqLen());
        assertEquals(262144L, status.getMaxBatchTokensSize());
        assertEquals(10000L, status.getAvailableKvCacheTokens());
        assertEquals(5L, status.appliedStatusCursor().statusVersion());
        assertEquals(3L,
                status.appliedStatusCursor().latestFinishedTaskVersion());
    }

    @Test
    void nullStatusResponseIsRejectedBeforeMutation() {
        WorkerStatus.CommittedWorkerStatus before = status.committedWorkerStatus();
        assertThrows(NullPointerException.class, () -> EngineStatusConverter.convertToStatusObservation(status, null));
        assertSame(before, status.committedWorkerStatus());
    }

    // ==================== onWorkerStatusUpdate ====================

    @Test
    void foreignStatusGenerationCannotRebindEndpoint() {
        WorkerStatusPB.Builder resp = WorkerStatusPB.newBuilder();
        resp.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        resp.setStatusVersion(1L);
        WorkerStatus newStatus = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "group-b", "10.0.0.2",
                8082, 8083, "site-a");
        newStatus.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared = newStatus
                    .prepareNewStatus(EngineStatusConverter.convertToStatusObservation(newStatus, resp.build()));
            assertThrows(IllegalArgumentException.class,
                    () -> endpoint.applyPreparedStatus(newStatus, prepared));
        } finally {
            newStatus.lock.unlock();
        }
        assertSame(status, endpoint.getStatus());
    }

    @Test
    void onWorkerStatusUpdate_calibrates_prefill() {
        WorkerStatusPB.Builder resp = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setFinishedTasks(resp, Map.of("100", task("100", 1000, 0, 1L)));

        // 仅上报完成任务时，protobuf 当前任务列表默认为空。
        EndpointTestSupport.applyStatus(endpoint, resp);
    }

    @Test
    void initializeFromAppliedStatusPreservesGenerationFields() {
        WorkerStatusPB.Builder resp = WorkerStatusPB.newBuilder();
        resp.setDpRank(5);
        resp.setAlive(true);

        EndpointTestSupport.applyStatus(endpoint, resp);

        assertEquals("site-x", endpoint.getStatus().getSite());
        assertEquals("group-x", endpoint.getStatus().getGroup());
        assertEquals(5L, endpoint.getStatus().getDpRank());
        assertTrue(endpoint.getStatus().pollHealth().reportedAlive());
    }

    private void calibrate(Map<String, TaskInfoPB.Builder> finished, Map<String, TaskInfoPB.Builder> running) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setFinishedTasks(response, finished);
        WorkerStatusPBs.setRunningTasks(response, running);
        EndpointTestSupport.applyStatus(endpoint, response);
    }

    private ScheduledRequest item(long requestId, long seqLen) {
        return new ScheduledRequest(
                ctx(Long.toString(requestId), seqLen),
                null,
                null,
                null,
                null,
                endpoint,
                null,
                null,
                0L);
    }

    private BalanceContext ctx(String requestId, long seqLen) {
        Request req = new Request();
        req.setRequestId(requestId);
        req.setSeqLen(seqLen);
        BalanceContext ctx = new BalanceContext(org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig());
        ctx.setRequest(req);
        return ctx;
    }

    private TaskInfoPB.Builder task(String requestId, long inputLength, long prefixLength, long batchId) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(requestId);
        task.setInputLength(inputLength);
        task.setPrefixLength(prefixLength);
        task.setBatchId(batchId);
        return task;
    }
}

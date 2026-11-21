package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.prefill.DispatchedRequest;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PrefillEndpointTest {

    private PrefillEndpoint endpoint;
    private FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final RequestSchedulerReporter v2Reporter = mock(RequestSchedulerReporter.class);

    @AfterEach
    void tearDown() {
        if (endpoint != null) {
            endpoint.close();
        }
    }

    @Test
    void finishedOnlyStatusReleasesV2PrefillClaim() {
        var queue = createV2Queue();
        var request = V2TestSupport.request(SchedulingTestConfig.newConfig(), "901", 50, 10);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertEquals(1L, queue.inflightCount());
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setFinishedTasks(response, Map.of("901", taskInfo(901L, 0L, null, 0, 10)));
        Runnable notification = EndpointTestSupport.applyStatus(endpoint, response);
        assertEquals(1, queue.dispatchedCount());
        notification.run();
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void unchangedVersionHeartbeatRefreshesV2RequestStatus() {
        var queue = createV2Queue();
        var request = V2TestSupport.request(SchedulingTestConfig.newConfig(), "902", 50, 10);
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        var record = new DispatchedRequest(request, 10L, 5L);
        V2TestSupport.dispatch(queue, record);
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertTrue(record.statusExpired(System.currentTimeMillis(), 1000L));
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        response.setAlive(true);
        WorkerStatusPBs.setRunningTasks(response, Map.of("902", taskInfo(902L, 0L, TaskPhase.RUNNING, 0, 10)));
        var status = endpoint.getStatus();
        response.setStatusVersion(status.appliedStatusCursor().statusVersion());
        endpoint.observeStatusHeartbeat(status,
                EngineStatusConverter.convertToStatusObservation(status, response.build())).run();
        assertEquals(1, queue.dispatchedCount());
        assertEquals(1L, queue.inflightCount());
    }

    @Test
    void unchangedVersionStatusCleansExpiredPublishedRequestUsingCurrentConfig() {
        var queue = createV2Queue();
        var request = V2TestSupport.request(config, "903", 50, 10);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertTrue(request.publish(org.flexlb.balance.schedulev2.request.Stage.DECODE_WAITING, List.of()));
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        response.setAlive(true);
        var status = endpoint.getStatus();
        response.setStatusVersion(status.appliedStatusCursor().statusVersion());

        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(60_000L);
        endpoint.observeStatusHeartbeat(status,
                EngineStatusConverter.convertToStatusObservation(status, response.build())).run();
        assertEquals(1, queue.dispatchedCount());
        org.mockito.Mockito.verifyNoInteractions(v2Reporter);

        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1_000L);
        endpoint.observeStatusHeartbeat(status,
                EngineStatusConverter.convertToStatusObservation(status, response.build())).run();
        endpoint.observeStatusHeartbeat(status,
                EngineStatusConverter.convertToStatusObservation(status, response.build())).run();
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        assertEquals(0L, queue.estimatedWorkMs(0L));
        verify(v2Reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
        org.mockito.Mockito.verifyNoMoreInteractions(v2Reporter);
    }

    @ParameterizedTest
    @EnumSource(value = TaskPhase.class, names = {"PENDING", "RUNNING"})
    void activeStatusRefreshesRequestBeforeCallbackChecksTimeout(TaskPhase phase) {
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1000L);
        var queue = createV2Queue();
        var request = V2TestSupport.request(config, "904", 50, 10);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertTrue(request.publish(org.flexlb.balance.schedulev2.request.Stage.DECODE_WAITING, List.of()));
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setRunningTasks(response, Map.of("904", taskInfo(904L, 0L, phase, 0, 10)));

        EndpointTestSupport.applyStatus(endpoint, response).run();

        assertEquals(1, queue.dispatchedCount());
        assertEquals(1L, queue.inflightCount());
        org.mockito.Mockito.verifyNoInteractions(v2Reporter);
    }

    @Test
    void finishedStatusStillReportsEarlierRequestStatusTimeout() {
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1000L);
        var queue = createV2Queue();
        var request = V2TestSupport.request(config, "905", 50, 10);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertTrue(request.publish(org.flexlb.balance.schedulev2.request.Stage.DECODE_WAITING, List.of()));
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setFinishedTasks(response, Map.of("905", taskInfo(905L, 0L, null, 0, 10)));

        EndpointTestSupport.applyStatus(endpoint, response).run();

        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        verify(v2Reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
    }

    @Test
    void retirementRemovesExistingV2DispatchRecords() {
        var queue = createV2Queue();
        var request = V2TestSupport.request(config, "906", 50, 10);
        request.setQueueSeq(1L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertNull(queue.tryAcquirePublishSlot(request, 1));
        assertTrue(request.publish(org.flexlb.balance.schedulev2.request.Stage.DECODE_WAITING, List.of()));
        var waiting = V2TestSupport.request(config, "907", 50, 10);
        waiting.setQueueSeq(2L);
        V2TestSupport.choose(waiting, endpoint, 0L, 7L);
        waiting.moveTo(org.flexlb.balance.schedulev2.request.Stage.MAIN_WAITING,
                org.flexlb.balance.schedulev2.request.Stage.PREFILL_WAITING);
        queue.addWaiting(waiting);

        endpoint.close();
        endpoint.close();

        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        assertEquals(1, queue.waitingCount());
        assertEquals(7L, queue.estimatedWorkMs(0L));
        assertFalse(queue.removeDispatched(request));
        queue.removeWaiting(waiting);
        assertEquals(0L, queue.estimatedWorkMs(0L));
        org.mockito.Mockito.verifyNoInteractions(v2Reporter);
    }

    private PrefillQueue createV2Queue() {
        if (endpoint != null) {
            endpoint.close();
        }
        var queues = mock(PrefillQueueRegistry.class);
        var configService = mock(org.flexlb.config.ConfigService.class);
        org.mockito.Mockito.when(configService.loadBalanceConfig()).thenReturn(config);
        var events = new org.flexlb.balance.scheduler.EndpointEventHandler(mock(RequestRegistry.class), queues,
                configService, new org.flexlb.balance.schedulev2.decode.DecodeReservations(),
                mock(org.flexlb.service.monitor.RequestSchedulerReporter.class));
        endpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090), config,
                events, v2Reporter);
        var queue = endpoint.prefillQueue();
        org.mockito.Mockito.when(queues.find(endpoint)).thenReturn(queue);
        return queue;
    }

    private static void applyHeartbeat(PrefillEndpoint endpoint, WorkerStatusPB.Builder response) {
        WorkerStatus status = endpoint.getStatus();
        Runnable statusNotifier;
        status.lock.lock();
        try {
            response.setStatusVersion(status.appliedStatusCursor().statusVersion());
            statusNotifier = endpoint.observeStatusHeartbeat(status,
                    EngineStatusConverter.convertToStatusObservation(status, response.build()));
        } finally {
            status.lock.unlock();
        }
        statusNotifier.run();
    }

    private static TaskInfoPB.Builder taskInfo(long id, long batchId, TaskPhase phase, long prefix, long input) {
        var task = TaskInfoPB.newBuilder().setRequestIdStr(Long.toString(id)).setBatchId(batchId)
                .setPrefixLength(prefix).setInputLength(input);
        if (phase != null) {
            WorkerStatusPBs.phase(task, phase);
        }
        return task;
    }
}

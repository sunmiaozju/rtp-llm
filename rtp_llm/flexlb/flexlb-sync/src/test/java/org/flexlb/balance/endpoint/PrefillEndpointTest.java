package org.flexlb.balance.endpoint;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.prefill.CommitResult;
import org.flexlb.balance.endpoint.prefill.ReservationResult;
import org.flexlb.balance.scheduler.RequestSlot.DeliveryClaim;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.prefill.DispatchedRequest;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.DebugInfo;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.PriorityPreemptionProgress;
import org.flexlb.enums.TaskPhase;
import org.flexlb.metric.MicrometerFlexMonitor;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class PrefillEndpointTest {

    private PrefillEndpoint endpoint;
    private FlexlbConfig config;
    private BatchSchedulerReporter endpointReporter;
    private EndpointTestSupport.TestRequestRuntime requestRuntime;
    private final RequestSchedulerReporter v2Reporter = mock(RequestSchedulerReporter.class);

    @BeforeEach
    void setUp() {
        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.1", 8080, 8090);

        config = org.flexlb.balance.scheduler.SchedulingTestConfig.routeConfig();
        configureBatch(config, config.fixedWindowDecision().getMaxRequests(), 300, null);
        setFormula(config, "10 + 0.1*sum(computeTokens) + 5*batchSize");

        endpointReporter = mock(BatchSchedulerReporter.class);
        requestRuntime = EndpointTestSupport.requestRuntime();
        endpoint = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                config,
                EndpointTestSupport.routeStrategy(requestRuntime),
                requestRuntime.events(),
                endpointReporter);
        endpoint.startGeneration();
    }

    @AfterEach
    void tearDown() {
        endpoint.close();
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
        endpoint.close();
        var queues = mock(PrefillQueueRegistry.class);
        var configService = mock(org.flexlb.config.ConfigService.class);
        org.mockito.Mockito.when(configService.loadBalanceConfig()).thenReturn(config);
        var events = new org.flexlb.balance.scheduler.EndpointEventHandler(requestRuntime.requests(), queues,
                configService, new org.flexlb.balance.schedulev2.decode.DecodeReservations(),
                mock(org.flexlb.service.monitor.RequestSchedulerReporter.class));
        endpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090), config,
                EndpointTestSupport.routeStrategy(requestRuntime), events, endpointReporter, v2Reporter);
        endpoint.startGeneration();
        var queue = endpoint.prefillQueue();
        org.mockito.Mockito.when(queues.find(endpoint)).thenReturn(queue);
        return queue;
    }

    // ---- batch commit / release ----

    @Test
    void nonBatchPublishOwnsOneExactQueuedIdentityUntilRemoval() {
        FlexlbConfig routeConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        routeConfig.setDispatcher(DispatcherConfig.nonBatch());
        PrefillEndpoint routeEndpoint = routeEndpoint(routeConfig, "127.0.0.4");
        try {
            assertEquals(0, routeEndpoint.queuedRequestCount());

            ScheduledRequest queued = createScheduledRequest(
                    routeEndpoint, routeConfig, 1L, 500, 200);
            assertTrue(EndpointTestSupport.offer(routeEndpoint, queued));
            assertEquals(1, routeEndpoint.queuedRequestCount());

            assertTrue(routeEndpoint.removeQueued(queued, "test cleanup"));
            assertEquals(0, routeEndpoint.queuedRequestCount());
        } finally {
            routeEndpoint.close();
        }
    }

    private PrefillEndpoint routeEndpoint(FlexlbConfig routeConfig, String ip) {
        PrefillEndpoint routeEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(
                        RoleType.PREFILL, ip, 8080, 8090),
                routeConfig,
                EndpointTestSupport.routeStrategy(requestRuntime),
                requestRuntime.events(),
                endpointReporter);
        routeEndpoint.startGeneration();
        return routeEndpoint;
    }

    @Test
    void inflightMaxAgeMetricTracksTimeSinceLatestObservation()
            throws InterruptedException {
        // Reported max age measures staleness (time since the batch was last
        // observed), not wall-clock time since creation: a fresh RUNNING
        // observation resets it, after which it grows with elapsed idle time.
        commitQueuedRoute(endpoint, 1L, 5_000L);
        calibrate(Map.of(), Map.of(
                "1", taskInfo(1L, 1L, TaskPhase.RUNNING, 0, 0)));
        Thread.sleep(30);

        endpoint.reportBatchMetrics(endpointReporter);

        verify(endpointReporter).reportInflightMaxAgeMs(
                anyString(), anyString(), org.mockito.ArgumentMatchers.longThat(age -> age >= 20));
    }

    @Test
    void calibrateMissingBatchIdRemovesDirectRequestLedgerEntry() {
        commitQueuedRoute(endpoint, 101L, 100L);
        assertEquals(1, endpoint.getSingleRequestCount());

        TaskInfoPB.Builder finished = TaskInfoPB.newBuilder();
        finished.setRequestIdStr("101");
        finished.setBatchId(-1L);
        calibrate(Map.of("101", finished), Map.of());

        assertEquals(0, endpoint.getSingleRequestCount());
        assertEquals(0, endpoint.observedRequestCount());
    }

    @Test
    void committedWorkMetricIsZeroWhenIdle() {
        assertEquals(0L, endpoint.getLoadMetric().orElseThrow());
    }

    @Test
    void committedWorkMetricReflectsInflightPrediction() {
        commitQueuedRoute(endpoint, 1L, 5_000L);

        long remainingWorkMs = endpoint.getLoadMetric().orElseThrow();
        assertTrue(remainingWorkMs > 0,
                "inflight work must have a positive remaining duration");
        assertTrue(remainingWorkMs <= 5000,
                "remaining work must not exceed the original prediction");
    }

    @Test
    void runningCommittedWorkMetricDecreasesWithElapsedTime() throws InterruptedException {
        commitQueuedRoute(endpoint, 1L, 5_000L);

        long remainingBefore = endpoint.getLoadMetric().orElseThrow();

        // Mark the batch as running so elapsed time counts
        Map<String, TaskInfoPB.Builder> running = new HashMap<>();
        TaskInfoPB.Builder runningTask = TaskInfoPB.newBuilder();
        runningTask.setRequestIdStr("1");
        runningTask.setBatchId(1L);
        WorkerStatusPBs.phase(runningTask, TaskPhase.RUNNING);
        running.put("1", runningTask);
        calibrate(Map.of(), running);

        Thread.sleep(50);

        long remainingAfter = endpoint.getLoadMetric().orElseThrow();
        assertTrue(remainingAfter <= remainingBefore,
                "remaining work must decrease after observed progress");
    }

    // ---- eviction ----

    @Test
    void observedRequestCountUnionsEngineTasksWithLocalLedger() {
        commitQueuedRoute(endpoint, 101L, 100L);
        commitQueuedRoute(endpoint, 102L, 100L);

        TaskInfoPB.Builder overlapping = taskInfo(102L, 1L, TaskPhase.RUNNING, 0, 0);
        TaskInfoPB.Builder untrackedOne = taskInfo(900L, 90L, TaskPhase.RUNNING, 0, 0);
        TaskInfoPB.Builder untrackedTwo = taskInfo(901L, 91L, TaskPhase.RECEIVED, 0, 0);
        TaskInfoPB.Builder duplicateUntracked = taskInfo(900L, 92L, TaskPhase.RUNNING, 0, 0);
        TaskInfoPB.Builder overlayOnly = taskInfo(999L, 99L, TaskPhase.PENDING, 0, 0);
        overlayOnly.setPriorityPreemptionProgress(WorkerStatusPBs.progress(PriorityPreemptionProgress.CANCELING));

        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setRunningTasks(response, Map.of("102", overlapping, "900a", untrackedOne, "901", untrackedTwo,
                "900b", duplicateUntracked, "999", overlayOnly));
        EndpointTestSupport.applyStatus(endpoint, response);

        assertEquals(4, endpoint.observedRequestCount(),
                "two local requests plus two unique Engine-only tasks");

        response.clearRunningTaskInfo();
        EndpointTestSupport.applyStatus(endpoint, response);
        assertEquals(2, endpoint.observedRequestCount());
    }

    @Test
    void observedRequestCountFallsBackToEngineQueryLengthScalars() {
        commitQueuedRoute(endpoint, 101L, 100L);

        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setWaitingQueryLen(3);
        response.setRunningQueryLen(2);
        EndpointTestSupport.applyStatus(endpoint, response);

        assertEquals(6, endpoint.observedRequestCount(),
                "an unseen local shadow cannot prove identity with scalar Engine work");
        assertTrue(endpoint.captureLoadView().work().hasUnknownWork());
        assertTrue(endpoint.captureLoadView().work().totalRemainingWorkMs().isEmpty());
    }

    @Test
    void observedRequestCountUsesConservativeScalarBoundForPartialTaskDetails() {
        commitQueuedRoute(endpoint, 101L, 100L);

        TaskInfoPB.Builder overlapping = taskInfo(101L, 1L, TaskPhase.RUNNING, 0, 0);
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setRunningTasks(response, Map.of("101", overlapping));
        response.setWaitingQueryLen(3);
        response.setRunningQueryLen(2);
        EndpointTestSupport.applyStatus(endpoint, response);

        assertEquals(5, endpoint.observedRequestCount(),
                "scalar active count must cover a partial detail list without double-counting local tasks");
    }

    @Test
    void observedRequestCountIncludesBatcherQueue() throws InterruptedException {
        PrefillEndpoint queuedEndpoint = newFixedWindowEndpoint(60_000L);
        try {
            assertEquals(0, queuedEndpoint.observedRequestCount());
            ScheduledRequest item = createScheduledRequest(
                    queuedEndpoint, 1L, 500L, 200L);
            assertTrue(EndpointTestSupport.offer(queuedEndpoint, item));

            assertEquals(1, queuedEndpoint.observedRequestCount(),
                    "pending count includes the canonical ACTIVE queue owner");
        } finally {
            queuedEndpoint.close();
        }
    }

    @Test
    void loadViewCapturesExactPublishedIdentityBeforeActiveRemoval() {
        PrefillEndpoint handoffEndpoint = newFixedWindowEndpoint(60_000);
        try {
            ScheduledRequest active = createScheduledRequest(handoffEndpoint, 111L, 500L, 0L);
            assertTrue(EndpointTestSupport.offer(handoffEndpoint, active));

            org.flexlb.balance.estimate.PrefillTtftEstimate.LoadView snapshot =
                    handoffEndpoint.captureLoadView();
            assertEquals(1, snapshot.queue().activeItems().size());
            assertEquals("111",
                    snapshot.queue().activeItems().getFirst().requestId());
        } finally {
            handoffEndpoint.close();
        }
    }

    @Test
    void observedRequestCountCannotMissActiveToCommittedHandoff() throws Exception {
        PrefillEndpoint handoffEndpoint = newFixedWindowEndpoint(60_000);
        long requestId = 222L;
        try {
            ScheduledRequest active = createScheduledRequest(
                    handoffEndpoint, requestId, 500L, 0L);
            assertTrue(EndpointTestSupport.offer(handoffEndpoint, active));
            assertTrue(handoffEndpoint.removeQueued(
                    active, "test exact ownership handoff"));
            commitQueuedRoute(handoffEndpoint, requestId, 100L);

            assertEquals(1L, handoffEndpoint.observedRequestCount());
            assertEquals(0, handoffEndpoint.queuedRequestCount());
            assertEquals(1,
                    handoffEndpoint.captureLoadView()
                            .work().requests().size());
        } finally {
            handoffEndpoint.close();
        }
    }

    // ---- batch metrics reporting ----

    @ParameterizedTest
    @EnumSource(SchedulerConfig.Type.class)
    void nonBatchInflightGaugeKeepsConfirmedRequestsWithinTheFourRequestLimit(SchedulerConfig.Type schedulerType) {
        FlexlbConfig requestConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        requestConfig.setDispatcher(DispatcherConfig.nonBatch());
        requestConfig.getDispatcher().setMaxInflightPerPrefillWorker(4);
        requestConfig.getScheduler().setType(schedulerType);
        PrefillEndpoint metricsEndpoint = routeEndpoint(requestConfig, "127.0.0.4");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            BatchSchedulerReporter reporter = new BatchSchedulerReporter(new MicrometerFlexMonitor(registry));
            reporter.init();
            Map<String, TaskInfoPB.Builder> running = new HashMap<>();
            for (long requestId = 1; requestId <= 4; requestId++) {
                ScheduledRequest item = createScheduledRequest(metricsEndpoint, requestConfig, requestId, 500, 0);
                try (var reservation = EndpointTestSupport.offerAndTakeSingleReservation(metricsEndpoint, item, 100L);
                        var admission = metricsEndpoint.tryBeginRouteCommitAdmission();
                        var commitResult = admission.commit(List.of(item), List.of(reservation))) {
                    running.put(item.requestId(), taskInfo(requestId, 0L, TaskPhase.RUNNING, 0, 0));
                }
            }
            metricsEndpoint.reportBatchMetrics(reporter);
            assertEquals(4.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());

            WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setRunningTasks(response, running);
            EndpointTestSupport.applyStatus(metricsEndpoint, response);
            metricsEndpoint.reportBatchMetrics(reporter);

            assertEquals(4.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
            assertFalse(metricsEndpoint.canDeliver(4L, 1));
        } finally {
            registry.close();
            metricsEndpoint.close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = TaskPhase.class, names = {"RECEIVED", "RUNNING"})
    void nonBatchUnconfirmedGaugeDropsOnConfirmationAndInflightDropsOnlyOnCompletion(TaskPhase phase) {
        FlexlbConfig requestConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        requestConfig.setDispatcher(DispatcherConfig.nonBatch());
        PrefillEndpoint metricsEndpoint = routeEndpoint(requestConfig, "127.0.0.4");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            BatchSchedulerReporter reporter = new BatchSchedulerReporter(new MicrometerFlexMonitor(registry));
            reporter.init();
            ScheduledRequest item = createScheduledRequest(metricsEndpoint, requestConfig, 1L, 500, 0);
            try (var reservation = EndpointTestSupport.offerAndTakeSingleReservation(metricsEndpoint, item, 100L);
                    var admission = metricsEndpoint.tryBeginRouteCommitAdmission();
                    var commitResult = admission.commit(List.of(item), List.of(reservation))) {
                metricsEndpoint.reportBatchMetrics(reporter);
            }
            assertEquals(1.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());

            WorkerStatusPB.Builder running = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setRunningTasks(running, Map.of("1", taskInfo(1L, 0L, phase, 0, 0)));
            EndpointTestSupport.applyStatus(metricsEndpoint, running);
            metricsEndpoint.reportBatchMetrics(reporter);
            assertEquals(0.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
            assertEquals(1.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());

            WorkerStatusPB.Builder finished = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setFinishedTasks(finished, Map.of("1", taskInfo(1L, 0L, TaskPhase.RUNNING, 0, 100L)));
            EndpointTestSupport.applyStatus(metricsEndpoint, finished);
            metricsEndpoint.reportBatchMetrics(reporter);
            assertEquals(0.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
            assertEquals(0.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
        } finally {
            registry.close();
            metricsEndpoint.close();
        }
    }

    @Test
    void nonBatchQueueGaugeSeparatesQueuedRequestsFromEngineOnlyWork() {
        FlexlbConfig requestConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        requestConfig.setDispatcher(DispatcherConfig.nonBatch());
        PrefillEndpoint metricsEndpoint = routeEndpoint(requestConfig, "127.0.0.4");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            BatchSchedulerReporter reporter = new BatchSchedulerReporter(new MicrometerFlexMonitor(registry));
            reporter.init();
            ScheduledRequest item = createScheduledRequest(metricsEndpoint, requestConfig, 1L, 500, 0);
            assertTrue(EndpointTestSupport.offer(metricsEndpoint, item));
            WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setRunningTasks(response, Map.of("900", taskInfo(900L, 0L, TaskPhase.RECEIVED, 0, 0)));
            EndpointTestSupport.applyStatus(metricsEndpoint, response);
            metricsEndpoint.reportBatchMetrics(reporter);

            assertEquals(1.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
            assertEquals(1, metricsEndpoint.queuedRequestCount());
            assertEquals(0.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.4:8080").gauge().value());
            assertFalse(metricsEndpoint.canAcceptRequest(
                    requestConfig.getDispatcher().getMaxInflightPerPrefillWorker()));
        } finally {
            registry.close();
            metricsEndpoint.close();
        }
    }

    @Test
    void requestGaugesKeepSiblingPrefillEnginesAndWorkerRolesSeparate() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        WorkerStatus firstStatus = WorkerStatus.createDiscovered(
                RoleType.PREFILL, null, "127.0.0.1", 8080, 8090,
                null, null, 0, 2);
        WorkerStatus siblingStatus = WorkerStatus.createDiscovered(
                RoleType.PDFUSION, null, "127.0.0.1", 8080, 8090,
                null, null, 1, 2);
        FlexlbConfig requestConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        requestConfig.setDispatcher(DispatcherConfig.nonBatch());
        PrefillEndpoint first = EndpointTestSupport.standalonePrefillEndpoint(firstStatus, requestConfig,
                EndpointTestSupport.routeStrategy(requestRuntime), requestRuntime.events(), endpointReporter);
        PrefillEndpoint sibling = EndpointTestSupport.standalonePrefillEndpoint(siblingStatus, requestConfig,
                EndpointTestSupport.routeStrategy(requestRuntime), requestRuntime.events(), endpointReporter);
        first.startGeneration();
        sibling.startGeneration();
        try {
            BatchSchedulerReporter reporter = new BatchSchedulerReporter(new MicrometerFlexMonitor(registry));
            reporter.init();
            for (PrefillEndpoint target : List.of(first, sibling)) {
                ScheduledRequest item = createScheduledRequest(target, requestConfig, 1L, 500, 0);
                try (var reservation = EndpointTestSupport.offerAndTakeSingleReservation(target, item, 100L);
                        var admission = target.tryBeginRouteCommitAdmission();
                        var commitResult = admission.commit(List.of(item), List.of(reservation))) {
                    target.reportBatchMetrics(reporter);
                }
            }
            WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setRunningTasks(response, Map.of("1", taskInfo(1L, 0L, TaskPhase.RUNNING, 0, 0)));
            EndpointTestSupport.applyStatus(first, response);
            first.reportBatchMetrics(reporter);

            assertEquals(1.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.1:8080@0").gauge().value());
            assertEquals(1.0, registry.get("flexlb.app.flexlb.inflight.request.count")
                    .tags("role", "PDFUSION", "engineIp", "127.0.0.1:8080@1").gauge().value());
            assertEquals(0.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PREFILL", "engineIp", "127.0.0.1:8080@0").gauge().value());
            assertEquals(1.0, registry.get("flexlb.app.flexlb.worker.status.unconfirmed.request.count")
                    .tags("role", "PDFUSION", "engineIp", "127.0.0.1:8080@1").gauge().value());
        } finally {
            registry.close();
            first.close();
            sibling.close();
        }
    }

    @Test
    void reportBatchMetricsBucketsQueueLengthByPriority() {
        // Long fixed window so offered items stay queued during the assertions
        PrefillEndpoint slowEndpoint = newFixedWindowEndpoint(60_000);
        try {
            assertTrue(EndpointTestSupport.offer(
                    slowEndpoint, createPriorityScheduledRequest(slowEndpoint, 1L, 70)));
            assertTrue(EndpointTestSupport.offer(
                    slowEndpoint,
                    createScheduledRequest(slowEndpoint, 2L, 300, 0)));

            BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
            slowEndpoint.reportBatchMetrics(reporter);

            // Single-report with priority tag (no global untagged series)
            verify(reporter).reportBatcherQueueSize("PREFILL", "127.0.0.1:8080", 2);
            assertEquals(Map.of(70, 1, 0, 1), slowEndpoint.queuedRequestCountsByPriority());
            verify(reporter, never()).reportBatcherQueueDepthByPriority(any(), any(), anyInt(), anyInt());
        } finally {
            slowEndpoint.close();
        }
    }

    @Test
    void reportBatchMetricsEmitsPriorityZeroFallbackForEmptyQueue() {
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        endpoint.reportBatchMetrics(reporter);

        verify(reporter).reportBatcherQueueSize("PREFILL", "127.0.0.1:8080", 0);
        assertTrue(endpoint.queuedRequestCountsByPriority().isEmpty());
        verify(reporter, never()).reportBatcherQueueDepthByPriority(any(), any(), anyInt(), anyInt());
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "PDFUSION"})
    void reportsQueueAndInflightMetricsWithTheWorkerRole(RoleType role) {
        WorkerStatus status = EndpointTestSupport.workerStatus(role, "127.0.0.1", 8080, 8090);
        PrefillEndpoint metricsEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                status, config, EndpointTestSupport.routeStrategy(requestRuntime),
                requestRuntime.events(), endpointReporter);
        try {
            BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
            metricsEndpoint.reportBatchMetrics(reporter);

            verify(reporter).reportBatcherQueueSize(role.name(), "127.0.0.1:8080", 0);
            verify(reporter, never()).reportBatcherQueueDepthByPriority(any(), any(), anyInt(), anyInt());
            verify(reporter).reportInflightRequestCount(role.name(), "127.0.0.1:8080", 0);
            verify(reporter).reportInflightMaxAgeMs(role.name(), "127.0.0.1:8080", 0L);
        } finally {
            metricsEndpoint.close();
        }
    }

    // ---- WorkerEndpoint inherited behavior ----

    @Test
    void applyWorkerStatusResponseUpdatesAliveStatusOnSameGeneration() {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        response.setAlive(true);

        EndpointTestSupport.applyStatus(endpoint, response);

        assertTrue(endpoint.getStatus().pollHealth().reportedAlive());
    }

    // ---- close ----

    @Test
    void retirementOwnerCanReenterCloseFromSynchronousShutdownCallback() throws Exception {
        FlexlbConfig retirementConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(retirementConfig, 1, 0, null);
        retirementConfig.setDispatcher(DispatcherConfig.nonBatch());

        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.5", 8080, 8090);

        CountDownLatch reentrantCloseEntered = new CountDownLatch(1);
        CountDownLatch reentrantCloseReturned = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicReference<PrefillEndpoint> retirementEndpointRef = new AtomicReference<>();
        EndpointTestSupport.TestRequestRuntime runtime = new EndpointTestSupport.TestRequestRuntime();
        org.mockito.Mockito.doAnswer(invocation -> {
            reentrantCloseEntered.countDown();
            try {
                retirementEndpointRef.get().close();
                reentrantCloseReturned.countDown();
            } catch (Throwable failure) {
                callbackFailure.compareAndSet(null, failure);
            }
            return null;
        }).when(runtime.events()).onQueuedWorkerStopped(org.mockito.Mockito.any());
        PrefillEndpoint retirementEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                retirementConfig,
                EndpointTestSupport.routeStrategy(runtime),
                runtime.events(),
                mock(BatchSchedulerReporter.class));
        retirementEndpointRef.set(retirementEndpoint);
        retirementEndpoint.startGeneration();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ScheduledRequest item = createScheduledRequest(
                    retirementEndpoint, retirementConfig, 8_101L, 128, 0);
            assertTrue(EndpointTestSupport.offer(retirementEndpoint, item));

            Future<?> outerClose = executor.submit(retirementEndpoint::close);
            outerClose.get(2, TimeUnit.SECONDS);
            assertTrue(reentrantCloseEntered.await(1, TimeUnit.SECONDS));
            assertTrue(reentrantCloseReturned.await(1, TimeUnit.SECONDS),
                    "retirement-owner reentry must return instead of waiting on itself");
            assertTrue(callbackFailure.get() == null,
                    () -> String.valueOf(callbackFailure.get()));

            assertEquals(ReservationResult.Status.ENDPOINT_RETIRED,
                    retirementEndpoint
                            .reserveSingle(
                                    createScheduledRequest(retirementEndpoint, retirementConfig, 8_102L, 128, 0), 0L)
                            .status());
        } finally {
            retirementEndpoint.close();
            executor.shutdownNow();
        }
    }

    @Test
    void admittedCallbackCanCloseEndpointBeforeItsHandoffPermitIsReleased() throws Exception {
        FlexlbConfig retirementConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(retirementConfig, 1, 0, null);
        retirementConfig.setDispatcher(DispatcherConfig.nonBatch());
        setFormula(retirementConfig, "1");

        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.6", 8080, 8090);

        CountDownLatch callbackResolved = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicReference<PrefillEndpoint> retirementEndpointRef = new AtomicReference<>();
        EndpointTestSupport.TestRequestRuntime runtime =
                new EndpointTestSupport.TestRequestRuntime() {
            @Override
            void onCompleted(
                    DeliveryClaim claim,
                    DeliveryResult completion) {
                try {
                    retirementEndpointRef.get().close();
                } catch (Throwable failure) {
                    callbackFailure.compareAndSet(null, failure);
                } finally {
                    callbackResolved.countDown();
                }
            }
        };
        PrefillEndpoint retirementEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                retirementConfig,
                EndpointTestSupport.liveRouteStrategy(runtime),
                runtime.events(),
                mock(BatchSchedulerReporter.class));
        retirementEndpointRef.set(retirementEndpoint);
        retirementEndpoint.startGeneration();
        try {
            DecodeEndpoint decode = mock(DecodeEndpoint.class);
            DecodeEndpoint.ReservationHandle decodeReservation =
                    mock(DecodeEndpoint.ReservationHandle.class);
            org.mockito.Mockito.when(decodeReservation.requestId()).thenReturn("8201");
            DecodeEndpoint.EngineDispatchPermit permit =
                    mock(DecodeEndpoint.EngineDispatchPermit.class);
            org.mockito.Mockito
                    .when(decode.acquireDispatchPermit(org.mockito.Mockito.any(DecodeEndpoint.ReservationHandle.class),
                            org.mockito.Mockito.any(DecodeEndpoint.CapacityLimit.class)))
                    .thenReturn(new DecodeEndpoint.EngineDispatchPermitAcquisition(
                            DecodeEndpoint.EngineDispatchPermitAcquireStatus.SUCCESS, permit));
            org.mockito.Mockito.when(permit.dispatch())
                    .thenReturn(
                            DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED);
            ScheduledRequest admitted = createScheduledRequest(
                    retirementEndpoint,
                    retirementConfig,
                    8_201L,
                    128,
                    0,
                    decode,
                    decodeReservation);
            assertTrue(EndpointTestSupport.offer(retirementEndpoint, admitted));
            assertTrue(callbackResolved.await(2, TimeUnit.SECONDS));
            assertNull(callbackFailure.get(),
                    "an admitted handoff must defer cleanup instead of self-awaiting");
            retirementEndpoint.awaitRetirement();
        } finally {
            retirementEndpoint.close();
        }
    }

    @Test
    void zeroCompletionPredictionDoesNotBlockDelivery()
            throws Exception {
        FlexlbConfig routeConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(routeConfig, 1, 0, null);
        routeConfig.setDispatcher(DispatcherConfig.nonBatch());
        setFormula(routeConfig, "0");
        CountDownLatch firstPreparation = new CountDownLatch(1);
        CountDownLatch delivered = new CountDownLatch(1);
        EndpointTestSupport.TestRequestRuntime runtime = new EndpointTestSupport.TestRequestRuntime() {
            @Override
            void onCompleted(DeliveryClaim claim, DeliveryResult completion) {
                delivered.countDown();
            }
        };
        DeliveryStrategy strategy = org.mockito.Mockito.spy(
                EndpointTestSupport.liveRouteStrategy(runtime));
        org.mockito.Mockito.doAnswer(invocation -> {
            Object transaction = invocation.callRealMethod();
            firstPreparation.countDown();
            return transaction;
        }).when(strategy).prepare(org.mockito.Mockito.any());
        PrefillEndpoint routeEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.10", 8080, 8090),
                routeConfig, strategy, runtime.events(), endpointReporter);
        routeEndpoint.startGeneration();
        try {
            ScheduledRequest waiting = createScheduledRequest(routeEndpoint, routeConfig, 8_301L, 128, 0);
            assertTrue(EndpointTestSupport.offer(routeEndpoint, waiting));
            assertTrue(firstPreparation.await(2, TimeUnit.SECONDS));
            assertTrue(delivered.await(2, TimeUnit.SECONDS),
                    "zero predicted execution time must not require a later status update");
            assertEquals(0, routeEndpoint.queuedRequestCount());
            assertEquals(1, routeEndpoint.observedRequestCount(),
                    "delivery must retain ownership until Engine completion");
        } finally {
            routeEndpoint.close();
        }
    }

    @Test
    void unknownCompletionTimeDoesNotBlockDelivery() throws Exception {
        FlexlbConfig routeConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(routeConfig, 1, 0, null);
        routeConfig.setDispatcher(DispatcherConfig.nonBatch());
        setFormula(routeConfig, "100");
        CountDownLatch firstPreparation = new CountDownLatch(1);
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicInteger preparationCount = new AtomicInteger();
        EndpointTestSupport.TestRequestRuntime runtime = new EndpointTestSupport.TestRequestRuntime() {
            @Override
            void onCompleted(DeliveryClaim claim, DeliveryResult completion) {
                delivered.countDown();
            }
        };
        DeliveryStrategy strategy = org.mockito.Mockito.spy(
                EndpointTestSupport.liveRouteStrategy(runtime));
        org.mockito.Mockito.doAnswer(invocation -> {
            Object transaction = invocation.callRealMethod();
            preparationCount.incrementAndGet();
            firstPreparation.countDown();
            return transaction;
        }).when(strategy).prepare(org.mockito.Mockito.any());
        PrefillEndpoint routeEndpoint = EndpointTestSupport.standalonePrefillEndpoint(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.11", 8080, 8090),
                routeConfig, strategy, runtime.events(), endpointReporter);
        routeEndpoint.startGeneration();
        try {
            TaskInfoPB.Builder unknown = taskInfo(8_402L, -1L, TaskPhase.RUNNING, 0, -1L);
            unknown.setInputLength(128L);
            WorkerStatusPB.Builder status = WorkerStatusPB.newBuilder();
            WorkerStatusPBs.setRunningTasks(status, Map.of("8402", unknown));
            status.setRunningQueryLen(1);
            EndpointTestSupport.applyStatus(routeEndpoint, status).run();
            ScheduledRequest waiting = createScheduledRequest(routeEndpoint, routeConfig, 8_401L, 128, 0);
            assertTrue(EndpointTestSupport.offer(routeEndpoint, waiting));
            assertTrue(firstPreparation.await(2, TimeUnit.SECONDS));
            assertTrue(delivered.await(2, TimeUnit.SECONDS),
                    "unknown preceding execution time must not become an admission gate");
            assertEquals(0, routeEndpoint.queuedRequestCount());
            assertEquals(2, routeEndpoint.observedRequestCount(),
                    "Engine work and the delivered route retain separate identities");

            for (int repeat = 0; repeat < 20; repeat++) {
                applyHeartbeat(routeEndpoint, status);
            }
            assertEquals(1, preparationCount.get(), "identical heartbeats must not redeliver the route");
            assertEquals(0, routeEndpoint.queuedRequestCount());
        } finally {
            routeEndpoint.close();
        }
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

    @Test
    void closeHandlesEveryCommittedRouteAcrossSchedulingModes() {
        commitQueuedRoute(endpoint, 100L, 100L);
        ScheduledRequest route = createScheduledRequest(200L, 200L, 0L);
        // A queue route can only be reserved after its canonical item has been
        // offered into the ACTIVE queue; reserveSingle rejects a non-active
        // identity with REQUEST_LEFT_QUEUE.
        assertTrue(EndpointTestSupport.offer(endpoint, route));
        List<CommitResult> commitResults = EndpointTestSupport.commitRoutes(endpoint, 200L, List.of(route));
        commitResults.forEach(CommitResult::close);
        assertEquals(2, endpoint.getSingleRequestCount());

        endpoint.close();

        assertEquals(0, endpoint.getSingleRequestCount());
        EndpointTestSupport.PrefillRetirement retirement =
                requestRuntime.prefillRetirements().stream()
                        .findFirst()
                        .orElseThrow();
        assertEquals(List.of("100", "200"), retirement.ownedItems().stream()
                .map(ScheduledRequest::requestId).sorted().toList(),
                "both immediate and queued routes retain their canonical retirement owner");
        assertTrue(retirement.ownedItems().contains(route));
    }

    @Test
    void closeShutsDownBatcher() {
        endpoint.close();
        ScheduledRequest item = createScheduledRequest(1L, 500, 200);
        assertFalse(EndpointTestSupport.offer(endpoint, item));
        assertEquals(0, endpoint.queuedRequestCount());
    }

    // ---- helpers ----

    private PrefillEndpoint newFixedWindowEndpoint(long fixedWaitMs) {
        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.1", 8080, 8090);

        FlexlbConfig slowConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(slowConfig, 100, fixedWaitMs, null);
        EndpointTestSupport.TestRequestRuntime runtime =
                EndpointTestSupport.requestRuntime();
        PrefillEndpoint created = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                slowConfig,
                EndpointTestSupport.routeStrategy(runtime),
                runtime.events(),
                mock(BatchSchedulerReporter.class));
        created.startGeneration();
        return created;
    }

    private static PrefillEndpoint newEndpointWithFormula(String expression) {
        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.9", 8089, 8099);

        FlexlbConfig endpointConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(
                endpointConfig,
                endpointConfig.fixedWindowDecision().getMaxRequests(),
                300L,
                null);
        setFormula(endpointConfig, expression);
        EndpointTestSupport.TestRequestRuntime runtime =
                EndpointTestSupport.requestRuntime();
        PrefillEndpoint created = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                endpointConfig,
                EndpointTestSupport.routeStrategy(runtime),
                runtime.events(),
                mock(BatchSchedulerReporter.class));
        created.startGeneration();
        return created;
    }

    private ScheduledRequest createPriorityScheduledRequest(
            PrefillEndpoint owner, long requestId, int priority) {
        long now = System.currentTimeMillis();
        Request request = new Request();
        request.setRequestId(Long.toString(requestId));
        request.setSeqLen(500);
        request.setPriority(priority);

        BalanceContext ctx = new BalanceContext(config);
        ctx.setRequest(request);
        ctx.setSchedulingMetadata(SchedulingMetadata.explicit(priority, now + 60_000));

        return new ScheduledRequest(
                ctx, null, null, null, null, owner, null, null, now);
    }

    private void calibrate(Map<String, TaskInfoPB.Builder> finished, Map<String, TaskInfoPB.Builder> running) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setFinishedTasks(response, finished);
        WorkerStatusPBs.setRunningTasks(response, running);
        EndpointTestSupport.applyStatus(endpoint, response);
    }

    private static PrefillEndpoint createLearningEndpoint() {
        WorkerStatus status = EndpointTestSupport.workerStatus(
                RoleType.PREFILL, "127.0.0.8", 8080, 8090);

        FlexlbConfig learningConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        configureBatch(
                learningConfig,
                learningConfig.fixedWindowDecision().getMaxRequests(),
                300,
                null);
        learningConfig.getRouter().getRoles().getPrefill()
                .getExecutionTimeEstimator()
                .setType(RoutingConfig.EstimatorType.LEARNING);
        EndpointTestSupport.TestRequestRuntime runtime =
                EndpointTestSupport.requestRuntime();
        PrefillEndpoint created = EndpointTestSupport.standalonePrefillEndpoint(
                status,
                learningConfig,
                EndpointTestSupport.routeStrategy(runtime),
                runtime.events(),
                mock(BatchSchedulerReporter.class));
        created.startGeneration();
        return created;
    }

    private ScheduledRequest createScheduledRequest(long requestId, long seqLen, long hitCacheLen) {
        return createScheduledRequest(endpoint, requestId, seqLen, hitCacheLen);
    }

    private static ScheduledRequest createScheduledRequest(PrefillEndpoint owner,
                                             long requestId,
                                             long seqLen,
                                             long hitCacheLen) {
        return createScheduledRequest(
                owner, org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig(), requestId, seqLen, hitCacheLen);
    }

    private static ScheduledRequest createScheduledRequest(
            PrefillEndpoint owner,
            FlexlbConfig requestConfig,
            long requestId,
            long seqLen,
            long hitCacheLen) {
        return createScheduledRequest(
                owner,
                requestConfig,
                requestId,
                seqLen,
                hitCacheLen,
                null,
                null);
    }

    private static ScheduledRequest createScheduledRequest(
            PrefillEndpoint owner,
            FlexlbConfig requestConfig,
            long requestId,
            long seqLen,
            long hitCacheLen,
            DecodeEndpoint decode,
            DecodeEndpoint.ReservationHandle decodeReservation) {
        Request request = new Request();
        request.setRequestId(Long.toString(requestId));
        request.setSeqLen(seqLen);

        BalanceContext ctx = new BalanceContext(requestConfig);
        ctx.setRequest(request);

        ServerStatus prefill = new ServerStatus();
        prefill.setRole(RoleType.PREFILL);
        prefill.setServerIp("127.0.0.1");
        prefill.setHttpPort(8080);
        prefill.setGrpcPort(8090);
        DebugInfo debugInfo = new DebugInfo();
        debugInfo.setHitCacheLen(hitCacheLen);
        prefill.setDebugInfo(debugInfo);

        return new ScheduledRequest(
                ctx,
                null,
                null,
                prefill,
                null,
                owner,
                decode,
                decodeReservation,
                System.currentTimeMillis());
    }

    private static void configureBatch(
            FlexlbConfig target,
            int maxRequests,
            long maxCollectionWaitMs,
            Integer maxInflightBatches) {
        target.fixedWindowDecision().setMaxRequests(maxRequests);
        target.fixedWindowDecision().setMaxCollectionWaitMs(maxCollectionWaitMs);
        if (maxInflightBatches != null) {
            target.getDispatcher().setMaxInflightPerPrefillWorker(maxInflightBatches);
        }
    }

    private static void setFormula(FlexlbConfig target, String expression) {
        target.getRouter().getRoles().getPrefill().getExecutionTimeEstimator()
                .setExpression(expression);
    }

    private static TaskInfoPB.Builder priorityCanceledTask(long requestId, long batchId) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(Long.toString(requestId));
        task.setBatchId(batchId);
        task.getErrorInfoBuilder().setErrorCode(8429);
        task.getErrorInfoBuilder().setErrorMessage("priority preempted");
        task.setPriorityPreemptionProgress(WorkerStatusPBs.progress(PriorityPreemptionProgress.CANCELED));
        return task;
    }

    private static TaskInfoPB.Builder taskInfo(long requestId,
                                               long batchId,
                                               TaskPhase phase,
                                               int errorCode,
                                               long executionTimeMs) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(Long.toString(requestId));
        task.setBatchId(batchId);
        WorkerStatusPBs.phase(task, phase);
        task.getErrorInfoBuilder().setErrorCode(errorCode);
        task.setExecutionTimeMs(executionTimeMs);
        return task;
    }

    private static void commitQueuedRoute(PrefillEndpoint target, long requestId, long predictedMs) {
        FlexlbConfig routeConfig = org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig();
        routeConfig.setDispatcher(DispatcherConfig.nonBatch());
        ScheduledRequest item = createScheduledRequest(target, routeConfig, requestId, 500, 0);
        try (var reservation = EndpointTestSupport.offerAndTakeSingleReservation(target, item, predictedMs);
                var admission = target.tryBeginRouteCommitAdmission();
                var commitResult = admission.commit(List.of(item), List.of(reservation))) {
            // 提交后 Prefill 一直记着这个请求，直到引擎上报它结束。
        }
    }

}

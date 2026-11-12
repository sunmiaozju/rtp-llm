package org.flexlb.balance.schedulev2.prefill;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.estimate.PrefillTtftEstimate;
import org.flexlb.balance.estimate.WorkView;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.prediction.FormulaPredictor;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class PrefillQueueTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final PrefillEndpoint endpoint = V2TestSupport.prefillEndpoint("p1:8000");
    private final PrefillQueue queue = endpoint.prefillQueue();
    private final RequestSchedulerReporter reporter = mock(RequestSchedulerReporter.class);
    private final java.util.concurrent.atomic.AtomicLong nextQueueSeq = new java.util.concurrent.atomic.AtomicLong();

    @Test
    void cleanupDoesNotWaitForQueueMonitor() throws Exception {
        Request normal = request("normal");
        Request expired = request("expired");
        V2TestSupport.dispatch(queue, new DispatchedRequest(normal, 10L, 5L));
        V2TestSupport.dispatch(queue, new DispatchedRequest(expired, 10L, 5L));
        publish(expired);
        assertEquals(null, queue.tryAcquirePublishSlot(normal, 2));
        assertEquals(null, queue.tryAcquirePublishSlot(expired, 2));
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            synchronized (queue) {
                assertTrue(executor.submit(() -> queue.removeDispatched(normal))
                        .get(2, java.util.concurrent.TimeUnit.SECONDS));
                executor.submit(
                        () -> queue.removeRequestStatusExpired(System.currentTimeMillis() + 10_000L, 1000L, reporter))
                        .get(2, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(0, queue.dispatchedCount());
            }
        }
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void concurrentDeleteReleasesAndNotifiesOnce() throws Exception {
        Request request = request("delete-once");
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(request, 1));
        var notifications = new java.util.concurrent.atomic.AtomicInteger();
        queue.onCapacityChanged(notifications::incrementAndGet);
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> {
                start.await();
                return queue.removeDispatched(request);
            });
            var second = executor.submit(() -> {
                start.await();
                return queue.removeDispatched(request);
            });
            start.countDown();
            assertNotEquals(first.get(2, java.util.concurrent.TimeUnit.SECONDS),
                    second.get(2, java.util.concurrent.TimeUnit.SECONDS));
        }
        assertEquals(1, notifications.get());
        assertEquals(0L, queue.inflightCount());
        assertEquals(0L, queue.estimatedWorkMs(0L));
        assertEquals(org.flexlb.balance.schedulev2.QueueBlockReason.RESERVATION_CHANGED,
                queue.tryAcquirePublishSlot(request, 1));
    }

    @Test
    void expiryKeepsEngineStatusThatArrivesAfterTimeoutDecision() {
        Request request = request("late-status");
        var record = org.mockito.Mockito.spy(new DispatchedRequest(request, 10L, 5L));
        V2TestSupport.dispatch(queue, record);
        publish(request);
        assertEquals(null, queue.tryAcquirePublishSlot(request, 1));
        org.mockito.Mockito.doAnswer(call -> {
            V2TestSupport.reportTasks(queue, Map.of(request.requestId(), TaskPhase.RUNNING), Map.of());
            return true;
        }).when(record).statusExpired(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong());
        queue.removeRequestStatusExpired(System.currentTimeMillis(), 1000L, reporter);
        assertEquals(0, queue.dispatchedCount());
        assertEquals(1L, queue.inflightCount());
        assertFalse(queue.removeDispatched(request));
    }

    @Test
    void delayedReleaseDoesNotRemoveReplacementClaim() {
        var tasks = new PrefillActiveTasks();
        Request request = request("replacement");
        var old = new DispatchedRequest(request, 10L, 5L);
        var replacement = new DispatchedRequest(request, 10L, 5L);
        assertEquals(null, tasks.tryAcquire(old, 1));
        tasks.release(old);
        assertEquals(null, tasks.tryAcquire(replacement, 1));
        tasks.release(old);
        tasks.releaseExpired(old, System.currentTimeMillis(), 1000L);
        assertEquals(1L, tasks.inflightCount());
        tasks.release(replacement);
        assertEquals(0L, tasks.inflightCount());
    }

    @Test
    void finishedOnlyReportReleasesClaimWithoutConfirmationTimeout() {
        Request request = request("finished-only");
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        publish(request);
        assertEquals(null, queue.tryAcquirePublishSlot(request, 1));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));

        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        queue.removeRequestStatusExpired(request.publishedAtMs() + 1_000L, 1_000L, reporter);
        assertEquals(0, queue.dispatchedCount());
        verifyNoInteractions(reporter);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void waitingRequestExpiresWhenItsStatusStopsUpdating() {
        Request request = request("waiting");
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
        publish(request);
        assertEquals(null, queue.tryAcquirePublishSlot(request, 1));
        V2TestSupport.reportTasks(queue, Map.of("waiting", TaskPhase.PENDING), Map.of());
        queue.removeRequestStatusExpired(System.currentTimeMillis(), 1000L, reporter);
        assertEquals(1, queue.dispatchedCount());
        verifyNoInteractions(reporter);
        assertEquals(1L, queue.inflightCount());
        queue.removeRequestStatusExpired(System.currentTimeMillis() + 10_000L, 1000L, reporter);
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        verify(reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
        queue.removeRequestStatusExpired(System.currentTimeMillis() + 20_000L, 1000L, reporter);
        verifyNoMoreInteractions(reporter);
    }

    @Test
    void inflightLimitCountsDispatchedAndUnknownEngineRequests() {
        Request published = request("1");
        V2TestSupport.dispatch(queue, new DispatchedRequest(published, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(published, 4));
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L, Map.of("other", TaskPhase.RUNNING));

        assertEquals(2L, queue.inflightCount());
        assertEquals(1L, queue.unknownRequestCount());
        assertTrue(queue.canDispatch(10L, 3));
        assertFalse(queue.canDispatch(10L, 2));
    }

    @Test
    void kvCheckSubtractsOnlyRequestsTheEngineHasNotReported() {
        Request published = request("1");
        V2TestSupport.dispatch(queue, new DispatchedRequest(published, 60L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(published, 4));
        V2TestSupport.report(endpoint, 1L, 100L, 1_000L, Map.of());

        assertTrue(queue.canDispatch(40L, 10));
        assertFalse(queue.canDispatch(41L, 10));

        V2TestSupport.report(endpoint, 2L, 100L, 1_000L, Map.of("1", TaskPhase.PENDING));

        assertEquals(1L, queue.inflightCount());
        assertTrue(queue.canDispatch(100L, 10));
    }

    @Test
    void emptyMachineAcceptsRequestLargerThanAvailableKv() {
        V2TestSupport.report(endpoint, 1L, 10L, 1_000L, Map.of());

        assertTrue(queue.canDispatch(500L, 10));
    }

    @Test
    void missingActiveReportReleasesClaimAndLateFinishedDoesNotReleaseAgain() {
        Request reported = request("1");
        Request notReported = request("2");
        V2TestSupport.dispatch(queue, new DispatchedRequest(reported, 10L, 5L));
        V2TestSupport.dispatch(queue, new DispatchedRequest(notReported, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(reported, 1));
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L, Map.of("1", TaskPhase.RUNNING));

        V2TestSupport.report(endpoint, 2L, 1_000L, 1_000L, Map.of());

        assertEquals(1, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        assertFalse(queue.removeDispatched(reported));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of("1", TaskPhase.RUNNING));
        assertEquals(1, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        queue.removeRequestStatusExpired(System.currentTimeMillis() + 10_000L, 1000L, reporter);
        verifyNoInteractions(reporter);
        assertTrue(queue.removeDispatched(notReported));
    }

    @Test
    void publishedRecordNotReportedByEngineIsRemovedAfterConfirmTimeout() {
        Request unpublished = request("1");
        Request reported = request("2");
        Request notReported = request("3");
        V2TestSupport.dispatch(queue, new DispatchedRequest(unpublished, 10L, 5L));
        DispatchedRequest record = new DispatchedRequest(reported, 10L, 5L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.dispatch(queue, new DispatchedRequest(notReported, 10L, 5L));
        publish(reported);
        publish(notReported);
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L, Map.of("2", TaskPhase.RUNNING));
        long publishedAtMs = notReported.publishedAtMs();
        record.updateStatus(WorkView.Phase.ENGINE_RUNNING, publishedAtMs + 50L);

        queue.removeRequestStatusExpired(publishedAtMs + 79L, 80L, reporter);
        assertEquals(3, queue.dispatchedCount());

        queue.removeRequestStatusExpired(publishedAtMs + 80L, 80L, reporter);
        assertEquals(2, queue.dispatchedCount());
        assertFalse(queue.removeDispatched(notReported));
        queue.removeRequestStatusExpired(publishedAtMs + 129L, 80L, reporter);
        assertEquals(2, queue.dispatchedCount());
        queue.removeRequestStatusExpired(publishedAtMs + 130L, 80L, reporter);
        assertEquals(1, queue.dispatchedCount());
        assertFalse(queue.removeDispatched(reported));
        assertTrue(queue.removeDispatched(unpublished));
    }

    @Test
    void decodeWaitingCandidatesDoNotConsumeActiveTaskLimit() {
        for (int i = 0; i < 20; i++) {
            V2TestSupport.dispatch(queue, new DispatchedRequest(request("candidate-" + i), 10L, 5L));
        }
        assertTrue(queue.hasDispatchSlot(4));
        Request first = request("first");
        V2TestSupport.dispatch(queue, new DispatchedRequest(first, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(first, 1));
        assertFalse(queue.hasDispatchSlot(1));
        queue.releasePublishSlot(first);
        assertTrue(queue.hasDispatchSlot(1));
    }

    @Test
    void engineAcknowledgementDoesNotDoubleCountPublishedTask() {
        Request first = request("first");
        V2TestSupport.dispatch(queue, new DispatchedRequest(first, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(first, 4));
        V2TestSupport.report(endpoint, 1L, 1000L, 1000L,
                Map.of("first", TaskPhase.RUNNING, "external", TaskPhase.PENDING));
        assertTrue(queue.hasDispatchSlot(3));
        assertFalse(queue.hasDispatchSlot(2));
        V2TestSupport.report(endpoint, 2L, 1000L, 1000L, Map.of());
        assertTrue(queue.hasDispatchSlot(1));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of("first", TaskPhase.RUNNING));
        assertTrue(queue.hasDispatchSlot(1));
    }

    @Test
    void concurrentAcquisitionsCannotExceedFourActiveTasks() throws Exception {
        var workers = java.util.concurrent.Executors.newFixedThreadPool(16);
        var start = new java.util.concurrent.CountDownLatch(1);
        var successes = new java.util.concurrent.atomic.AtomicInteger();
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        try {
            for (int i = 0; i < 16; i++) {
                Request request = request("concurrent-" + i);
                V2TestSupport.dispatch(queue, new DispatchedRequest(request, 10L, 5L));
                futures.add(workers.submit(() -> {
                    start.await();
                    if (queue.tryAcquirePublishSlot(request, 4) == null) {
                        successes.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var future : futures) {
                future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
            assertEquals(4, successes.get());
            assertFalse(queue.hasDispatchSlot(4));
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void loadViewHasWaitingItemsDispatchedWorkAndUnknownCount() {
        Request waiting = request("w");
        waiting.setQueueSeq(1L);
        V2TestSupport.choose(waiting, endpoint, 4L, 20L);
        waiting.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        queue.addWaiting(waiting);
        Request dispatched = request("d");
        dispatched.setQueueSeq(2L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(dispatched, 10L, 30L));
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L, Map.of("d", TaskPhase.RUNNING, "x", TaskPhase.PENDING));
        GroupPlanner.Constraints constraints = new GroupPlanner.Constraints(1, Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L);
        Comparator<GroupPlanner.Item> order = Comparator.comparingLong(GroupPlanner.Item::enqueueSeq);

        PrefillTtftEstimate.LoadView view = queue.loadView(1_000L, order, constraints);

        assertEquals(List.of("w"), view.queue().activeItems().stream().map(GroupPlanner.Item::requestId).toList());
        assertEquals(4L, view.queue().activeItems().getFirst().hitCache());
        assertEquals(List.of(new WorkView.RequestWork("d", WorkView.Phase.ENGINE_RUNNING, 30L)),
                view.work().requests());
        assertEquals(1L, view.work().unknownRequestCount());

        queue.removeWaiting(waiting);
        assertNotEquals(view.ownershipVersion(), queue.loadView(1_000L, order, constraints).ownershipVersion());
    }

    private static void publish(Request request) {
        request.publish(Stage.DECODE_WAITING, List.of());
    }

    @Test
    void predictedWorkCountsRequestOnceWhileItMovesFromWaitingToDispatched() {
        Request earlier = request("1");
        earlier.setQueueSeq(1L);
        V2TestSupport.choose(earlier, endpoint, 0L, 10L);
        earlier.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        queue.addWaiting(earlier);
        assertEquals(25L, queue.estimatedWorkMs(15L));

        queue.addDispatched(new DispatchedRequest(earlier, 16L, 10L));
        assertEquals(25L, queue.estimatedWorkMs(15L));
        earlier.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        queue.removeWaiting(earlier);
        assertEquals(25L, queue.estimatedWorkMs(15L));
        earlier.publish(Stage.DECODE_WAITING, List.of());
        assertEquals(25L, queue.estimatedWorkMs(15L));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of("1", TaskPhase.RUNNING));
        assertEquals(15L, queue.estimatedWorkMs(15L));
    }

    @Test
    void validActualCacheHitCorrectsUnderestimatedWorkBeforeNextSelection() {
        useFormula("200 + 0.069*sum(computeTokens) + 0.0001*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "long-prefill", 50, 610_831L);
        DispatchedRequest record = new DispatchedRequest(request, 2304L, 279L);
        V2TestSupport.dispatch(queue, record);
        var originalChoice = request.selectedPrefill();
        assertEquals(651L, queue.estimatedWorkMs(372L));

        var task = cacheTask(request.requestId(), true, 2304L, null, TaskPhase.RUNNING);
        reportTask(task);

        assertEquals(42_188L, record.predictedMs());
        assertEquals(42_560L, queue.estimatedWorkMs(372L));
        assertEquals(1L, queue.inflightCount());
        assertEquals(2304L, record.kvTokens());
        assertEquals(originalChoice, request.selectedPrefill());
        assertEquals(279L, request.selectedPrefill().predictedMs());
        var order = Comparator.comparingLong(GroupPlanner.Item::enqueueSeq);
        var constraints = new GroupPlanner.Constraints(1, Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L);
        assertEquals(42_188L, queue.loadView(0L, order, constraints).work().requests().getFirst().remainingWorkMs());

        reportTask(task);
        assertEquals(42_560L, queue.estimatedWorkMs(372L));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(372L, queue.estimatedWorkMs(372L));
    }

    @Test
    void actualCacheHitAdjustsWorkInBothDirectionsIncludingZeroHit() {
        useFormula("100 + sum(computeTokens)");
        DispatchedRequest record = new DispatchedRequest(request("cache-correction"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(record.request().requestId(), true, 8L, null, TaskPhase.PENDING));
        assertEquals(108L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), true, 12L, null, TaskPhase.RUNNING));
        assertEquals(104L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(116L, queue.estimatedWorkMs(0L));

        assertTrue(queue.removeDispatched(record.request()));
        assertFalse(queue.removeDispatched(record.request()));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void runningProgressUsesRemainingComputeWithoutCountingCompletedTokensAsCache() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "running-progress", 50, 1000L);
        DispatchedRequest record = new DispatchedRequest(request, 800L, 900L);
        V2TestSupport.dispatch(queue, record);
        var firstStep = cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING);
        when(firstStep.telemetry().completedPrefillTokens()).thenReturn(300L);
        reportTask(firstStep);
        assertEquals(1000L, queue.estimatedWorkMs(0L));
        reportTask(firstStep);
        assertEquals(1000L, queue.estimatedWorkMs(0L));

        reportTask(cacheTask(request.requestId(), true, 200L, 200L, TaskPhase.RUNNING));
        assertEquals(700L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.RUNNING));
        assertEquals(0L, queue.estimatedWorkMs(0L));
        assertEquals(1, queue.dispatchedCount());
        assertEquals(1L, queue.inflightCount());

        assertTrue(queue.removeDispatched(request));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void missingRemainingTokensFallBackToActualUncachedWork() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "missing-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        reportTask(cacheTask(request.requestId(), true, 200L, null, TaskPhase.RUNNING));
        assertEquals(1300L, queue.estimatedWorkMs(0L));
    }

    @Test
    void workerStatusWireAndEventDeliveryDistinguishMissingRemainingTokensFromZero() throws Exception {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "wire-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        var owner = WorkerStatus.createDiscovered(RoleType.PREFILL, "default", "p1", 8000, 8001, "test");
        var queues = mock(PrefillQueueRegistry.class);
        when(queues.find(endpoint)).thenReturn(queue);
        var events = new EndpointEventHandler(mock(RequestRegistry.class), queues);
        var task = EngineRpcService.TaskInfoPB.newBuilder()
                .setRequestIdStr(request.requestId())
                .setPhase(EngineRpcService.TaskPhase.TASK_PHASE_RUNNING)
                .setInputLength(1000L)
                .setPrefixLengthValid(true)
                .setPrefixLength(200L);
        var status = EngineRpcService.WorkerStatusPB.newBuilder()
                .setRole("RoleType.PREFILL")
                .setAlive(true)
                .addRunningTaskInfo(task);

        var missing = EngineStatusConverter.convertToStatusObservation(owner,
                EngineRpcService.WorkerStatusPB.parseFrom(status.build().toByteArray()));
        assertEquals(null, missing.activeTasks().get(request.requestId()).telemetry().remainingPrefillTokens());
        events.onPrefillStatus(endpoint, missing, List.of());
        assertEquals(1300L, queue.estimatedWorkMs(0L));

        status.setRunningTaskInfo(0, task.setRemainingPrefillTokens(0L));
        var completed = EngineStatusConverter.convertToStatusObservation(owner,
                EngineRpcService.WorkerStatusPB.parseFrom(status.build().toByteArray()));
        assertEquals(0L, completed.activeTasks().get(request.requestId()).telemetry().remainingPrefillTokens());
        events.onPrefillStatus(endpoint, completed, List.of());
        assertEquals(0L, queue.estimatedWorkMs(0L));
        assertEquals(1L, queue.inflightCount());
    }

    @Test
    void invalidCacheHitKeepsCorrectedWorkAndWaitingIgnoresRunningProgress() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "preempted-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.RUNNING));
        assertEquals(0L, queue.estimatedWorkMs(0L));

        reportTask(cacheTask(request.requestId(), false, 200L, 0L, TaskPhase.PENDING));
        assertEquals(0L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.PENDING));
        assertEquals(1300L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING));
        assertEquals(1000L, queue.estimatedWorkMs(0L));
    }

    @Test
    void unresolvedCacheHitKeepsPredictionWithoutCallingPredictor() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        DispatchedRequest record = new DispatchedRequest(request("invalid-cache"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        reportTask(cacheTask(record.request().requestId(), false, 0L, 0L, TaskPhase.RUNNING));
        var missingTelemetry = cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING);
        when(missingTelemetry.telemetry()).thenReturn(null);
        reportTask(missingTelemetry);

        assertEquals(10L, queue.estimatedWorkMs(0L));
        verifyNoInteractions(predictor);
    }

    @Test
    void correctionUsesEngineInputLengthWhenItIsReported() {
        useFormula("sum(computeTokens)");
        Request request = V2TestSupport.request(config, "engine-input", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 10L));
        var task = cacheTask(request.requestId(), true, 200L, null, TaskPhase.RUNNING);
        when(task.inputLength()).thenReturn(996L);
        reportTask(task);
        assertEquals(796L, queue.estimatedWorkMs(0L));
    }

    @Test
    void actualCacheHitKeepsEngineReportedOneTokenOfCompute() {
        useFormula("100 + sum(computeTokens)");
        Request request = request("full-hit");
        request.ctx().getRequest().setBlockSize(4L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 16L, 10L));
        var task = cacheTask(request.requestId(), true, 15L, null, TaskPhase.RUNNING);
        reportTask(task);
        assertEquals(101L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 15L, 0L, TaskPhase.RUNNING));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void correctedWorkIsRemovedOnceWhenStatusExpires() {
        useFormula("100 + sum(computeTokens)");
        Request request = request("corrected-expiry");
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 16L, 10L));
        publish(request);
        reportTask(cacheTask(request.requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(116L, queue.estimatedWorkMs(0L));
        queue.removeRequestStatusExpired(System.currentTimeMillis() + 10_000L, 1000L, reporter);
        queue.removeRequestStatusExpired(System.currentTimeMillis() + 20_000L, 1000L, reporter);
        assertEquals(0L, queue.estimatedWorkMs(0L));
        assertEquals(0, queue.dispatchedCount());
        verify(reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
    }

    @Test
    void failedPredictionKeepsWorkAndDoesNotInterruptStatusUpdates() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 0L)).thenReturn(-1L);
        DispatchedRequest record = new DispatchedRequest(request("prediction-failure"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(10L, queue.estimatedWorkMs(0L));
        assertTrue(record.seenByEngine());
        assertEquals(1L, queue.inflightCount());

        useFormula("100 + sum(computeTokens)");
        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(116L, queue.estimatedWorkMs(0L));
    }

    @Test
    void cleanupCanRemoveRecordWhilePredictionRunsWithoutRestoringItsWork() throws Exception {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        var predictionStarted = new java.util.concurrent.CountDownLatch(1);
        var finishPrediction = new java.util.concurrent.CountDownLatch(1);
        when(evaluator.estimateMs(16L, 0L)).thenAnswer(call -> {
            predictionStarted.countDown();
            assertTrue(finishPrediction.await(2, java.util.concurrent.TimeUnit.SECONDS));
            return 116L;
        });
        DispatchedRequest record = new DispatchedRequest(request("concurrent-correction-cleanup"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var update = executor.submit(
                    () -> reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING)));
            try {
                assertTrue(predictionStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));
                assertTrue(queue.removeDispatched(record.request()));
            } finally {
                finishPrediction.countDown();
            }
            update.get(2, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertEquals(0, queue.dispatchedCount());
        assertEquals(10L, record.predictedMs());
        V2TestSupport.reportTasks(queue, Map.of(), Map.of());
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void predictedWorkDoesNotInventDurationForUnknownEngineWork() {
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L, Map.of("other", TaskPhase.RUNNING));
        assertEquals(-1L, queue.estimatedWorkMs(15L));
    }

    @Test
    void predictedWorkIncludesLowerPriorityWaitingWorkWithoutChangingQueueOrder() {
        PrefillQueue priorityQueue = new PrefillQueue(endpoint, true);
        Request low = V2TestSupport.request(config, "low", 10, 16L);
        low.setQueueSeq(1L);
        V2TestSupport.choose(low, endpoint, 0L, 100L);
        low.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        priorityQueue.addWaiting(low);
        Request high = V2TestSupport.request(config, "high", 80, 16L);
        high.setQueueSeq(2L);
        V2TestSupport.choose(high, endpoint, 0L, 15L);
        high.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        priorityQueue.addWaiting(high);

        assertEquals("high", priorityQueue.waitingIterator().next().requestId());
        assertEquals(130L, priorityQueue.estimatedWorkMs(15L));
        priorityQueue.addDispatched(new DispatchedRequest(low, 16L, 100L));
        assertEquals(130L, priorityQueue.estimatedWorkMs(15L));
    }

    @Test
    void duplicateWaitingAndRemovalDoNotChangePredictedWorkTwice() {
        Request waiting = addWaiting("waiting", 10L);
        assertEquals(10L, waiting.selectedPrefill().predictedMs());
        queue.addWaiting(waiting);
        assertEquals(10L, waiting.selectedPrefill().predictedMs());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        queue.removeWaiting(waiting);
        waiting.setSelectedPrefill(null);
        queue.removeWaiting(waiting);
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void returnedRequestUsesNewPredictionWhenItEntersQueueAgain() {
        Request waiting = addWaiting("returned", 10L);
        queue.removeWaiting(waiting);
        V2TestSupport.choose(waiting, endpoint, 0L, 30L);
        queue.addWaiting(waiting);
        assertEquals(30L, waiting.selectedPrefill().predictedMs());
        assertEquals(30L, queue.estimatedWorkMs(0L));
    }

    @Test
    void zeroPredictedWorkStillTracksTransferAndRemoval() {
        Request waiting = addWaiting("zero-work", 0L);
        assertEquals(1, queue.waitingCount());
        assertEquals(0L, waiting.selectedPrefill().predictedMs());
        queue.addDispatched(new DispatchedRequest(waiting, 16L, 0L));
        queue.removeWaiting(waiting);
        assertEquals(1, queue.dispatchedCount());
        assertEquals(15L, queue.estimatedWorkMs(15L));
        assertTrue(queue.removeDispatched(waiting));
        assertFalse(queue.removeDispatched(waiting));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void removingDispatchedDuringTransferLeavesOnlyWaitingPrediction() {
        Request waiting = addWaiting("transfer", 10L);
        queue.addDispatched(new DispatchedRequest(waiting, 16L, 10L));
        assertEquals(10L, queue.estimatedWorkMs(0L));
        assertTrue(queue.removeDispatched(waiting));
        assertEquals(10L, queue.estimatedWorkMs(0L));
        queue.removeWaiting(waiting);
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void cancelledRequestDoesNotAddDispatchedRecordAfterWaitingCleanup() {
        Request waiting = addWaiting("cancelled-transfer", 10L);
        assertTrue(waiting.moveTo(Stage.PREFILL_WAITING, Stage.CANCELLED));
        queue.removeWaiting(waiting);
        queue.addDispatched(new DispatchedRequest(waiting, 16L, 10L));
        assertEquals(0, queue.dispatchedCount());
        assertFalse(queue.removeDispatched(waiting));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void duplicateDispatchedAddsCountSameRequestOnce() {
        Request waiting = addWaiting("same-request", 10L);
        DispatchedRequest record = new DispatchedRequest(waiting, 16L, 10L);
        queue.addDispatched(record);
        queue.addDispatched(record);
        queue.addDispatched(new DispatchedRequest(waiting, 16L, 10L));
        assertEquals(1, queue.dispatchedCount());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        queue.removeWaiting(waiting);
        assertEquals(10L, queue.estimatedWorkMs(0L));
        assertTrue(queue.removeDispatched(waiting));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void predictedWorkAccumulatesManyRequestsAndReturnsToZero() {
        var requests = new java.util.ArrayList<Request>();
        for (int i = 0; i < 1000; i++) {
            requests.add(addWaiting("large-queue-" + i, 3_600_000L));
        }
        assertEquals(3_600_000_015L, queue.estimatedWorkMs(15L));
        for (Request request : requests) {
            queue.addDispatched(new DispatchedRequest(request, 16L, request.selectedPrefill().predictedMs()));
            queue.removeWaiting(request);
        }
        assertEquals(3_600_000_000L, queue.estimatedWorkMs(0L));
        for (Request request : requests) {
            queue.removeDispatched(request);
        }
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void readingPredictedWorkDoesNotVisitDispatchedRecords() {
        DispatchedRequest record = org.mockito.Mockito.spy(new DispatchedRequest(request("record"), 16L, 10L));
        V2TestSupport.dispatch(queue, record);
        org.mockito.Mockito.clearInvocations(record);
        assertEquals(25L, queue.estimatedWorkMs(15L));
        verifyNoInteractions(record);
    }

    @Test
    void statusExpiryRemovesPredictedWorkOnce() {
        Request expired = request("expired-work");
        V2TestSupport.dispatch(queue, new DispatchedRequest(expired, 16L, 10L));
        publish(expired);
        assertEquals(10L, queue.estimatedWorkMs(0L));
        queue.removeRequestStatusExpired(expired.publishedAtMs() + 1000L, 1000L, reporter);
        queue.removeRequestStatusExpired(expired.publishedAtMs() + 2000L, 1000L, reporter);
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void concurrentTransfersAndDeletesLeaveNoPredictedWork() throws Exception {
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(8)) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 100; i++) {
                String id = "work-" + i;
                tasks.add(executor.submit(() -> {
                    Request waiting = addWaiting(id, 10L);
                    queue.addDispatched(new DispatchedRequest(waiting, 16L, 10L));
                    queue.removeWaiting(waiting);
                    queue.removeDispatched(waiting);
                }));
            }
            for (var task : tasks) {
                task.get(2, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
        assertEquals(0, queue.waitingCount());
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    private Request addWaiting(String id, long predictedMs) {
        Request request = request(id);
        request.setQueueSeq(nextQueueSeq.incrementAndGet());
        V2TestSupport.choose(request, endpoint, 0L, predictedMs);
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        queue.addWaiting(request);
        return request;
    }

    private void useFormula(String expression) {
        when(endpoint.getPredictor()).thenReturn(new FormulaPredictor(expression));
    }

    private WorkerStatus.TaskObservation cacheTask(String requestId,
                                                   boolean valid,
                                                   long hitTokens,
                                                   Long remainingTokens,
                                                   TaskPhase phase) {
        var telemetry = mock(WorkerStatus.TaskTelemetry.class);
        when(telemetry.prefixLengthValid()).thenReturn(valid);
        when(telemetry.remainingPrefillTokens()).thenReturn(remainingTokens);
        var task = mock(WorkerStatus.TaskObservation.class);
        when(task.requestId()).thenReturn(requestId);
        when(task.phase()).thenReturn(phase);
        when(task.prefixLength()).thenReturn(hitTokens);
        when(task.telemetry()).thenReturn(telemetry);
        return task;
    }

    private void reportTask(WorkerStatus.TaskObservation task) {
        var tasks = Map.of(task.requestId(), task);
        var engine = new WorkerStatus.EngineObservation(RoleType.PREFILL, null, 1000L, 1000L, tasks, 0.0, 0L, 1L, 1L,
                0L, 0L, 0L, 0L, 0L);
        var observation = mock(WorkerStatus.StatusObservation.class);
        when(observation.engine()).thenReturn(engine);
        when(observation.finishedTasks()).thenReturn(Map.of());
        queue.handlePrefillStatus(observation);
    }

    private Request request(String requestId) {
        Request request = V2TestSupport.request(config, requestId, 50, 16L);
        request.setQueueSeq(nextQueueSeq.incrementAndGet());
        return request;
    }
}

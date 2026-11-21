package org.flexlb.balance.schedulev2.prefill;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.prediction.FormulaPredictor;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class PrefillQueueTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final RequestSchedulerReporter reporter = mock(RequestSchedulerReporter.class);
    private final PrefillEndpoint endpoint = V2TestSupport.prefillEndpoint("p1:8000", reporter);
    private final PrefillQueue queue = endpoint.prefillQueue();
    private final java.util.concurrent.atomic.AtomicLong nextQueueSeq = new java.util.concurrent.atomic.AtomicLong();

    @Test
    void cleanupDoesNotWaitForQueueMonitor() throws Exception {
        Request normal = request("normal");
        Request expired = request("expired");
        V2TestSupport.dispatch(queue, new DispatchedRequest(normal, 10L, 5L));
        V2TestSupport.dispatch(queue, new DispatchedRequest(expired, 10L, 5L));
        publish(expired);
        org.springframework.test.util.ReflectionTestUtils.setField(expired, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        assertEquals(null, queue.tryAcquirePublishSlot(normal, 2));
        assertEquals(null, queue.tryAcquirePublishSlot(expired, 2));
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            synchronized (queue) {
                assertTrue(executor.submit(() -> queue.removeDispatched(normal))
                        .get(2, java.util.concurrent.TimeUnit.SECONDS));
                executor.submit(() -> V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L))
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
            org.mockito.Mockito.doCallRealMethod()
                    .when(record)
                    .statusExpired(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong());
            V2TestSupport.reportTasks(queue, Map.of(request.requestId(), TaskPhase.RUNNING), Map.of());
            return true;
        }).when(record).statusExpired(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong());
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
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
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
        assertEquals(0, queue.dispatchedCount());
        verifyNoInteractions(reporter);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void waitingRequestExpiresWhenItsStatusStopsUpdating() {
        Request request = request("waiting");
        DispatchedRequest dispatchedRequest = new DispatchedRequest(request, 10L, 5L);
        V2TestSupport.dispatch(queue, dispatchedRequest);
        publish(request);
        assertEquals(null, queue.tryAcquirePublishSlot(request, 1));
        V2TestSupport.reportTasks(queue, Map.of("waiting", TaskPhase.PENDING), Map.of());
        V2TestSupport.reportTasks(queue, Map.of("waiting", TaskPhase.PENDING), Map.of(), 1000L);
        assertEquals(1, queue.dispatchedCount());
        verifyNoInteractions(reporter);
        assertEquals(1L, queue.inflightCount());
        long expiredAtMs = System.currentTimeMillis() - 10_000L;
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs", expiredAtMs);
        org.springframework.test.util.ReflectionTestUtils.setField(dispatchedRequest, "lastStatusUpdatedAtMs",
                expiredAtMs);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        verify(reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
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
        Request incoming = enqueue("incoming", 10L);
        assertEquals(QueueBlockReason.PREFILL_INFLIGHT_FULL, queue.tryReserveForDispatch(incoming, 2));
        assertNull(queue.tryReserveForDispatch(incoming, 3));
    }

    @Test
    void kvCheckSubtractsOnlyRequestsTheEngineHasNotReported() {
        Request published = request("1");
        V2TestSupport.dispatch(queue, new DispatchedRequest(published, 60L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(published, 4));
        V2TestSupport.report(endpoint, 1L, 100L, 1_000L, Map.of());

        Request overBudget = enqueue("over-budget", 41L);
        Request fits = enqueue("fits", 40L);
        assertEquals(QueueBlockReason.PREFILL_KV_FULL, queue.tryReserveForDispatch(overBudget, 10));
        assertNull(queue.tryReserveForDispatch(fits, 10));
        assertTrue(queue.removeDispatched(fits));

        V2TestSupport.report(endpoint, 2L, 100L, 1_000L, Map.of("1", TaskPhase.PENDING));

        assertEquals(1L, queue.inflightCount());
        assertNull(queue.tryReserveForDispatch(enqueue("full-budget", 100L), 10));
    }

    @Test
    void emptyMachineAcceptsRequestLargerThanAvailableKv() {
        V2TestSupport.report(endpoint, 1L, 10L, 1_000L, Map.of());

        assertNull(queue.tryReserveForDispatch(enqueue("large", 500L), 10));
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
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
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
        record.updateStatus(DispatchedRequest.Phase.ENGINE_RUNNING, publishedAtMs + 50L);

        assertFalse(record.statusExpired(publishedAtMs + 129L, 80L));
        assertTrue(record.statusExpired(publishedAtMs + 130L, 80L));
        org.springframework.test.util.ReflectionTestUtils.setField(notReported, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        V2TestSupport.reportTasks(queue, Map.of("2", TaskPhase.RUNNING), Map.of(), 1000L);
        assertEquals(2, queue.dispatchedCount());
        assertFalse(queue.removeDispatched(notReported));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
        assertEquals(1, queue.dispatchedCount());
        assertFalse(queue.removeDispatched(reported));
        assertTrue(queue.removeDispatched(unpublished));
    }

    @Test
    void recordingDispatchDoesNotTakeSlotUntilReservationSucceeds() {
        Request first = enqueue("first", 10L);
        queue.addDispatched(new DispatchedRequest(first, 10L, 5L));
        assertEquals(0L, queue.inflightCount());
        assertNull(queue.tryAcquirePublishSlot(first, 1));
        assertEquals(1L, queue.inflightCount());
        queue.releasePublishSlot(first);
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void engineAcknowledgementDoesNotDoubleCountPublishedTask() {
        Request first = request("first");
        V2TestSupport.dispatch(queue, new DispatchedRequest(first, 10L, 5L));
        assertEquals(null, queue.tryAcquirePublishSlot(first, 4));
        V2TestSupport.report(endpoint, 1L, 1000L, 1000L,
                Map.of("first", TaskPhase.RUNNING, "external", TaskPhase.PENDING));
        assertEquals(2L, queue.inflightCount());
        V2TestSupport.report(endpoint, 2L, 1000L, 1000L, Map.of());
        assertEquals(0L, queue.inflightCount());
        V2TestSupport.reportTasks(queue, Map.of(), Map.of("first", TaskPhase.RUNNING));
        assertEquals(0L, queue.inflightCount());
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
            assertEquals(4L, queue.inflightCount());
        } finally {
            workers.shutdownNow();
        }
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

        assertEquals(42_188L, record.getCorrectedTotalMs());
        assertEquals(41_988L, record.predictedMs());
        assertEquals(42_360L, queue.estimatedWorkMs(372L));
        assertEquals(1L, queue.inflightCount());
        assertEquals(2304L, record.kvTokens());
        assertEquals(originalChoice, request.selectedPrefill());
        assertEquals(279L, request.selectedPrefill().predictedMs());
        assertEquals(41_988L, queue.dispatchedWorkMs().orElseThrow());

        reportTask(task);
        assertEquals(42_360L, queue.estimatedWorkMs(372L));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(372L, queue.estimatedWorkMs(372L));
    }

    @Test
    void unchangedActualCacheHitSkipsRepeatedPrediction() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 8L, 8L)).thenReturn(110L);
        when(evaluator.estimateMs(16L, 0L, 8L)).thenReturn(100L);
        Request request = request("matching-cache");
        DispatchedRequest record = new DispatchedRequest(request, 8L, 10L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.choose(request, endpoint, 8L, 10L);

        reportTask(cacheTask(request.requestId(), true, 8L, null, TaskPhase.RUNNING));
        reportTask(cacheTask(request.requestId(), true, 8L, null, TaskPhase.RUNNING));

        assertEquals(10L, record.predictedMs());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        assertEquals(1L, queue.inflightCount());
        verify(evaluator).estimateMs(16L, 8L, 8L);
        verify(evaluator).estimateMs(16L, 0L, 8L);
        verifyNoMoreInteractions(evaluator);
    }

    @Test
    void zeroCacheHitKeepsWorkWhenPredictionMatches() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 16L, 0L)).thenReturn(110L);
        when(evaluator.estimateMs(16L, 0L, 0L)).thenReturn(100L);
        DispatchedRequest record = new DispatchedRequest(request("matching-zero-cache"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));

        assertEquals(10L, record.predictedMs());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        verify(evaluator).estimateMs(16L, 16L, 0L);
        verify(evaluator).estimateMs(16L, 0L, 0L);
        verifyNoMoreInteractions(evaluator);
    }

    @Test
    void correctedTotalIsComputedOnceAfterCacheBecomesValid() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(10_000L, 8000L, 2000L)).thenReturn(1553L);
        when(evaluator.estimateMs(10_000L, 5000L, 2000L)).thenReturn(1346L);
        when(evaluator.estimateMs(10_000L, 7000L, 2000L)).thenReturn(1484L);
        Request request = V2TestSupport.request(config, "cached-total", 50, 10_000L);
        DispatchedRequest record = new DispatchedRequest(request, 8000L, 1144L);
        V2TestSupport.dispatch(queue, record);

        assertEquals(-1L, record.getCorrectedTotalMs());
        reportTask(cacheTask(request.requestId(), false, 0L, 0L, TaskPhase.PENDING));
        assertEquals(-1L, record.getCorrectedTotalMs());
        assertEquals(1144L, queue.estimatedWorkMs(0L));
        verifyNoInteractions(predictor, evaluator);

        reportTask(cacheTask(request.requestId(), true, 2000L, 3000L, TaskPhase.RUNNING));
        assertEquals(1553L, record.getCorrectedTotalMs());
        assertEquals(207L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 2000L, 1000L, TaskPhase.RUNNING));
        assertEquals(1553L, record.getCorrectedTotalMs());
        assertEquals(69L, queue.estimatedWorkMs(0L));

        verify(evaluator).estimateMs(10_000L, 8000L, 2000L);
        verify(evaluator).estimateMs(10_000L, 5000L, 2000L);
        verify(evaluator).estimateMs(10_000L, 7000L, 2000L);
        verifyNoMoreInteractions(evaluator);
    }

    @Test
    void confirmedCacheAndCompletedComputeRemovePreprocessingAndLoadingCosts() {
        useFormula("1000 + 0.069*sum(computeTokens) + 0.0008*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "completed-cost", 50, 10_000L);
        DispatchedRequest record = new DispatchedRequest(request, 8000L, 1144L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(request.requestId(), true, 2000L, 3000L, TaskPhase.RUNNING));
        assertEquals(1553L, record.getCorrectedTotalMs());
        assertEquals(207L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 2000L, 1000L, TaskPhase.RUNNING));
        assertEquals(69L, queue.estimatedWorkMs(0L));

        assertTrue(queue.removeDispatched(request));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void actualCacheHitAdjustsWorkInBothDirectionsIncludingZeroHit() {
        useFormula("100 + sum(computeTokens)");
        DispatchedRequest record = new DispatchedRequest(request("cache-correction"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.choose(record.request(), endpoint, 8L, 10L);

        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(16L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), false, 0L, null, TaskPhase.PENDING));
        assertEquals(16L, queue.estimatedWorkMs(0L));
        assertEquals(DispatchedRequest.Phase.ENGINE_QUEUED, record.phase());
        reportTask(cacheTask(record.request().requestId(), true, 8L, null, TaskPhase.RUNNING));
        assertEquals(8L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(16L, queue.estimatedWorkMs(0L));

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
        when(firstStep.completedPrefillTokens()).thenReturn(300L);
        reportTask(firstStep);
        assertEquals(500L, queue.estimatedWorkMs(0L));
        reportTask(firstStep);
        assertEquals(500L, queue.estimatedWorkMs(0L));

        reportTask(cacheTask(request.requestId(), true, 200L, 200L, TaskPhase.RUNNING));
        assertEquals(200L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.RUNNING));
        assertEquals(200L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.RUNNING));
        assertEquals(200L, record.predictedMs());
        assertEquals(200L, queue.estimatedWorkMs(0L));
        assertEquals(1, queue.dispatchedCount());
        assertEquals(1L, queue.inflightCount());

        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(0, queue.dispatchedCount());
        assertEquals(0L, queue.inflightCount());
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void initialZeroRemainingTokensFallBackToActualUncachedWork() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "initial-zero-progress", 50, 1000L);
        DispatchedRequest record = new DispatchedRequest(request, 800L, 900L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(request.requestId(), false, 0L, 0L, TaskPhase.PENDING));
        assertEquals(900L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.PENDING));
        assertEquals(800L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 0L, TaskPhase.PENDING));
        assertEquals(800L, record.predictedMs());
        assertEquals(800L, queue.estimatedWorkMs(0L));
    }

    @Test
    void nonPositiveRemainingWorkKeepsPreviousPredictionUntilPositiveUpdateOrFinished() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 16L, 0L)).thenReturn(100L);
        when(evaluator.estimateMs(16L, 8L, 0L)).thenReturn(100L);
        when(evaluator.estimateMs(16L, 12L, 0L)).thenReturn(120L);
        when(evaluator.estimateMs(16L, 14L, 0L)).thenReturn(98L);
        Request request = request("non-positive-work");
        DispatchedRequest record = new DispatchedRequest(request, 16L, 10L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(request.requestId(), true, 0L, 8L, TaskPhase.RUNNING));
        assertEquals(10L, record.predictedMs());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 0L, 4L, TaskPhase.RUNNING));
        assertEquals(10L, record.predictedMs());
        assertEquals(10L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 0L, 2L, TaskPhase.RUNNING));
        assertEquals(2L, record.predictedMs());
        assertEquals(2L, queue.estimatedWorkMs(0L));

        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void missingRemainingTokensFallBackToActualUncachedWork() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "missing-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        reportTask(cacheTask(request.requestId(), true, 200L, null, TaskPhase.RUNNING));
        assertEquals(800L, queue.estimatedWorkMs(0L));
    }

    @Test
    void remainingTokensAboveActualComputeDoNotInflateWork() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "stale-progress", 50, 1000L);
        DispatchedRequest record = new DispatchedRequest(request, 800L, 900L);
        V2TestSupport.dispatch(queue, record);

        // 进度仍是旧缓存命中下的整段 prompt，已完成 token 不能为负
        reportTask(cacheTask(request.requestId(), true, 200L, 1000L, TaskPhase.RUNNING));
        assertEquals(800L, record.predictedMs());
        assertEquals(800L, queue.estimatedWorkMs(0L));

        reportTask(cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING));
        assertEquals(500L, queue.estimatedWorkMs(0L));
    }

    @Test
    void workerStatusWirePreservesMissingAndCompletedZeroRemainingTokens() throws Exception {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "wire-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        var owner = WorkerStatus.createDiscovered(RoleType.PREFILL, "default", "p1", 8000, 8001, "test");
        var queues = mock(PrefillQueueRegistry.class);
        when(queues.find(endpoint)).thenReturn(queue);
        var configService = mock(ConfigService.class);
        when(configService.loadBalanceConfig()).thenReturn(config);
        var events = new EndpointEventHandler(mock(RequestRegistry.class), queues, configService, reporter);
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
        assertEquals(null, missing.activeTasks().get(request.requestId()).remainingPrefillTokens());
        events.onPrefillStatus(endpoint, missing);
        assertEquals(800L, queue.estimatedWorkMs(0L));

        status.setRunningTaskInfo(0, task.setRemainingPrefillTokens(500L));
        var progress = EngineStatusConverter.convertToStatusObservation(owner,
                EngineRpcService.WorkerStatusPB.parseFrom(status.build().toByteArray()));
        events.onPrefillStatus(endpoint, progress);
        assertEquals(500L, queue.estimatedWorkMs(0L));

        status.setRunningTaskInfo(0, task.setRemainingPrefillTokens(0L));
        var zeroRemaining = EngineStatusConverter.convertToStatusObservation(owner,
                EngineRpcService.WorkerStatusPB.parseFrom(status.build().toByteArray()));
        assertEquals(0L, zeroRemaining.activeTasks().get(request.requestId()).remainingPrefillTokens());
        events.onPrefillStatus(endpoint, zeroRemaining);
        assertEquals(500L, queue.estimatedWorkMs(0L));
        assertEquals(1L, queue.inflightCount());
    }

    @Test
    void invalidCacheHitKeepsCorrectedWorkUntilCacheBecomesValid() {
        useFormula("100 + sum(computeTokens) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "preempted-progress", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 900L));
        reportTask(cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING));
        assertEquals(500L, queue.estimatedWorkMs(0L));

        reportTask(cacheTask(request.requestId(), false, 0L, 1000L, TaskPhase.PENDING));
        assertEquals(500L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), false, 0L, 0L, TaskPhase.PENDING));
        assertEquals(500L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 0L, 500L, TaskPhase.RUNNING));
        assertEquals(500L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING));
        assertEquals(500L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 200L, 200L, TaskPhase.RUNNING));
        assertEquals(200L, queue.estimatedWorkMs(0L));

        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(request.requestId(), TaskPhase.RUNNING));
        assertEquals(0L, queue.estimatedWorkMs(0L));
        assertEquals(0L, queue.inflightCount());
    }

    @Test
    void unresolvedCacheHitKeepsPredictionAndStillUpdatesTaskStatus() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 16L, 0L)).thenReturn(116L);
        when(evaluator.estimateMs(16L, 8L, 0L)).thenReturn(108L);
        DispatchedRequest record = new DispatchedRequest(request("invalid-cache"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.choose(record.request(), endpoint, 4L, 10L);
        publish(record.request());

        for (Long remainingTokens : new Long[]{null, 0L, 8L}) {
            reportTask(cacheTask(record.request().requestId(), false, 8L, remainingTokens, TaskPhase.PENDING));
            assertEquals(10L, record.predictedMs());
            assertEquals(10L, queue.estimatedWorkMs(0L));
            assertTrue(record.seenByEngine());
            assertFalse(record.statusExpired(System.currentTimeMillis(), 1000L));
            assertEquals(DispatchedRequest.Phase.ENGINE_QUEUED, record.phase());
            assertEquals(1L, queue.inflightCount());
        }
        verifyNoInteractions(predictor, evaluator);

        reportTask(cacheTask(record.request().requestId(), true, 0L, 8L, TaskPhase.RUNNING));
        assertEquals(8L, record.predictedMs());
        assertEquals(8L, queue.estimatedWorkMs(0L));
        assertEquals(DispatchedRequest.Phase.ENGINE_RUNNING, record.phase());
        verify(evaluator).estimateMs(16L, 16L, 0L);
        verify(evaluator).estimateMs(16L, 8L, 0L);
        verifyNoMoreInteractions(evaluator);
    }

    @Test
    void actualCacheHitChangeReestimatesWorkWithUnchangedRemainingTokens() {
        useFormula("100 + sum(computeTokens^2) + 2*sum(hitCacheTokens)");
        Request request = V2TestSupport.request(config, "changed-prefix", 50, 1000L);
        DispatchedRequest record = new DispatchedRequest(request, 800L, 900L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(request.requestId(), true, 200L, 500L, TaskPhase.RUNNING));
        assertEquals(640_500L, record.getCorrectedTotalMs());
        assertEquals(550_000L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 100L, 500L, TaskPhase.RUNNING));
        assertEquals(810_300L, record.getCorrectedTotalMs());
        assertEquals(650_000L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 0L, 500L, TaskPhase.RUNNING));
        assertEquals(1_000_100L, record.getCorrectedTotalMs());
        assertEquals(750_000L, queue.estimatedWorkMs(0L));

        assertTrue(queue.removeDispatched(request));
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void failedPrefixReestimateDoesNotReusePreviousCorrectedTotal() {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        when(evaluator.estimateMs(16L, 8L, 8L)).thenReturn(124L);
        when(evaluator.estimateMs(16L, 4L, 8L)).thenReturn(120L);
        when(evaluator.estimateMs(16L, 12L, 4L)).thenReturn(-1L, 120L);
        when(evaluator.estimateMs(16L, 10L, 4L)).thenReturn(118L);
        DispatchedRequest record = new DispatchedRequest(request("failed-prefix"), 8L, 10L);
        V2TestSupport.dispatch(queue, record);

        reportTask(cacheTask(record.request().requestId(), true, 8L, 4L, TaskPhase.RUNNING));
        assertEquals(124L, record.getCorrectedTotalMs());
        assertEquals(4L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), true, 4L, 4L, TaskPhase.RUNNING));
        assertEquals(-1L, record.getCorrectedTotalMs());
        assertEquals(4L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(record.request().requestId(), true, 4L, 2L, TaskPhase.RUNNING));
        assertEquals(120L, record.getCorrectedTotalMs());
        assertEquals(2L, queue.estimatedWorkMs(0L));

        verify(evaluator).estimateMs(16L, 8L, 8L);
        verify(evaluator).estimateMs(16L, 4L, 8L);
        verify(evaluator, times(2)).estimateMs(16L, 12L, 4L);
        verify(evaluator).estimateMs(16L, 10L, 4L);
        verifyNoMoreInteractions(evaluator);
    }

    @Test
    void correctionUsesRequestInputLength() {
        useFormula("sum(computeTokens)");
        Request request = V2TestSupport.request(config, "engine-input", 50, 1000L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 800L, 10L));
        var task = cacheTask(request.requestId(), true, 200L, null, TaskPhase.RUNNING);
        when(task.inputLength()).thenReturn(996L);
        reportTask(task);
        assertEquals(800L, queue.estimatedWorkMs(0L));
    }

    @Test
    void actualCacheHitKeepsEngineReportedOneTokenOfCompute() {
        useFormula("100 + sum(computeTokens)");
        Request request = request("full-hit");
        request.ctx().getRequest().setBlockSize(4L);
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 16L, 10L));
        var task = cacheTask(request.requestId(), true, 15L, null, TaskPhase.RUNNING);
        reportTask(task);
        assertEquals(1L, queue.estimatedWorkMs(0L));
        reportTask(cacheTask(request.requestId(), true, 15L, 0L, TaskPhase.RUNNING));
        assertEquals(1L, queue.estimatedWorkMs(0L));
    }

    @Test
    void correctedWorkIsRemovedOnceWhenStatusExpires() {
        useFormula("100 + sum(computeTokens)");
        Request request = request("corrected-expiry");
        V2TestSupport.dispatch(queue, new DispatchedRequest(request, 16L, 10L));
        V2TestSupport.choose(request, endpoint, 8L, 10L);
        publish(request);
        reportTask(cacheTask(request.requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(16L, queue.estimatedWorkMs(0L));
        org.springframework.test.util.ReflectionTestUtils.setField(request, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 0L);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 0L);
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
        when(evaluator.estimateMs(16L, 16L, 0L)).thenReturn(-1L);
        DispatchedRequest record = new DispatchedRequest(request("prediction-failure"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.choose(record.request(), endpoint, 8L, 10L);
        reportTask(cacheTask(record.request().requestId(), true, 0L, null, TaskPhase.RUNNING));
        assertEquals(10L, queue.estimatedWorkMs(0L));
        assertTrue(record.seenByEngine());
        assertEquals(1L, queue.inflightCount());

        useFormula("100 + sum(computeTokens)");
        reportTask(cacheTask(record.request().requestId(), true, 0L, 8L, TaskPhase.RUNNING));
        assertEquals(8L, queue.estimatedWorkMs(0L));
    }

    @Test
    void cleanupCanRemoveRecordWhilePredictionRunsWithoutRestoringItsWork() throws Exception {
        PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
        PrefillTimePredictor.Evaluator evaluator = mock(PrefillTimePredictor.Evaluator.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        when(predictor.evaluator()).thenReturn(evaluator);
        var predictionStarted = new java.util.concurrent.CountDownLatch(1);
        var finishPrediction = new java.util.concurrent.CountDownLatch(1);
        when(evaluator.estimateMs(16L, 16L, 0L)).thenAnswer(call -> {
            predictionStarted.countDown();
            assertTrue(finishPrediction.await(2, java.util.concurrent.TimeUnit.SECONDS));
            return 116L;
        });
        DispatchedRequest record = new DispatchedRequest(request("concurrent-correction-cleanup"), 16L, 10L);
        V2TestSupport.dispatch(queue, record);
        V2TestSupport.choose(record.request(), endpoint, 8L, 10L);
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
        PrefillQueue priorityQueue = new PrefillQueue(endpoint, true, reporter);
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
        org.springframework.test.util.ReflectionTestUtils.setField(expired, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
        V2TestSupport.reportTasks(queue, Map.of(), Map.of(), 1000L);
        assertEquals(0L, queue.estimatedWorkMs(0L));
    }

    @Test
    void statusTimeoutIsReportedEvenWhenRemovalReturnsFalse() {
        Request expired = request("timeout-report");
        V2TestSupport.dispatch(queue, new DispatchedRequest(expired, 16L, 10L));
        publish(expired);
        org.springframework.test.util.ReflectionTestUtils.setField(expired, "publishedAtMs",
                System.currentTimeMillis() - 10_000L);
        PrefillQueue statusQueue = org.mockito.Mockito.spy(queue);
        org.mockito.Mockito.doAnswer(call -> {
            queue.removeDispatched(expired);
            return false;
        }).when(statusQueue).removeDispatched(expired);

        V2TestSupport.reportTasks(statusQueue, Map.of(), Map.of(), 1000L);

        verify(reporter).reportStatusUpdateTimeout(RoleType.PREFILL, endpoint.ipPort());
        assertEquals(0, queue.dispatchedCount());
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
        var task = mock(WorkerStatus.TaskObservation.class);
        when(task.requestId()).thenReturn(requestId);
        when(task.phase()).thenReturn(phase);
        when(task.prefixLength()).thenReturn(hitTokens);
        when(task.prefixLengthValid()).thenReturn(valid);
        when(task.remainingPrefillTokens()).thenReturn(remainingTokens);
        return task;
    }

    private void reportTask(WorkerStatus.TaskObservation task) {
        var tasks = Map.of(task.requestId(), task);
        var engine = new WorkerStatus.EngineObservation(RoleType.PREFILL, null, 1000L, 1000L, tasks, 0.0, 0L, 1L, 1L,
                0L, 0L, 0L, 0L, 0L);
        var observation = mock(WorkerStatus.StatusObservation.class);
        when(observation.engine()).thenReturn(engine);
        when(observation.finishedTasks()).thenReturn(Map.of());
        queue.handlePrefillStatus(observation, Long.MAX_VALUE);
    }

    @Test
    void releaseKeepsPublishedMapsReadOnlyAndDoesNotChangeEarlierSnapshots() {
        PrefillActiveTasks activeTasks = new PrefillActiveTasks();
        DispatchedRequest first = new DispatchedRequest(request("first-snapshot"), 10L, 5L);
        DispatchedRequest second = new DispatchedRequest(request("second-snapshot"), 10L, 5L);
        assertEquals(null, activeTasks.tryAcquire(first, 2));
        assertEquals(null, activeTasks.tryAcquire(second, 2));
        var snapshots =
                (java.util.concurrent.atomic.AtomicReference<?>) org.springframework.test.util.ReflectionTestUtils
                        .getField(activeTasks, "capacitySnapshot");
        PrefillCapacitySnapshot beforeRelease = (PrefillCapacitySnapshot) snapshots.get();

        activeTasks.release(first);
        PrefillCapacitySnapshot afterRelease = (PrefillCapacitySnapshot) snapshots.get();
        assertEquals(2, beforeRelease.localPublishReservations().size());
        assertEquals(Map.of(second.request().requestId(), second), afterRelease.localPublishReservations());
        assertThrows(UnsupportedOperationException.class, () -> afterRelease.localPublishReservations().clear());

        activeTasks.release(second);
        assertEquals(1, afterRelease.localPublishReservations().size());
        assertEquals(0L, activeTasks.inflightCount());
    }

    @Test
    void finishedCleanupUsesCurrentEngineCapacityBeforeNotifyingScheduler() {
        Request finished = request("finished-capacity");
        V2TestSupport.dispatch(queue, new DispatchedRequest(finished, 16L, 10L));
        assertEquals(null, queue.tryAcquirePublishSlot(finished, 1));
        publish(finished);
        Request incoming = request("incoming-capacity");
        V2TestSupport.dispatch(queue, new DispatchedRequest(incoming, 16L, 10L));
        var acquiredSlots = new java.util.concurrent.atomic.AtomicInteger();
        queue.onCapacityChanged(() -> {
            if (queue.tryAcquirePublishSlot(incoming, 1) == null) {
                acquiredSlots.incrementAndGet();
            }
        });

        V2TestSupport.reportTasks(queue, Map.of("external-capacity", TaskPhase.RUNNING),
                Map.of(finished.requestId(), TaskPhase.RUNNING));

        assertEquals(0, acquiredSlots.get());
        assertEquals(1L, queue.inflightCount());
        assertEquals(1, queue.dispatchedCount());
    }

    private Request enqueue(String requestId, long seqLen) {
        Request request = V2TestSupport.request(config, requestId, 50, seqLen);
        request.setQueueSeq(nextQueueSeq.incrementAndGet());
        V2TestSupport.choose(request, endpoint, 0L, 5L);
        assertTrue(request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
        queue.addWaiting(request);
        return request;
    }

    private Request request(String requestId) {
        Request request = V2TestSupport.request(config, requestId, 50, 16L);
        request.setQueueSeq(nextQueueSeq.incrementAndGet());
        return request;
    }
}

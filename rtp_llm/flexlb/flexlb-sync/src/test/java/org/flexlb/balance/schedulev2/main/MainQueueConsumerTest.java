package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.DirectExecutorService;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.RequestPreparePool;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.decode.DecodeQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.RequestWorkLoadEstimateResult;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MainQueueConsumerTest {

    private static final BooleanSupplier RUNNING = () -> true;

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final ConfigService configService = mock(ConfigService.class);
    private final QueueCapacity queueCapacity = new QueueCapacity();
    private final RequestTable requestTable = new RequestTable();
    private final V2QueueReporter reporter = mock(V2QueueReporter.class);
    private final MainQueue mainQueue = new MainQueue(queueCapacity, 2, true, reporter);
    private final PrefillQueueRegistry prefillQueues;
    private final RequestCleanup cleanup;
    private final PrefillEndpoint endpoint = V2TestSupport.prefillEndpoint("p1:8000");
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);
    private final PrefillPicker picker = mock(PrefillPicker.class);
    private final RequestWorkLoadEstimateResult inputs =
            RequestWorkLoadEstimateResult.success(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC), List.of());
    private final List<Request> sent = new ArrayList<>();
    private final MainQueueConsumer loop;

    MainQueueConsumerTest() {
        config.getDispatcher().setType(org.flexlb.config.DispatcherConfig.Type.NON_BATCH_V2);
        when(configService.loadBalanceConfig()).thenReturn(config);
        when(picker.prepare(any(), any(), any())).thenReturn(inputs);
        prefillQueues = new PrefillQueueRegistry();
        V2TestSupport.register(prefillQueues, endpoint);
        cleanup = new RequestCleanup(requestTable, mainQueue, queueCapacity, prefillQueues, new DecodeQueue(false));
        when(endpointRegistry.getEndpointCount(RoleType.PREFILL)).thenReturn(1);
        loop = createConsumer(new RequestPreparePool(new DirectExecutorService(), mainQueue, picker, RoleType.PREFILL));
    }

    private MainQueueConsumer createConsumer(RequestPreparePool workers) {
        return new MainQueueConsumer(mainQueue, cleanup, queueCapacity, endpointRegistry, configService, picker,
                prefillQueues, RoleType.PREFILL, request -> {
                    prefillQueues.addWaiting(request);
                    sent.add(request);
                }, workers, reporter);
    }

    @Test
    void pickedRequestHoldsQueueSlotAndGoesToPrefillQueue() {
        Request request = add("1", "g");
        SelectedPrefill selected = V2TestSupport.selected(endpoint, 4L, 30L);
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(selected);

        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));

        assertEquals(List.of(request), sent);
        assertSame(endpoint, request.selectedPrefill().endpoint());
        assertEquals(4L, request.selectedPrefill().hitCacheTokens());
        assertEquals(30L, request.selectedPrefill().predictedMs());
        assertEquals(Stage.PREFILL_WAITING, request.stage());
        assertEquals(0, mainQueue.size());
        assertEquals(1, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    @Test
    void noPrefillNodesLeaveRequestsQueuedWithoutPreparingOrReporting() {
        when(endpointRegistry.getEndpointCount(RoleType.PREFILL)).thenReturn(0);
        Request request = add("1", "g");

        assertFalse(loop.runOnce(RUNNING));

        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertEquals(1, mainQueue.size());
        assertNull(request.getWorkLoadEstimate());
        verifyNoInteractions(picker, reporter);
    }

    @Test
    void selectionExceptionFailsRequestAndReleasesEverything() {
        Request request = add("1", "g");
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenThrow(new IllegalStateException("cannot score"));

        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));

        assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), request.future().join().getCode());
        assertEquals("prefill selection failed: cannot score", request.future().join().getErrorMessage());
        assertEquals(0, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
        assertEquals(0, mainQueue.size());
    }

    @Test
    void requestCancelledDuringPickDoesNotReserveSlotOrEnterPrefillQueue() {
        Request request = add("1", "g");
        when(picker.pick(eq(request), eq(RoleType.PREFILL), any(), any())).thenAnswer(call -> {
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return V2TestSupport.selected(endpoint, 0L, 10L);
        });

        assertTrue(loop.runOnce(RUNNING));
        loop.runOnce(RUNNING);

        assertTrue(sent.isEmpty());
        assertEquals(0, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    private Request add(String requestId, String group) {
        return add(requestId, group, 50);
    }

    private Request add(String requestId, String group, int priority) {
        Request request = V2TestSupport.request(config, requestId, priority, 16L);
        request.future().whenComplete((response, failure) -> {
            if (request.stage().isEnded()) {
                cleanup.releaseEnded(request);
            }
        });
        requestTable.add(request);
        when(picker.groupOf(request.ctx())).thenReturn(group);
        assertNull(mainQueue.add(request, config));
        return request;
    }

    @Test
    void oneRoundTakesOnlyTheHighestPriorityRequest() {
        Request low = add("1", "g", 10);
        Request high = add("2", "g", 80);

        assertTrue(loop.runOnce(RUNNING));

        assertNull(low.getWorkLoadEstimate());
        assertSame(inputs, high.getWorkLoadEstimate());
        assertSame(high, mainQueue.pollFirst());
        assertSame(low, mainQueue.pollFirst());
    }

    @Test
    void addKeepsTotalCountWhenConsumerHasAlreadyMovedRequestToPrefill() {
        Request request = spy(V2TestSupport.request(config, "1", 50, 16L));
        doAnswer(call -> {
            assertSame(request, mainQueue.pollFirst());
            assertTrue(request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
            return call.callRealMethod();
        }).doCallRealMethod().when(request).stage();

        assertNull(mainQueue.add(request, config));

        assertEquals(Stage.PREFILL_WAITING, request.stage());
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    @Test
    void addBackRemovesARequestThatEndedBetweenItsTwoStageChecks() {
        Request request = spy(V2TestSupport.request(config, "1", 50, 16L));
        doReturn(Stage.MAIN_WAITING, Stage.CANCELLED).when(request).stage();

        mainQueue.addBack(request);

        assertNull(mainQueue.pollFirst());
    }

    @Test
    void fullMainQueueRejectsVictimAndReportsBothPriorities() {
        SchedulingTestConfig.usePriorityQueue(config);
        config.priorityOrdering().setHighPriorityThreshold(50);
        config.getScheduler().setMaxQueuedRequests(1);
        Request low = add("low", "g", 40);
        Request high = add("high", "g", 50);

        assertEquals(Stage.FAILED, low.stage());
        assertEquals(8433, low.future().join().getCode());
        assertEquals(Stage.MAIN_WAITING, high.stage());
        assertEquals(1, mainQueue.size());
        assertSame(high, mainQueue.pollFirst());
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
        verify(reporter).mainPreempted(50, 40);
        org.mockito.Mockito.verifyNoMoreInteractions(reporter);
    }

    @Test
    void fullMainQueueDoesNotEvictRequestAlreadyTakenByConsumer() {
        SchedulingTestConfig.usePriorityQueue(config);
        config.priorityOrdering().setHighPriorityThreshold(50);
        config.getScheduler().setMaxQueuedRequests(1);
        Request low = spy(V2TestSupport.request(config, "low", 40, 16L));
        low.future().whenComplete((response, failure) -> cleanup.releaseEnded(low));
        assertNull(mainQueue.add(low, config));
        doAnswer(call -> {
            assertSame(low, mainQueue.pollFirst());
            return call.callRealMethod();
        }).doCallRealMethod().when(low).stage();
        Request high = V2TestSupport.request(config, "high", 50, 16L);

        assertEquals(StrategyErrorType.QUEUE_FULL_HIGH_PRIORITY_REJECTED, mainQueue.add(high, config));

        assertEquals(Stage.MAIN_WAITING, low.stage());
        assertFalse(low.future().isDone());
        assertEquals(0, mainQueue.size());
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
        verifyNoInteractions(reporter);
    }

    @Test
    void fullSubqueuesOnlyMarkNewestLowestPriorityWhileConsumerWaits() throws Exception {
        SchedulingTestConfig.usePriorityQueue(config);
        config.priorityOrdering().setHighPriorityThreshold(60);
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(2);
        doAnswer(call -> V2TestSupport.selected(endpoint, 0L, 10L)).when(picker).pick(any(), any(), any(), any());
        Request older = add("1", "g", 10);
        Request newer = add("2", "g", 10);
        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));
        Request high = add("3", "g", 80);

        assertTrue(loop.runOnce(RUNNING));
        CountDownLatch blocked = signalOnFullSubqueue(high, endpoint);
        try (ExecutorService consumer = Executors.newSingleThreadExecutor()) {
            var round = consumer.submit(() -> loop.runOnce(RUNNING));
            try {
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                assertFalse(round.isDone());
                assertNull(older.queueProgress().prefillReturnPriority());
                assertEquals(80, newer.queueProgress().prefillReturnPriority());
                assertEquals(Stage.PREFILL_WAITING, newer.stage());
                assertFalse(newer.future().isDone());
                assertEquals(Stage.MAIN_WAITING, high.stage());
                assertEquals(0, mainQueue.size());
                assertEquals(2, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
                assertEquals(3L, queueCapacity.globalQueueAndEngineSubQueueCount());
                org.mockito.Mockito.verify(reporter, org.mockito.Mockito.never())
                        .preemptedReturn(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt(),
                                any());
            } finally {
                high.cancel(CancelReason.DEADLINE_EXCEEDED);
            }
            assertTrue(round.get(5, TimeUnit.SECONDS));
            assertEquals(2, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
            assertEquals(2L, queueCapacity.globalQueueAndEngineSubQueueCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fullSubqueuesDoNotPreemptAnotherPrefillOrDecodeWaiting(boolean lowWaitingForDecode) throws Exception {
        SchedulingTestConfig.usePriorityQueue(config);
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(1);
        Request low = add("1", "g", 10);
        doAnswer(call -> V2TestSupport.selected(endpoint, 0L, 10L)).when(picker)
                .pick(low, RoleType.PREFILL, "g", inputs);
        assertTrue(loop.runOnce(RUNNING));
        assertTrue(loop.runOnce(RUNNING));
        if (lowWaitingForDecode) {
            assertTrue(low.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING));
        }
        Request high = add("2", "g", 80);
        PrefillEndpoint chosenEndpoint = lowWaitingForDecode ? endpoint : V2TestSupport.prefillEndpoint("p2:8000");
        when(picker.pick(high, RoleType.PREFILL, "g", inputs))
                .thenReturn(V2TestSupport.selected(chosenEndpoint, 0L, 10L));

        assertTrue(loop.runOnce(RUNNING));
        CountDownLatch blocked = signalOnFullSubqueue(high, chosenEndpoint);
        try (ExecutorService consumer = Executors.newSingleThreadExecutor()) {
            var round = consumer.submit(() -> loop.runOnce(RUNNING));
            try {
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                assertEquals(lowWaitingForDecode ? Stage.DECODE_WAITING : Stage.PREFILL_WAITING, low.stage());
                assertNull(low.queueProgress().prefillReturnPriority());
                assertEquals(Stage.MAIN_WAITING, high.stage());
            } finally {
                high.cancel(CancelReason.CLIENT_CANCELLED);
            }
            assertTrue(round.get(5, TimeUnit.SECONDS));
            assertEquals(1, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
            assertEquals(0, mainQueue.size());
        }
    }

    private CountDownLatch signalOnFullSubqueue(Request request, PrefillEndpoint chosenEndpoint) {
        CountDownLatch blocked = new CountDownLatch(1);
        doAnswer(call -> {
            blocked.countDown();
            return null;
        }).when(reporter)
                .blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL, chosenEndpoint.ipPort());
        return blocked;
    }

    @Test
    void loopContinuesAfterError() throws InterruptedException {
        CountDownLatch nextRound = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        doAnswer(call -> {
            if (calls.getAndIncrement() == 0) {
                throw new AssertionError("test round failure");
            }
            nextRound.countDown();
            return 0;
        }).when(endpointRegistry).getEndpointCount(RoleType.PREFILL);

        AtomicBoolean running = new AtomicBoolean(true);
        Thread consumerThread = newConsumerThread(loop, running);
        consumerThread.start();
        try {
            assertTrue(nextRound.await(5, TimeUnit.SECONDS));
        } finally {
            stopConsumer(consumerThread, running);
        }
    }

    private static Thread newConsumerThread(MainQueueConsumer consumer, AtomicBoolean running) {
        Thread thread = new Thread(() -> consumer.run(running::get), "test-main-queue-consumer");
        thread.setDaemon(true);
        return thread;
    }

    private void stopConsumer(Thread thread, AtomicBoolean running) throws InterruptedException {
        running.set(false);
        mainQueue.wakeConsumer();
        thread.join(5_000L);
        assertFalse(thread.isAlive());
    }

    @Test
    void twoEnqueuesNotifyTwoWaitingConsumers() throws Exception {
        CountDownLatch waiting = new CountDownLatch(2);
        try (ExecutorService consumers = Executors.newFixedThreadPool(2)) {
            Runnable awaitWakeup = () -> {
                waiting.countDown();
                mainQueue.awaitWakeup(10_000L);
            };
            var first = consumers.submit(awaitWakeup);
            var second = consumers.submit(awaitWakeup);
            try {
                assertTrue(waiting.await(5, TimeUnit.SECONDS));
                add("1", "g");
                add("2", "g");

                first.get(2, TimeUnit.SECONDS);
                second.get(2, TimeUnit.SECONDS);
            } finally {
                first.cancel(true);
                second.cancel(true);
            }
        }
    }

    @Test
    void queuedWakeupsAreKeptButSequentialNotificationsDoNotExceedConsumerCount() throws Exception {
        mainQueue.wakeConsumer();
        mainQueue.wakeConsumer();
        mainQueue.wakeConsumer();

        CountDownLatch twoWakeupsTaken = new CountDownLatch(1);
        try (ExecutorService consumer = Executors.newSingleThreadExecutor()) {
            var thirdWait = consumer.submit(() -> {
                mainQueue.awaitWakeup(10_000L);
                mainQueue.awaitWakeup(10_000L);
                twoWakeupsTaken.countDown();
                mainQueue.awaitWakeup(10_000L);
            });
            try {
                assertTrue(twoWakeupsTaken.await(2, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> thirdWait.get(100, TimeUnit.MILLISECONDS));

                mainQueue.wakeConsumer();

                thirdWait.get(2, TimeUnit.SECONDS);
            } finally {
                thirdWait.cancel(true);
            }
        }
    }

    @Test
    void preparationRunsTogetherAndDecisionsUseAlreadyUpdatedQueues() throws Exception {
        Request first = add("1", "g");
        Request second = add("2", "g");
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch finishFirst = new CountDownLatch(1);
        CompletableFuture<Request> firstSent = new CompletableFuture<>();
        CompletableFuture<Request> secondSent = new CompletableFuture<>();
        doAnswer(call -> {
            bothStarted.countDown();
            if (call.getArgument(0) == first.ctx()) {
                assertTrue(finishFirst.await(5, TimeUnit.SECONDS));
            }
            return inputs;
        }).when(picker).prepare(any(), any(), any());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (RequestPreparePool workers = new RequestPreparePool(executor, mainQueue, picker, RoleType.PREFILL)) {
            MainQueueConsumer consumer = new MainQueueConsumer(mainQueue, cleanup, queueCapacity, endpointRegistry,
                    configService, picker, prefillQueues, RoleType.PREFILL, request -> {
                        prefillQueues.addWaiting(request);
                        (request == first ? firstSent : secondSent).complete(request);
                    }, workers, reporter);
            AtomicBoolean running = new AtomicBoolean(true);
            Thread consumerThread = newConsumerThread(consumer, running);
            AtomicInteger decisions = new AtomicInteger();
            doAnswer(call -> {
                assertSame(consumerThread, Thread.currentThread());
                PrefillQueue queue = prefillQueues.find(endpoint);
                assertEquals(decisions.getAndIncrement(), queue == null ? 0 : queue.waitingCount());
                return V2TestSupport.selected(endpoint, 0L, 10L);
            }).when(picker).pick(any(), any(), any(), any());

            consumerThread.start();
            try {
                assertTrue(bothStarted.await(5, TimeUnit.SECONDS));
                assertSame(second, secondSent.get(5, TimeUnit.SECONDS));
                assertEquals(Stage.MAIN_WAITING, first.stage());
                assertEquals(0, mainQueue.size());
                assertEquals(2L, queueCapacity.globalQueueAndEngineSubQueueCount());
                finishFirst.countDown();
                assertSame(first, firstSent.get(5, TimeUnit.SECONDS));
            } finally {
                finishFirst.countDown();
                stopConsumer(consumerThread, running);
            }
        }
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void fullQuotaKeepsSelectionOutOfMainQueueUntilCapacityReturns() throws Exception {
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(1);
        assertTrue(queueCapacity.tryReserveEngineSubQueueSlot(config, RoleType.PREFILL, 1));
        Request request = add("1", "g");
        SelectedPrefill selected = V2TestSupport.selected(endpoint, 0L, 10L);
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(selected);
        assertTrue(loop.runOnce(RUNNING));
        CountDownLatch blocked = signalOnFullSubqueue(request, endpoint);
        try (ExecutorService consumer = Executors.newSingleThreadExecutor()) {
            var round = consumer.submit(() -> loop.runOnce(RUNNING));
            try {
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                assertFalse(round.isDone());
                assertEquals(Stage.MAIN_WAITING, request.stage());
                assertEquals(0, mainQueue.size());
                assertEquals(1, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
                assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());

                queueCapacity.releaseEngineSubQueueSlot(RoleType.PREFILL);

                assertTrue(round.get(5, TimeUnit.SECONDS));
                assertEquals(Stage.PREFILL_WAITING, request.stage());
                assertSame(selected, request.selectedPrefill());
                assertEquals(List.of(request), sent);
                verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
                verify(picker).pick(request, RoleType.PREFILL, "g", inputs);
            } finally {
                if (!round.isDone()) {
                    request.cancel(CancelReason.CLIENT_CANCELLED);
                }
            }
        }
    }

    @Test
    void saturatedPreparationRunsOnTheConsumerAndReturnsTheResultToTheQueue() {
        Request request = add("1", "g");
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(V2TestSupport.selected(endpoint, 0L, 10L));
        ExecutorService executor = mock(ExecutorService.class);
        doThrow(new RejectedExecutionException()).when(executor).execute(any());
        try (RequestPreparePool workers = new RequestPreparePool(executor, mainQueue, picker, RoleType.PREFILL)) {
            MainQueueConsumer consumer = createConsumer(workers);
            assertTrue(consumer.runOnce(RUNNING));
            assertEquals(1, request.ctx().getMetrics().getPrepareSubmitRejectedCount());
            assertSame(inputs, request.getWorkLoadEstimate());
            assertEquals(1, mainQueue.size());
            assertTrue(sent.isEmpty());

            assertTrue(consumer.runOnce(RUNNING));
            assertEquals(List.of(request), sent);
            verify(executor).execute(any());
            verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aSecondConsumerCannotTakeARequestWhileItIsOutForPreparation(boolean hasExpiredEstimate) throws Exception {
        Request request = add("1", "g");
        long originalOrder = request.queueSeq();
        if (hasExpiredEstimate) {
            request.setWorkLoadEstimate(new RequestWorkLoadEstimateResult(inputs.cacheMatch(), inputs.candidates(),
                    null, System.nanoTime() - TimeUnit.MINUTES.toNanos(1)));
        }
        var previousEstimate = request.getWorkLoadEstimate();
        ExecutorService executor = mock(ExecutorService.class);
        CountDownLatch submitting = new CountDownLatch(1);
        CountDownLatch finishSubmit = new CountDownLatch(1);
        doAnswer(call -> {
            submitting.countDown();
            assertTrue(finishSubmit.await(5, TimeUnit.SECONDS));
            call.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any());

        try (RequestPreparePool workers = new RequestPreparePool(executor, mainQueue, picker, RoleType.PREFILL);
                ExecutorService consumers = Executors.newSingleThreadExecutor()) {
            var firstRound = consumers.submit(() -> createConsumer(workers).runOnce(RUNNING));
            try {
                assertTrue(submitting.await(5, TimeUnit.SECONDS));
                assertFalse(createConsumer(workers).runOnce(RUNNING));
                assertSame(previousEstimate, request.getWorkLoadEstimate());
                assertEquals(0, mainQueue.size());
                assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
                verify(executor).execute(any());
            } finally {
                finishSubmit.countDown();
            }
            assertTrue(firstRound.get(5, TimeUnit.SECONDS));
            assertSame(inputs, request.getWorkLoadEstimate());
            assertEquals(originalOrder, request.queueSeq());
            assertSame(request, mainQueue.pollFirst());
            assertNull(mainQueue.pollFirst());
            assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
            verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        }
    }

    @Test
    void cancelledDuringPreparationIsNotPutBack() {
        Request request = add("1", "g");
        when(picker.prepare(request.ctx(), RoleType.PREFILL, "g")).thenAnswer(call -> {
            assertTrue(request.cancel(CancelReason.CLIENT_CANCELLED));
            return inputs;
        });

        assertTrue(loop.runOnce(RUNNING));

        assertEquals(Stage.CANCELLED, request.stage());
        assertNull(request.getWorkLoadEstimate());
        assertNull(mainQueue.pollFirst());
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    @Test
    void returnedRequestReusesItsEstimateIfItIsStillValid() {
        Request request = add("1", "g");
        doAnswer(call -> V2TestSupport.selected(endpoint, 0L, 10L)).when(picker)
                .pick(request, RoleType.PREFILL, "g", inputs);
        assertTrue(loop.runOnce(RUNNING));
        var estimate = request.getWorkLoadEstimate();
        assertTrue(loop.runOnce(RUNNING));

        cleanup.returnToMain(request, Stage.PREFILL_WAITING);
        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertSame(estimate, request.getWorkLoadEstimate());
        assertTrue(loop.runOnce(RUNNING));

        assertEquals(List.of(request, request), sent);
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        verify(picker, times(2)).pick(request, RoleType.PREFILL, "g", inputs);
    }

    @Test
    void changedTtlExpiresExistingMaterials() {
        Request request = add("1", "g");
        assertTrue(loop.runOnce(RUNNING));
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");

        var expired = new RequestWorkLoadEstimateResult(inputs.cacheMatch(), inputs.candidates(), null,
                System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(25L));
        request.setWorkLoadEstimate(expired);
        config.getDispatcher().setCacheMatchResultTtlMs(10L);
        loop.runOnce(RUNNING);
        verify(picker, times(2)).prepare(request.ctx(), RoleType.PREFILL, "g");

        assertSame(inputs, request.getWorkLoadEstimate());
    }
}

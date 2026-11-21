package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.DirectExecutorService;
import org.flexlb.balance.schedulev2.LoopSignal;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.RequestPreparePool;
import org.flexlb.balance.schedulev2.SelectionResult;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.decode.DecodeQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.PrefillInputs;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MainQueueConsumerTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final ConfigService configService = mock(ConfigService.class);
    private final QueueCapacity queueCapacity = new QueueCapacity();
    private final LoopSignal loopSignal = new LoopSignal();
    private final RequestTable requestTable = new RequestTable();
    private final V2QueueReporter reporter = mock(V2QueueReporter.class);
    private final MainQueue mainQueue = new MainQueue(queueCapacity, loopSignal, true, reporter);
    private final PrefillQueueRegistry prefillQueues;
    private final RequestCleanup cleanup;
    private final PrefillEndpoint endpoint = V2TestSupport.prefillEndpoint("p1:8000");
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);
    private final PrefillPicker picker = mock(PrefillPicker.class);
    private final PrefillInputs inputs =
            new PrefillInputs(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC), List.of());
    private final List<Request> sent = new ArrayList<>();
    private final MainQueueConsumer loop;

    MainQueueConsumerTest() {
        config.getDispatcher().setType(org.flexlb.config.DispatcherConfig.Type.NON_BATCH_V2);
        when(configService.loadBalanceConfig()).thenReturn(config);
        when(picker.prepare(any(), any(), any())).thenReturn(inputs);
        when(picker.inputsAreCurrent(any(), any(), any())).thenReturn(true);
        prefillQueues = new PrefillQueueRegistry();
        V2TestSupport.register(prefillQueues, endpoint);
        cleanup = new RequestCleanup(requestTable, mainQueue, queueCapacity, loopSignal, prefillQueues,
                new DecodeQueue(false));
        when(endpointRegistry.getEndpointCount(RoleType.PREFILL)).thenReturn(1);
        loop = createConsumer(new RequestPreparePool(new DirectExecutorService(), loopSignal));
    }

    private MainQueueConsumer createConsumer(RequestPreparePool workers) {
        return new MainQueueConsumer(mainQueue, loopSignal, cleanup, queueCapacity, endpointRegistry, configService,
                picker, prefillQueues, RoleType.PREFILL, request -> {
                    prefillQueues.addWaiting(request);
                    sent.add(request);
                }, workers, reporter);
    }

    @Test
    void pickedRequestHoldsQueueSlotAndGoesToPrefillQueue() {
        Request request = add("1", "g");
        SelectedRole selected = V2TestSupport.selected(endpoint, 4L, 30L);
        WorkerEndpoint.EndpointHold hold = selected.takeEndpointHold();
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.success(selected));

        assertFalse(loop.runOnce());
        assertTrue(loop.runOnce());

        assertEquals(List.of(request), sent);
        assertSame(endpoint, request.selectedPrefill().endpoint());
        assertEquals(4L, request.selectedPrefill().hitCacheTokens());
        assertEquals(30L, request.selectedPrefill().predictedMs());
        verify(hold).close();
        assertEquals(Stage.PREFILL_WAITING, request.stage());
        assertEquals(0, mainQueue.size());
        assertEquals(1, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    @Test
    void noPrefillNodesLeaveRequestsQueuedWithoutPreparingOrReporting() {
        when(endpointRegistry.getEndpointCount(RoleType.PREFILL)).thenReturn(0);
        Request request = add("1", "g");

        assertFalse(loop.runOnce());

        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertEquals(1, mainQueue.size());
        assertNull(request.getWorkLoadEstimate());
        verifyNoInteractions(picker, reporter);
    }

    @Test
    void blockedRequestDoesNotStopAnotherRequestInTheSameGroup() {
        Request blocked = add("1", "g");
        Request next = add("2", "g");
        when(picker.pick(blocked, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.blocked(RoleType.PREFILL));
        SelectedRole selected = V2TestSupport.selected(endpoint, 0L, 10L);
        when(picker.pick(next, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.success(selected));

        assertFalse(loop.runOnce());
        assertTrue(loop.runOnce());

        assertEquals(List.of(next), sent);
        assertEquals(Stage.MAIN_WAITING, blocked.stage());
        assertEquals(Stage.PREFILL_WAITING, next.stage());
        assertNull(blocked.takeQueueSlot());
        assertEquals(1, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
    }

    @Test
    void rejectedSelectionFailsRequestAndReleasesEverything() {
        Request request = add("1", "g");
        Response failure = Response.buildErrorResponse(StrategyErrorType.INVALID_REQUEST, "bad request");
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.rejected(failure));

        assertFalse(loop.runOnce());
        assertTrue(loop.runOnce());

        assertSame(failure, request.future().join());
        assertEquals(0, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
        assertEquals(0, mainQueue.size());
    }

    @Test
    void requestCancelledDuringPickClosesSelectionAndReleasesSlot() {
        Request request = add("1", "g");
        SelectedRole selected = V2TestSupport.selected(endpoint, 0L, 10L);
        when(picker.pick(eq(request), eq(RoleType.PREFILL), any(), any())).thenAnswer(call -> {
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return PlacementResult.success(selected);
        });

        assertFalse(loop.runOnce());
        loop.runOnce();

        verify(selected).close();
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
    void blockedRequestsStillCountAgainstTheScanLimit() {
        config.getScheduler().setMaxQueuedRequests(1_000);
        for (int i = 1; i <= MainQueueConsumer.MAX_SCAN_PER_ROUND; i++) {
            add(Integer.toString(i), "g");
        }
        Request beyondLimit = add(Integer.toString(MainQueueConsumer.MAX_SCAN_PER_ROUND + 1), "g");
        when(picker.pick(any(), eq(RoleType.PREFILL), eq("g"), eq(inputs)))
                .thenReturn(PlacementResult.blocked(RoleType.PREFILL));

        assertFalse(loop.runOnce());
        assertFalse(loop.runOnce());

        verify(picker, times(MainQueueConsumer.MAX_SCAN_PER_ROUND)).pick(any(), eq(RoleType.PREFILL), eq("g"),
                eq(inputs));
        assertNull(beyondLimit.getWorkLoadEstimate());
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
        assertSame(high, mainQueue.iterator().next());
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
        verify(reporter).mainPreempted(50, 40);
        org.mockito.Mockito.verifyNoMoreInteractions(reporter);
    }

    @Test
    void fullSubqueuesOnlyMarkNewestLowestPriorityAndWaitForReleasedSlot() {
        SchedulingTestConfig.usePriorityQueue(config);
        config.priorityOrdering().setHighPriorityThreshold(60);
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(2);
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L))).when(picker)
                .pick(any(), any(), any(), any());
        Request older = add("1", "g", 10);
        Request newer = add("2", "g", 10);
        assertFalse(loop.runOnce());
        assertTrue(loop.runOnce());
        Request high = add("3", "g", 80);

        assertFalse(loop.runOnce());
        assertFalse(loop.runOnce());

        assertNull(older.queueProgress().prefillReturnPriority());
        assertEquals(80, newer.queueProgress().prefillReturnPriority());
        assertEquals(Stage.PREFILL_WAITING, newer.stage());
        assertFalse(newer.future().isDone());
        assertEquals(Stage.MAIN_WAITING, high.stage());
        assertEquals(2, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        assertEquals(3L, queueCapacity.globalQueueAndEngineSubQueueCount());
        org.mockito.Mockito.verify(reporter, org.mockito.Mockito.never())
                .preemptedReturn(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt(), any());
    }

    @Test
    void fullSubqueuesDoNotPreemptAnotherPrefillOrDecodeWaiting() {
        SchedulingTestConfig.usePriorityQueue(config);
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(1);
        Request low = add("1", "g", 10);
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L))).when(picker)
                .pick(low, RoleType.PREFILL, "g", inputs);
        assertFalse(loop.runOnce());
        assertTrue(loop.runOnce());
        Request high = add("2", "g", 80);
        PrefillEndpoint other = V2TestSupport.prefillEndpoint("p2:8000");
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(other, 0L, 10L))).when(picker)
                .pick(high, RoleType.PREFILL, "g", inputs);

        assertFalse(loop.runOnce());
        assertEquals(Stage.PREFILL_WAITING, low.stage());

        low.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L))).when(picker)
                .pick(high, RoleType.PREFILL, "g", inputs);
        assertFalse(loop.runOnce());
        assertEquals(Stage.DECODE_WAITING, low.stage());
        assertEquals(Stage.MAIN_WAITING, high.stage());
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
        loopSignal.wake();
        thread.join(5_000L);
        assertFalse(thread.isAlive());
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
        try (RequestPreparePool workers = new RequestPreparePool(executor, loopSignal)) {
            MainQueueConsumer consumer = new MainQueueConsumer(mainQueue, loopSignal, cleanup, queueCapacity,
                    endpointRegistry, configService, picker, prefillQueues, RoleType.PREFILL, request -> {
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
                return PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L));
            }).when(picker).pick(any(), any(), any(), any());

            consumerThread.start();
            try {
                assertTrue(bothStarted.await(5, TimeUnit.SECONDS));
                assertSame(second, secondSent.get(5, TimeUnit.SECONDS));
                assertEquals(Stage.MAIN_WAITING, first.stage());
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
    void fullQuotaReusesMaterialsUntilCapacityReturns() {
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        config.getDispatcher().setPrefillSubqueueLimitMultiplier(1);
        assertTrue(queueCapacity.tryReserveEngineSubQueueSlot(config, RoleType.PREFILL, 1));
        Request request = add("1", "g");
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L))).when(picker)
                .pick(any(), any(), any(), any());
        for (int i = 0; i < 10; i++) {
            assertFalse(loop.runOnce());
        }
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        assertEquals(Stage.MAIN_WAITING, request.stage());

        queueCapacity.releaseEngineSubQueueSlot(RoleType.PREFILL);
        assertTrue(loop.runOnce());
        assertEquals(Stage.PREFILL_WAITING, request.stage());
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
    }

    @Test
    void rejectedPreparationDoesNotSkipLaterSubmissionsOrReadyRequests() {
        Request first = add("1", "g");
        Request second = add("2", "g");
        Request ready = add("3", "g");
        ready.setWorkLoadEstimate(
                CompletableFuture.completedFuture(new SelectionResult<>(inputs, null, System.nanoTime())));
        SelectedRole selected = V2TestSupport.selected(endpoint, 0L, 10L);
        when(picker.pick(ready, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.success(selected));
        ExecutorService executor = mock(ExecutorService.class);
        doThrow(new RejectedExecutionException()).when(executor).execute(any());
        try (RequestPreparePool workers = new RequestPreparePool(executor, loopSignal)) {
            MainQueueConsumer consumer = createConsumer(workers);
            assertTrue(consumer.runOnce());
            verify(executor, times(2)).execute(any());
            assertEquals(1, first.ctx().getMetrics().getPrepareSubmitRejectedCount());
            assertEquals(1, second.ctx().getMetrics().getPrepareSubmitRejectedCount());
            assertEquals(2, mainQueue.size());
            assertEquals(List.of(ready), sent);
        }
    }

    @Test
    void returnedRequestReusesItsEstimateIfItIsStillValid() {
        Request request = add("1", "g");
        doAnswer(call -> PlacementResult.success(V2TestSupport.selected(endpoint, 0L, 10L))).when(picker)
                .pick(request, RoleType.PREFILL, "g", inputs);
        assertFalse(loop.runOnce());
        var estimate = request.getWorkLoadEstimate();
        assertTrue(loop.runOnce());

        cleanup.returnToMain(request, Stage.PREFILL_WAITING);
        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertSame(estimate, request.getWorkLoadEstimate());
        assertTrue(loop.runOnce());

        assertEquals(List.of(request, request), sent);
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        verify(picker, times(2)).inputsAreCurrent(inputs, RoleType.PREFILL, "g");
    }

    @Test
    void changedTtlExpiresExistingMaterialsAndCandidateChangesInvalidateThem() {
        Request request = add("1", "g");
        when(picker.pick(request, RoleType.PREFILL, "g", inputs)).thenReturn(PlacementResult.blocked(RoleType.PREFILL));
        loop.runOnce();
        loop.runOnce();
        verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");

        request.setWorkLoadEstimate(CompletableFuture.completedFuture(
                new SelectionResult<>(inputs, null, System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(25L))));
        config.getDispatcher().setCacheMatchResultTtlMs(10L);
        loop.runOnce();
        verify(picker, times(2)).prepare(request.ctx(), RoleType.PREFILL, "g");

        config.getDispatcher().setCacheMatchResultTtlMs(30_000L);
        when(picker.inputsAreCurrent(any(), any(), any())).thenReturn(false);
        loop.runOnce();
        assertNull(request.getWorkLoadEstimate());
        when(picker.inputsAreCurrent(any(), any(), any())).thenReturn(true);
        loop.runOnce();
        verify(picker, times(3)).prepare(request.ctx(), RoleType.PREFILL, "g");
    }
}

package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.eviction.DecodePreemptionCoordinator;
import org.flexlb.balance.eviction.EngineCancelChannel;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.scheduler.decode.DecodeRequirement;
import org.flexlb.balance.schedulev2.LoopSignal;
import org.flexlb.balance.schedulev2.PrefillPublishCheck;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.main.MainQueue;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.QueueOrderingConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.service.monitor.V2QueueReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DecodeQueueConsumerTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final ConfigService configService = mock(ConfigService.class);
    private final RequestSchedulerReporter reporter = mock(RequestSchedulerReporter.class);
    private final V2QueueReporter queueReporter = mock(V2QueueReporter.class);
    private final PrefillPublishCheck prefillCheck = mock(PrefillPublishCheck.class);
    private final DecodeReservations reservations = new DecodeReservations();
    private final DecodeQueue decodeQueue = new DecodeQueue(true);
    private final RequestTable requestTable = new RequestTable();
    private final QueueCapacity queueCapacity = new QueueCapacity();
    private final LoopSignal mainSignal = new LoopSignal();
    private final MainQueue mainQueue = new MainQueue(queueCapacity, mainSignal, true, queueReporter);
    private final RequestCleanup cleanup = spy(new RequestCleanup(requestTable, mainQueue, queueCapacity, mainSignal,
            new org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry(), decodeQueue));
    private final DecodeSelector selector = mock(DecodeSelector.class);
    private final PlacementAvailability availability = new PlacementAvailability();
    private final EndpointEventHandler endpointEvents = new EndpointEventHandler(mock(RequestRegistry.class),
            mock(org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry.class), configService, reservations,
            reporter);
    private final DecodeEndpoint decode =
            spy(new DecodeEndpoint(WorkerStatus.createDiscovered(RoleType.DECODE, "g", "127.0.0.2", 8080, 8081, "test"),
                    endpointEvents));
    private final DecodeEndpoint otherDecode = new DecodeEndpoint(
            WorkerStatus.createDiscovered(RoleType.DECODE, "g", "127.0.0.3", 8080, 8081, "test"), endpointEvents);
    private final List<Request> requests = new ArrayList<>();
    private DecodeQueueConsumerPoll pool;

    DecodeQueueConsumerTest() {
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH_V2);
        config.getRouter().getRoles().getDecode().getAvailability().setMaxEngineRequests(1L);
        when(configService.loadBalanceConfig()).thenReturn(config);
        when(prefillCheck.isAvailable(any())).thenReturn(true);
        when(selector.select(any(), any(), any())).thenAnswer(call -> success(decode));
    }

    @AfterEach
    void close() {
        for (Request request : requests) {
            request.cancel(CancelReason.CLIENT_CANCELLED);
        }
        if (pool != null) {
            pool.close();
        }
        decode.close();
        otherDecode.close();
        EndpointRegistry.getEndpointsByRole(RoleType.DECODE).clear();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8})
    void publishesQueuedRequestsWithConfiguredConsumerCount(int count) throws Exception {
        config.getRouter().getRoles().getDecode().getAvailability().setMaxEngineRequests(64L);
        for (int i = 1; i <= 32; i++) {
            addWaiting(Integer.toString(i), 30);
        }
        start(count);
        for (Request request : requests) {
            assertTrue(response(request).isSuccess());
        }
        pool.close();
        assertEquals(32, decode.getInflightCount());
        assertEquals(0, requestTable.waitingCount());
        assertEquals(0, decodeQueue.waitingCount());
    }

    @Test
    void prefillFullRequestsDoNotHideReadyRequestAfterTwoThousandEntries() throws Exception {
        doReturn(QueueBlockReason.PREFILL_INFLIGHT_FULL).when(prefillCheck).tryAcquire(any(), anyInt());
        for (int i = 1; i <= 2000; i++) {
            addWaiting(Integer.toString(i), 30);
        }
        Request ready = addWaiting("2001", 30);
        doReturn(null).when(prefillCheck).tryAcquire(eq(ready), anyInt());
        start(1);

        assertTrue(response(ready).isSuccess());
        pool.close();
        assertEquals(2000, decodeQueue.waitingCount());
        verify(selector).select(any(), any(), eq("g"));
    }

    @Test
    void singleConsumerTakesHigherPriorityThenFifoWithinSamePriority() throws Exception {
        config.getRouter().getRoles().getDecode().getAvailability().setMaxEngineRequests(10L);
        Request low = addWaiting("1", 30);
        Request high = addWaiting("2", 80);
        Request nextHigh = addWaiting("3", 80);
        List<String> selected = new ArrayList<>();
        doAnswer(call -> {
            DecodeRequirement requirement = call.getArgument(1);
            selected.add(requirement.requestId());
            return success(decode);
        }).when(selector).select(any(), any(), any());
        start(1);

        assertTrue(response(low).isSuccess());
        assertTrue(response(high).isSuccess());
        assertTrue(response(nextHigh).isSuccess());
        pool.close();
        assertEquals(List.of("2", "3", "1"), selected);
    }

    @Test
    void multipleConsumersSelectConcurrentlyAndEachRequestIsPublishedOnce() throws Exception {
        config.getRouter().getRoles().getDecode().getAvailability().setMaxEngineRequests(10L);
        Request first = addWaiting("1", 30);
        Request second = addWaiting("2", 30);
        decodeQueue.addWaiting(first);
        CountDownLatch selecting = new CountDownLatch(2);
        CountDownLatch releaseSelection = new CountDownLatch(1);
        doAnswer(call -> {
            assertTrue(Thread.currentThread().getName().startsWith("flexlb-v2-decode-consumer-"));
            selecting.countDown();
            assertTrue(releaseSelection.await(5, TimeUnit.SECONDS));
            return success(decode);
        }).when(selector).select(any(), any(), any());
        start(2);
        try {
            assertTrue(selecting.await(5, TimeUnit.SECONDS));
        } finally {
            releaseSelection.countDown();
        }

        assertTrue(response(first).isSuccess());
        assertTrue(response(second).isSuccess());
        pool.close();
        assertEquals(2, decode.getInflightCount());
        verify(selector, times(1)).select(eq(first.ctx()), any(), any());
        verify(selector, times(1)).select(eq(second.ctx()), any(), any());
        assertEquals(0, requestTable.size());
    }

    @Test
    void concurrentConsumersCannotOverReserveSameDecode() throws Exception {
        Request first = addWaiting("1", 30);
        Request second = addWaiting("2", 30);
        CountDownLatch selecting = new CountDownLatch(2);
        CountDownLatch releaseSelection = new CountDownLatch(1);
        CountDownLatch published = new CountDownLatch(1);
        first.future().thenRun(published::countDown);
        second.future().thenRun(published::countDown);
        doAnswer(call -> {
            selecting.countDown();
            assertTrue(releaseSelection.await(5, TimeUnit.SECONDS));
            return success(decode);
        }).when(selector).select(any(), any(), any());
        start(2);
        try {
            assertTrue(selecting.await(5, TimeUnit.SECONDS));
        } finally {
            releaseSelection.countDown();
        }
        assertTrue(published.await(5, TimeUnit.SECONDS));
        pool.close();

        assertEquals(1, decode.getInflightCount());
        assertEquals(1L, requests.stream().filter(r -> r.stage() == Stage.PUBLISHED).count());
        verify(prefillCheck, times(1)).release(any());
    }

    @Test
    void decodeBlockedRequestKeepsPrefillSlotUntilCancelled() throws Exception {
        Request request = addWaiting("1", 30);
        CountDownLatch selecting = new CountDownLatch(1);
        doAnswer(call -> {
            selecting.countDown();
            return PlacementResult.blocked(RoleType.DECODE);
        }).when(selector).select(any(), any(), any());
        start(1);
        assertTrue(selecting.await(5, TimeUnit.SECONDS));
        verify(prefillCheck, times(1)).tryAcquire(eq(request), anyInt());
        verify(prefillCheck, never()).release(request);
        assertEquals(1, requestTable.waitingCount());
        assertEquals(0, decodeQueue.waitingCount());

        request.cancel(CancelReason.CLIENT_CANCELLED);
        pool.close();
        verify(prefillCheck).release(request);
        assertNull(request.decodeChoice());
        assertFalse(reservations.contains("1"));
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
    }

    @Test
    void anotherConsumerCanPublishWhileLargeRequestWaits() throws Exception {
        Request large = addWaiting("1", 30);
        Request small = addWaiting("2", 30);
        doAnswer(
                call -> call.getArgument(0) == large.ctx() ? PlacementResult.blocked(RoleType.DECODE) : success(decode))
                .when(selector)
                .select(any(), any(), any());
        start(2);

        assertTrue(response(small).isSuccess());
        assertFalse(large.future().isDone());
        large.cancel(CancelReason.CLIENT_CANCELLED);
        pool.close();
        assertEquals(1, decode.getInflightCount());
    }

    @Test
    void publishedAddressesRetainReservationsAndReleaseQueueQuota() throws Exception {
        Request request = addWaiting("1", 30);
        assertTrue(queueCapacity.tryReserveEngineSubQueueSlot(config, RoleType.PREFILL, 1));
        request.holdQueueSlot(RoleType.PREFILL);
        start(1);

        Response response = response(request);
        assertTrue(response.isSuccess());
        pool.close();
        assertSame(request.selectedPrefill().serverStatus(), response.getServerStatus().get(0));
        assertEquals("127.0.0.2", response.getServerStatus().get(1).getServerIp());
        assertNotNull(decode.reservationHandle("1"));
        assertTrue(reservations.contains("1"));
        assertEquals(0, requestTable.size());
        assertEquals(0L, queueCapacity.globalQueueAndEngineSubQueueCount());
        assertEquals(0, queueCapacity.engineSubQueueWaiting(RoleType.PREFILL));
        verify(prefillCheck, never()).release(request);
    }

    @Test
    void releasedDecodeCapacityLetsWaitingConsumerPublish() throws Exception {
        Request first = addWaiting("1", 30);
        CountDownLatch tried = new CountDownLatch(1);
        doAnswer(call -> {
            DecodeRequirement requirement = call.getArgument(1);
            if ("2".equals(requirement.requestId())) {
                tried.countDown();
            }
            return success(decode);
        }).when(selector).select(any(), any(), any());
        start(1);
        assertTrue(response(first).isSuccess());
        Request second = addWaiting("2", 30);
        assertTrue(tried.await(5, TimeUnit.SECONDS));

        decode.release(first.decodeChoice().reservation(), DecodeEndpoint.ReleaseReason.EXPIRED);
        assertTrue(response(second).isSuccess());
        pool.close();
        assertEquals(1, decode.getInflightCount());
    }

    @Test
    void cancellationDuringReserveReleasesLocalResources() throws Exception {
        Request request = addWaiting("1", 30);
        doAnswer(call -> {
            Object result = call.callRealMethod();
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return result;
        }).when(decode).reserveForDispatch(any(), eq("1"), anyLong(), anyLong(), anyInt(), any());
        start(1);

        assertFalse(response(request).isSuccess());
        pool.close();
        assertNull(request.decodeChoice());
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
        verify(prefillCheck).release(request);
    }

    @Test
    void cancellationDuringSelectionDoesNotPublishOrLeakResources() throws Exception {
        Request request = addWaiting("1", 30);
        doAnswer(call -> {
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return success(decode);
        }).when(selector).select(eq(request.ctx()), any(), any());
        start(1);

        assertFalse(response(request).isSuccess());
        pool.close();
        assertEquals(0, decode.routingView().totalLoad());
        assertNull(request.decodeChoice());
        verify(prefillCheck).release(request);
    }

    @Test
    void prefillRetiringAfterLoopCheckDoesNotTriggerPublishRecheck() throws Exception {
        Request request = addWaiting("1", 30);
        AtomicBoolean prefillAvailable = new AtomicBoolean(true);
        when(prefillCheck.isAvailable(request)).thenAnswer(call -> prefillAvailable.get());
        doAnswer(call -> {
            prefillAvailable.set(false);
            return success(decode);
        }).when(selector).select(eq(request.ctx()), any(), any());
        start(1);

        assertTrue(response(request).isSuccess());
        pool.close();
        assertFalse(prefillAvailable.get());
        assertEquals(Stage.PUBLISHED, request.stage());
        assertEquals(0, mainQueue.size());
        assertEquals(1, decode.getInflightCount());
        verify(prefillCheck, never()).hold(any());
        verify(prefillCheck, never()).release(request);
    }

    @Test
    void prefillUnavailableAtLoopStartReturnsToMainWithoutReservingDecode() throws Exception {
        Request request = addWaiting("1", 30);
        when(prefillCheck.isAvailable(request)).thenReturn(true, false);
        CountDownLatch returned = new CountDownLatch(1);
        doAnswer(call -> {
            Object result = call.callRealMethod();
            returned.countDown();
            return result;
        }).when(cleanup).returnToMain(eq(request), any());
        start(1);

        assertTrue(returned.await(5, TimeUnit.SECONDS));
        pool.close();
        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertFalse(request.future().isDone());
        assertNull(request.selectedPrefill());
        assertNull(request.decodeChoice());
        assertEquals(1, mainQueue.size());
        assertEquals(1L, queueCapacity.globalQueueAndEngineSubQueueCount());
        assertEquals(0, decode.getInflightCount());
        verify(selector, never()).select(any(), any(), any());
        verify(prefillCheck).release(request);
    }

    @Test
    void prefillUnavailableOnRetryReturnsToMainAndReleasesHeldSlot() throws Exception {
        Request request = addWaiting("1", 30);
        AtomicBoolean prefillAvailable = new AtomicBoolean(true);
        when(prefillCheck.isAvailable(request)).thenAnswer(call -> prefillAvailable.get());
        doAnswer(call -> {
            prefillAvailable.set(false);
            return PlacementResult.blocked(RoleType.DECODE);
        }).when(selector).select(eq(request.ctx()), any(), any());
        CountDownLatch returned = new CountDownLatch(1);
        doAnswer(call -> {
            Object result = call.callRealMethod();
            returned.countDown();
            return result;
        }).when(cleanup).returnToMain(eq(request), any());
        start(1);

        assertTrue(returned.await(5, TimeUnit.SECONDS));
        pool.close();
        assertEquals(Stage.MAIN_WAITING, request.stage());
        assertFalse(request.future().isDone());
        assertEquals(1, mainQueue.size());
        assertEquals(0, decode.getInflightCount());
        verify(selector, times(1)).select(eq(request.ctx()), any(), any());
        verify(prefillCheck, times(1)).tryAcquire(eq(request), anyInt());
        verify(prefillCheck).release(request);
    }

    @Test
    void cancellationAtLoopPrefillCheckDoesNotRequeueEndedRequest() throws Exception {
        Request request = addWaiting("1", 30);
        when(prefillCheck.isAvailable(request)).thenReturn(true).thenAnswer(call -> {
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return false;
        });
        start(1);

        assertFalse(response(request).isSuccess());
        pool.close();
        assertEquals(0, mainQueue.size());
        assertNull(request.decodeChoice());
        assertEquals(0, decode.getInflightCount());
    }

    @Test
    void closedSelectionFailsDispatchAndReleasesPrefillSlot() throws Exception {
        Request request = addWaiting("1", 30);
        doReturn(PlacementResult.closed()).when(selector).select(any(), any(), any());
        start(1);

        assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), response(request).getCode());
        pool.close();
        assertEquals(Stage.FAILED, request.stage());
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
        verify(prefillCheck).release(request);
    }

    @Test
    void consumerContinuesAfterSelectionError() throws Exception {
        Request first = addWaiting("1", 30);
        doAnswer(call -> {
            throw new AssertionError("test selection failure");
        }).when(selector).select(eq(first.ctx()), any(), any());
        start(1);
        assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), response(first).getCode());

        Request next = addWaiting("2", 30);
        assertTrue(response(next).isSuccess());
        pool.close();
        assertFalse(reservations.contains("1"));
    }

    @Test
    void requirementFailureFailsRequestWithoutReservation() throws Exception {
        config.getRouter().getRoles().getDecode().getAvailability().setMaxKvUsagePercent(0L);
        Request request = addWaiting("1", 30);
        start(1);

        assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), response(request).getCode());
        pool.close();
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
        verify(selector, never()).select(any(), any(), any());
        verify(prefillCheck).release(request);
    }

    @Test
    void higherPriorityRequestPreemptsPublishedRequest() throws Exception {
        config.queueScheduler().getOrdering().setType(QueueOrderingConfig.Type.PRIORITY);
        Request low = addWaiting("1", 30);
        start(1);
        assertTrue(response(low).isSuccess());
        Request high = addWaiting("2", 80);

        assertTrue(response(high).isSuccess());
        pool.close();
        assertEquals(List.of("1"), high.future().join().getServerStatus().get(1).getPreemptRequestIds());
        verify(queueReporter).decodePreempted(80, 30, DecodeTaskPhase.ENGINE_MAY_HAVE_SEEN, decode.ipPort());
    }

    @Test
    void cancelledPreemptionDoesNotReportVictimAndCanBeRetried() throws Exception {
        config.queueScheduler().getOrdering().setType(QueueOrderingConfig.Type.PRIORITY);
        Request low = addWaiting("1", 30);
        doAnswer(call -> {
            DecodeEndpoint.ReservationHandle reservation = call.getArgument(0);
            if ("2".equals(reservation.requestId())) {
                requestTable.get("2").cancel(CancelReason.CLIENT_CANCELLED);
            }
            return call.callRealMethod();
        }).when(decode).acquireDispatchPermit(any(), any());
        start(1);
        assertTrue(response(low).isSuccess());
        Request high = addWaiting("2", 80);
        assertFalse(response(high).isSuccess());
        Request next = addWaiting("3", 80);

        assertTrue(response(next).isSuccess());
        pool.close();
        assertNull(decode.reservationHandle("2"));
        verify(queueReporter, times(1)).decodePreempted(80, 30, DecodeTaskPhase.ENGINE_MAY_HAVE_SEEN, decode.ipPort());
    }

    @Test
    void oversizedRequestCanUseEmptyDecodeWithoutPermit() throws Exception {
        Request request = addWaiting("1", 30);
        doReturn(PlacementResult.rejected(Response.error(StrategyErrorType.RESOURCE_EXHAUSTED))).when(selector)
                .select(any(), any(), any());
        register(otherDecode);
        start(1);

        assertTrue(response(request).isSuccess());
        pool.close();
        assertSame(otherDecode, request.decodeChoice().endpoint());
        assertTrue(reservations.contains("1"));
        assertEquals(1, otherDecode.getInflightCount());
    }

    @Test
    void emptyDecodeShortcutDoesNotRecheckPrefillBeforePublish() throws Exception {
        Request request = addWaiting("1", 30);
        AtomicBoolean prefillAvailable = new AtomicBoolean(true);
        when(prefillCheck.isAvailable(request)).thenAnswer(call -> prefillAvailable.get());
        doReturn(PlacementResult.blocked(RoleType.DECODE)).when(selector).select(any(), any(), any());
        DecodeEndpoint emptyDecode = spy(otherDecode);
        doAnswer(call -> {
            prefillAvailable.set(false);
            return call.callRealMethod();
        }).when(emptyDecode).reserveIfEmpty(any(), anyLong(), anyLong(), anyInt());
        register(emptyDecode);
        start(1);

        assertTrue(response(request).isSuccess());
        pool.close();
        assertFalse(prefillAvailable.get());
        assertEquals(1, otherDecode.getInflightCount());
        verify(prefillCheck, never()).hold(any());
        verify(prefillCheck, never()).release(request);
    }

    @Test
    void cancellationDuringEmptyReservationReleasesResources() throws Exception {
        Request request = addWaiting("1", 30);
        doReturn(PlacementResult.blocked(RoleType.DECODE)).when(selector).select(any(), any(), any());
        DecodeEndpoint emptyDecode = spy(otherDecode);
        doAnswer(call -> {
            Object result = call.callRealMethod();
            request.cancel(CancelReason.CLIENT_CANCELLED);
            return result;
        }).when(emptyDecode).reserveIfEmpty(any(), anyLong(), anyLong(), anyInt());
        register(emptyDecode);
        start(1);

        assertFalse(response(request).isSuccess());
        pool.close();
        assertEquals(0, otherDecode.getInflightCount());
        assertFalse(reservations.contains("1"));
        assertNull(request.decodeChoice());
    }

    @Test
    void publishedReservationIsProtectedFromOrphanSweep() throws Exception {
        Request request = addWaiting("1", 30);
        start(1);
        assertTrue(response(request).isSuccess());
        pool.close();

        decode.evictExpiredRequests(-1L, reservations::contains);
        assertNotNull(decode.reservationHandle("1"));
        assertTrue(reservations.contains("1"));
    }

    @Test
    void publishedReservationExpiresOnWorkerStatusTimeout() throws Exception {
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1000L);
        Request request = addWaiting("1", 30);
        start(1);
        assertTrue(response(request).isSuccess());
        pool.close();
        markRequestStale(decode, "1");

        updateDecodeStatus(decode, Map.of(), Map.of(), false);
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
        verify(reporter).reportStatusUpdateTimeout(RoleType.DECODE, decode.ipPort());
    }

    @Test
    void missingActiveRequestKeepsCapacityUntilStatusTimeout() throws Exception {
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(Long.MAX_VALUE);
        Request request = addWaiting("1", 30);
        start(1);
        assertTrue(response(request).isSuccess());
        pool.close();
        updateDecodeStatus(decode, Map.of("1", task("1", TaskPhase.RECEIVED)), Map.of(), false);
        updateDecodeStatus(decode, Map.of(), Map.of(), false);
        assertNotNull(decode.reservationHandle("1"));
        assertEquals(1L, decode.routingView().totalLoad());
        verify(reporter, never()).reportStatusUpdateTimeout(RoleType.DECODE, decode.ipPort());

        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1000L);
        markRequestStale(decode, "1");
        updateDecodeStatus(decode, Map.of(), Map.of(), false);
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void finishedOrRetiredDuringPublicationDoesNotRecreateReservation(boolean retire) throws Exception {
        Request request = addWaiting("1", 30);
        request.future().thenRun(() -> {
            if (retire) {
                decode.close();
            } else {
                updateDecodeStatus(decode, Map.of(), Map.of("1", task("1", TaskPhase.RUNNING)), false);
            }
        });
        start(1);

        assertTrue(response(request).isSuccess());
        pool.close();
        assertNull(decode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
    }

    @Test
    void unchangedHeartbeatDoesNotExpireEmptyDecodeReservation() throws Exception {
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(1000L);
        Request request = addWaiting("1", 30);
        doReturn(PlacementResult.blocked(RoleType.DECODE)).when(selector).select(any(), any(), any());
        register(otherDecode);
        start(1);
        assertTrue(response(request).isSuccess());
        pool.close();
        markRequestStale(otherDecode, "1");
        updateDecodeStatus(otherDecode, Map.of(), Map.of(), true);
        assertNotNull(otherDecode.reservationHandle("1"));

        updateDecodeStatus(otherDecode, Map.of(), Map.of(), false);
        assertNull(otherDecode.reservationHandle("1"));
        assertFalse(reservations.contains("1"));
    }

    private void start(int count) {
        pool = new DecodeQueueConsumerPoll(count, decodeQueue, availability,
                () -> new DecodeQueueConsumer(decodeQueue, cleanup, selector,
                        new DecodePreemptionCoordinator(mock(EngineCancelChannel.class), mock(RequestRegistry.class),
                                reporter),
                        configService, prefillCheck, reservations, queueReporter, decodeQueue::wake));
        pool.start();
    }

    private Request addWaiting(String id, int priority) {
        Request request = V2TestSupport.request(config, id, priority, 16L);
        request.setQueueSeq(Long.parseLong(id));
        V2TestSupport.choose(request, V2TestSupport.prefillEndpoint("p1:8000"), 0L, 0L);
        request.selectedPrefill().serverStatus().setGroup("g");
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        queueCapacity.increaseTotalQueueCount(request.future());
        requestTable.add(request);
        request.future().whenComplete((response, failure) -> {
            if (request.stage().isEnded()) {
                cleanup.releaseEnded(request);
            }
        });
        requests.add(request);
        decodeQueue.addWaiting(request);
        return request;
    }

    private static Response response(Request request) throws Exception {
        return request.future().get(5, TimeUnit.SECONDS);
    }

    private static PlacementResult<SelectedRole, RoleType> success(DecodeEndpoint endpoint) {
        ServerStatus status = new ServerStatus();
        status.setRole(RoleType.DECODE);
        status.setSuccess(true);
        status.setServerIp(endpoint.getIp());
        status.setHttpPort(8080);
        status.setGroup("g");
        return PlacementResult.success(SelectedRole.decode(endpoint.tryHold(), status));
    }

    private static void register(DecodeEndpoint endpoint) {
        EndpointRegistry.getEndpointsByRole(RoleType.DECODE).put(endpoint.getStatus().getLogicalIpPort(), endpoint);
    }

    @SuppressWarnings("unchecked")
    private static void markRequestStale(DecodeEndpoint endpoint, String id) {
        Object state = org.springframework.test.util.ReflectionTestUtils.getField(endpoint, "state");
        Map<String, org.flexlb.balance.endpoint.DecodeRequestState> requests =
                (Map<String, org.flexlb.balance.endpoint.DecodeRequestState>) org.springframework.test.util.ReflectionTestUtils
                        .getField(state, "decodeRequests");
        long staleAtMs = System.currentTimeMillis() - 10_000L;
        requests.get(id).setPublishedAtMs(staleAtMs);
        requests.get(id).setLastSeenAtMs(staleAtMs);
    }

    private static void updateDecodeStatus(DecodeEndpoint endpoint,
                                           Map<String, TaskInfo> active,
                                           Map<String, TaskInfo> finished,
                                           boolean sameVersion) {
        WorkerStatus status = endpoint.getStatus();
        WorkerStatusResponse response = new WorkerStatusResponse();
        response.setAlive(true);
        response.setRole(RoleType.DECODE);
        response.setTotalKvCacheTokens(1000L);
        response.setAvailableKvCacheTokens(1000L);
        response.setRunningTaskInfo(active);
        response.setFinishedTaskInfo(finished);
        response.setStatusVersion(sameVersion
                ? status.appliedStatusCursor().statusVersion()
                : Math.max(1L, status.appliedStatusCursor().statusVersion() + 1));
        Runnable notify;
        status.lock.lock();
        try {
            var observation = status.freezeStatusResponse(response);
            notify = sameVersion
                    ? endpoint.observeStatusHeartbeat(status, observation)
                    : endpoint.applyPreparedStatus(status, status.prepareNewStatus(observation));
        } finally {
            status.lock.unlock();
        }
        notify.run();
    }

    private static TaskInfo task(String id, TaskPhase phase) {
        TaskInfo task = new TaskInfo();
        task.setRequestId(id);
        task.setPhase(phase);
        task.setInputLength(16L);
        return task;
    }
}

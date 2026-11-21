package org.flexlb.balance.endpoint.prefill;

import org.flexlb.balance.endpoint.EndpointGenerationLifecycle;
import org.flexlb.balance.endpoint.EndpointTestSupport;
import org.flexlb.balance.estimate.WorkView;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PrefillStateSnapshotTest {
    private final AtomicLong clock = new AtomicLong(100);
    private final ReentrantLock lock = new ReentrantLock();
    private final PrefillActiveIndex waiting = PrefillActiveIndex.ordered(4,
            Comparator.comparing(ScheduledRequest::requestId));
    private final PrefillState state = new PrefillState(lock, waiting, clock::get, () -> { });
    private final EndpointGenerationLifecycle generation = new EndpointGenerationLifecycle(() -> { });

    @Test
    void workerStatusUnconfirmedCountExcludesQueuedAndConfirmedRequests() {
        enqueue(item(9));
        assertEquals(0, state.stats().waitingForWorkerStatusRequests());
        try (var reservation = reserveQueued(item(8), 10)) {
            assertEquals(0, state.stats().waitingForWorkerStatusRequests());
        }
        var request = item(1);
        try (var reservation = reserveQueued(request, 10);
                var handoff = state.commitSingleGroup(List.of(request), List.of(reservation),
                        generation.tryAcquireHandoff())) {
            assertEquals(1, state.stats().waitingForWorkerStatusRequests());
        }
        reconcile(Map.of(), Map.of("1", task(1, TaskPhase.RUNNING, 0, 0)));
        assertEquals(0, state.stats().waitingForWorkerStatusRequests());
        assertEquals(1, state.stats().committedRequests());
    }

    @Test
    void concurrentReadersShareACompleteImmutableMaterialization() throws Exception {
        ScheduledRequest first = item(1), second = item(2);
        commitRoute(second, 20);
        commitRoute(first, 10);
        var captured = capture();
        try (var executor = Executors.newFixedThreadPool(8)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<WorkView>> readers = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                readers.add(executor.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return captured.work().toWorkView();
                }));
            }
            start.countDown();
            WorkView shared = readers.getFirst().get(5, TimeUnit.SECONDS);
            for (var reader : readers) {
                assertSame(shared, reader.get(5, TimeUnit.SECONDS));
            }
            assertEquals(List.of("1", "2"), shared.requests().stream()
                    .map(WorkView.RequestWork::requestId).toList());
            assertTrue(state.removeCommittedRequest(second));
            assertTrue(state.removeCommittedRequest(first));
            assertEquals(2, shared.requests().size());
            assertTrue(capture().work().toWorkView().requests().isEmpty());
        }
    }

    @Test
    void committedWorkChangesReuseActiveMembershipButCaptureBothAtOnePoint() {
        ScheduledRequest committed = item(2);
        commitRoute(committed, 20);
        var queued = item(1);
        enqueue(queued);
        var before = capture();
        assertTrue(state.removeCommittedRequest(committed));
        var after = capture();
        assertSame(before.waiting(), after.waiting());
        assertEquals(1, before.work().toWorkView().requests().size());
        assertTrue(after.work().toWorkView().requests().isEmpty());
        enqueue(item(3));
        var added = capture();
        assertNotSame(after.waiting(), added.waiting());
        assertSame(after.work(), added.work());
        assertEquals(List.of(queued), before.waitingItems());
    }

    @Test
    void clockRollbackRecapturesWorkWithoutMutatingEarlierSnapshots() {
        commitRoute(item(1), 10);
        var original = capture();
        clock.set(101);
        assertSame(original.work(), capture().work());
        clock.set(90);
        var rebased = capture();
        assertNotSame(original.work(), rebased.work());
        assertEquals(100, original.work().toWorkView().createdAtMs());
        assertEquals(90, rebased.work().toWorkView().createdAtMs());
        assertEquals(rebased.createdAtMs(), rebased.work().toWorkView().createdAtMs());
        assertEquals(original.work().toWorkView().requests(), rebased.work().toWorkView().requests());
    }

    @Test
    void queuedRoutesUseOneCommitAndTerminalOwnership() {
        ScheduledRequest first = item(1), second = item(2);
        try (var firstReservation = reserveQueued(first, 30L); var secondReservation = reserveQueued(second, 40L)) {
            assertEquals(List.of(first, second), waitingItems());
            assertTrue(state.committedSnapshot().requests().isEmpty());
            try (var handoff = state.commitSingleGroup(List.of(first, second),
                    List.of(firstReservation, secondReservation), generation.tryAcquireHandoff())) {
                assertTrue(waitingItems().isEmpty());
                assertEquals(0L, handoff.precedingWork().totalRemainingWorkMs().orElseThrow(),
                        "the selected group's own provisional work is excluded");
                assertEquals(List.of("1", "2"), state.committedSnapshot().requests().stream()
                        .map(work -> work.requestId()).toList());
                assertEquals(70L, state.committedSnapshot().knownRemainingWorkMsAt(System.currentTimeMillis()));
            }
        }
        assertEquals(2, state.committedSnapshot().requests().size(), "closing committed capabilities cannot release Engine ownership");
        assertTrue(state.removeCommittedRequest(first));
        assertTrue(state.removeCommittedRequest(second));
        assertTrue(state.committedSnapshot().requests().isEmpty());
    }

    @Test
    void routeCommitUsesTheUpdatedPrediction() {
        ScheduledRequest queued = item(1);
        try (var reservation = reserveQueued(queued, 40L)) {
            assertTrue(capture().work().toWorkView().requests().isEmpty(), "queued work is not committed work");
            reservation.updatePrediction(queued, 80L);
            try (var handoff =
                    state.commitSingleGroup(List.of(queued), List.of(reservation), generation.tryAcquireHandoff())) {
                assertEquals(80L, capture().work().toWorkView().knownRemainingWorkMsAt(System.currentTimeMillis()));
            }
        }
    }

    @Test
    void leaseRollbackKeepsTheRequestQueued() {
        ScheduledRequest queued = item(1);
        enqueue(queued);
        state.reserveSingle(queued, 30L).reservation().close();
        assertEquals(List.of(queued), waitingItems());
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(state.reserveSingle(queued, 35L).reservation() != null);
    }

    @Test
    void routeCommitRejectsMissingQueueIndexBeforeCommittingAnyMember() {
        ScheduledRequest removed = item(1), kept = item(2);
        try (var removedReservation = reserveQueued(removed, 30L); var keptReservation = reserveQueued(kept, 40L)) {
            assertTrue(waiting.remove(removed));
            try (var handoff = generation.tryAcquireHandoff()) {
                assertThrows(IllegalStateException.class, () -> state.commitSingleGroup(List.of(kept, removed),
                        List.of(keptReservation, removedReservation), handoff));
            }
            assertFalse(state.removeCommittedRequest(kept));
            assertTrue(waiting.add(removed));
            try (var handoff = state.commitSingleGroup(List.of(kept, removed),
                    List.of(keptReservation, removedReservation), generation.tryAcquireHandoff())) {
                assertTrue(waiting.isEmpty());
            }
        }
    }

    @Test
    void retirementHandlesQueuedPreparedAndCommittedItemsWithoutModeExceptions() {
        ScheduledRequest queued = item(1), prepared = item(2), committed = item(3);
        enqueue(queued);
        var preparedReservation = reserveQueued(prepared, 20L);
        var committedReservation = reserveQueued(committed, 30L);
        try (var handoff = state.commitSingleGroup(List.of(committed), List.of(committedReservation),
                generation.tryAcquireHandoff())) {
            assertEquals(1, state.committedSnapshot().requests().size());
        }
        var retired = state.shutdown();
        assertNull(retired.checkFailure());
        assertEquals(3, retired.removedItems().size());
        assertTrue(retired.removedItems().containsAll(List.of(queued, prepared, committed)));
        preparedReservation.close();
        committedReservation.close();
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(waiting.isEmpty());
    }

    @Test
    void cancellationBeforeCommitReleasesAQueuedRouteExactlyOnce() {
        ScheduledRequest queued = item(1);
        var reservation = reserveQueued(queued, 40L);
        lock.lock();
        try {
            assertTrue(state.removeQueuedSingleRequest(queued, reservation));
            assertFalse(state.removeQueuedSingleRequest(queued, reservation));
        } finally {
            lock.unlock();
        }
        reservation.close();
        assertTrue(waitingItems().isEmpty());
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(capture().work().toWorkView().requests().isEmpty());
    }

    @Test
    void interleavedRouteCommitsSeeOnlyWorkCommittedBeforeThem() {
        ScheduledRequest first = item(1), second = item(2);
        try (var firstReservation = reserveQueued(first, 30L); var secondReservation = reserveQueued(second, 40L)) {
            try (var secondHandoff = state.commitSingleGroup(List.of(second), List.of(secondReservation),
                    generation.tryAcquireHandoff())) {
                assertEquals(0L, secondHandoff.precedingWork().totalRemainingWorkMs().orElseThrow(),
                        "a request still waiting in the queue is not preceding work");
            }
            try (var firstHandoff = state.commitSingleGroup(List.of(first), List.of(firstReservation),
                    generation.tryAcquireHandoff())) {
                assertEquals(40L, firstHandoff.precedingWork().totalRemainingWorkMs().orElseThrow(),
                        "the earlier reservation must see work committed before its own commit");
            }
        }
    }

    private org.flexlb.balance.endpoint.prefill.single.SingleReservation reserveQueued(ScheduledRequest item, long predictedMs) {
        enqueue(item);
        return state.reserveSingle(item, predictedMs).reservation();
    }

    private void commitRoute(ScheduledRequest item, long predictedMs) {
        try (var reservation = reserveQueued(item, predictedMs);
                var handoff =
                        state.commitSingleGroup(List.of(item), List.of(reservation), generation.tryAcquireHandoff())) {
            assertTrue(waitingItems().isEmpty());
        }
    }

    private void reconcile(Map<String, TaskInfoPB.Builder> finished,
                           Map<String, TaskInfoPB.Builder> active) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        WorkerStatusPBs.setFinishedTasks(response, finished);
        WorkerStatusPBs.setRunningTasks(response, active);
        var observation = EngineStatusConverter.convertToStatusObservation(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090), response.build());
        var outcome = state.updateFromWorkerStatus(observation, () -> {}, () -> {});
        assertNull(outcome.publicationFailure());
    }

    private long remainingWork() {
        return state.committedSnapshot().totalRemainingWorkMsAt(clock.get()).orElseThrow();
    }

    private static TaskInfoPB.Builder task(long requestId, TaskPhase phase, long errorCode, long executionMs) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(Long.toString(requestId));
        task.setBatchId(10L);
        WorkerStatusPBs.phase(task, phase);
        task.getErrorInfoBuilder().setErrorCode(errorCode);
        task.setExecutionTimeMs(executionMs);
        return task;
    }

    private static ScheduledRequest item(long id) {
        var item = mock(ScheduledRequest.class);
        when(item.requestId()).thenReturn(Long.toString(id));
        when(item.seqLen()).thenReturn(100L);
        return item;
    }

    private PrefillLoadView capture() {
        lock.lock();
        try {
            return state.loadView();
        } finally {
            lock.unlock();
        }
    }

    private void enqueue(ScheduledRequest item) {
        lock.lock();
        try {
            assertTrue(state.enqueueActive(item, 0L));
        } finally {
            lock.unlock();
        }
    }

    private List<ScheduledRequest> waitingItems() {
        return capture().waitingItems();
    }
}

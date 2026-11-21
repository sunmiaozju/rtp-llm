package org.flexlb.balance.endpoint;

import org.flexlb.balance.endpoint.prefill.PrefillActiveIndex;
import org.flexlb.balance.endpoint.prefill.PrefillState;
import org.flexlb.balance.endpoint.prefill.Reservation;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Real endpoint ledgers for scheduler-directory cleanup tests across package boundaries. */
public final class EndpointCleanupTestSupport {
    private EndpointCleanupTestSupport() { }

    public static void confirmDecode(DecodeEndpoint endpoint, String requestId) {
        var response = WorkerStatusPB.newBuilder()
                .addRunningTaskInfo(WorkerStatusPBs.task(requestId, TaskPhase.KV_ALLOCATED))
                .setAvailableKvCache(10_000L)
                .setTotalKvCache(10_000L);
        EndpointTestSupport.applyStatus(endpoint, response).run();
    }

    public static final class PrefillLedger {
        private final AtomicLong clock = new AtomicLong(100);
        private final ReentrantLock lock = new ReentrantLock();
        private final PrefillState state = new PrefillState(lock,
                PrefillActiveIndex.ordered(4, Comparator.comparing(ScheduledRequest::requestId)),
                clock::get, () -> { });
        private final EndpointGenerationLifecycle generation = new EndpointGenerationLifecycle(() -> { });

        public record Owner(ScheduledRequest item, Reservation reservation) {}

        public Owner commit(String requestId, long predictedMs) {
            ScheduledRequest item = mock(ScheduledRequest.class);
            when(item.requestId()).thenReturn(requestId);
            when(item.seqLen()).thenReturn(100L);
            lock.lock();
            try {
                assertTrue(state.enqueueActive(item, 1L), "the expired individual must release its sole capacity slot");
            } finally {
                lock.unlock();
            }
            try (var reservation = state.reserveSingle(item, predictedMs).reservation()) {
                assertNotNull(reservation);
                try (var handoff = state.commitSingleGroup(List.of(item), List.of(reservation),
                        generation.tryAcquireHandoff())) {
                    assertNotNull(handoff);
                }
                return new Owner(item, reservation);
            }
        }

        public void advanceBeyondTtl() {
            clock.addAndGet(100L);
        }

        public int sweep(Predicate<String> retain) {
            return state.evictExpiredSingles(10L, retain);
        }

        public void assertOwned(Owner owner, long predictedMs) {
            var stats = state.stats();
            assertEquals(1, stats.committedRequests());
            assertEquals(1, stats.singleRequests());
            var work = state.committedSnapshot();
            assertEquals(predictedMs, work.totalRemainingWorkMs().orElseThrow());
            assertEquals(0, work.unknownRequestCount());
            assertEquals(List.of(owner.item().requestId()), work.requests().stream()
                    .map(request -> request.requestId()).toList());
        }

        public void assertStaleReleaseIsIgnored(Owner old) {
            assertFalse(state.removeCommittedRequest(old.item()));
            old.reservation().close();
        }

        public void assertEmpty() {
            assertEquals(0, state.stats().committedRequests());
            assertEquals(0, state.stats().singleRequests());
            assertTrue(state.committedSnapshot().requests().isEmpty());
            assertEquals(0L, state.committedSnapshot().totalRemainingWorkMs().orElseThrow());
        }
    }
}

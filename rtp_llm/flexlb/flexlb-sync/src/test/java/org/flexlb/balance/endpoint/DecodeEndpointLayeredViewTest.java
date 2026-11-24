package org.flexlb.balance.endpoint;

import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.balance.eviction.DecodeEndpointSnapshot;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.CapacityLimit;
import org.flexlb.dao.route.DecodeRequirement;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.DecodeMode;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 5 tests for the decode layered view: calibrate splits confirmed
 * requests into accepted / running layers with priority inheritance,
 * the registry follows the WorkerStatus reports (retain / finished / TTL),
 * the shadow accounting invariants stay byte-for-byte Phase 4, and
 * the token-fenced weak-ACK transaction is all-or-nothing, retains victim
 * accounting until typed CANCELED, and provisionally reserves the incoming.
 */
class DecodeEndpointLayeredViewTest {

    private WorkerStatus status;
    private DecodeEndpoint endpoint;
    private final Map<Long, DecodeEndpoint.ReservationHandle> reservations =
            new HashMap<>();

    @BeforeEach
    void setUp() {
        status = EndpointTestSupport.workerStatus(
                RoleType.DECODE, "10.0.0.1", 8080, 8081);
        endpoint = new DecodeEndpoint(
                status, EndpointTestSupport.noopEventSink());
        updateStatus(Map.of(), Map.of(), 20_000);
    }

    // ==================== calibrate: layer split + inheritance ====================

    @Test
    void calibrate_splitsConfirmedIntoAcceptedAndRunningLayers() {
        reserve(1L, 500, 508, 30);
        reserve(2L, 500, 508, 40);

        TaskInfoPB.Builder accepted = runningTask(1L, TaskPhase.KV_ALLOCATED, 256);
        TaskInfoPB.Builder running = runningTask(2L, TaskPhase.RUNNING, 512);
        updateStatus(Map.of("1", accepted, "2", running), null, 10_000);

        assertEquals(1, endpoint.resourceSnapshot().acceptedCount());
        assertEquals(1, endpoint.resourceSnapshot().runningCount());
        assertEquals(2, endpoint.resourceSnapshot().confirmed().size());
        assertEquals(0, endpoint.getInflightCount());
        assertTrue(isConfirmed(1L));
        assertTrue(isConfirmed(2L));

        // Layered view inherits priority from the shadow entry
        // removed this round; KV is the reported inputLength estimate.
        DecodeEndpoint.DecodeRequestView acceptedView = confirmedView(1L);
        assertEquals(DecodeTaskPhase.ACCEPTED_NOT_RUNNING, acceptedView.phase());
        assertEquals(30, acceptedView.priority());
        assertEquals(256, acceptedView.kvTokens());
        assertFalse(acceptedView.claimedForPreemption());

        DecodeEndpoint.DecodeRequestView runningView = confirmedView(2L);
        assertEquals(DecodeTaskPhase.RUNNING, runningView.phase());
        assertEquals(40, runningView.priority());
    }

    @Test
    void calibrate_unknownConfirmedFallsBackToNoPriority() {
        // Report precedes any reserve: no shadow entry to inherit from.
        // Task40: the fallback is the no-priority sentinel (0), which keeps
        // untracked engine tasks out of every eviction candidate set.
        updateStatus(Map.of("9", runningTask(9L, TaskPhase.KV_ALLOCATED, 64)), null, 10_000);

        DecodeEndpoint.DecodeRequestView view = confirmedView(9L);
        assertEquals(0, view.priority());
        assertFalse(view.priorityKnown());
        assertEquals(64, view.kvTokens());
    }

    @Test
    void calibrate_promotesAcceptedToRunningOnRefresh() {
        reserve(1L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);
        assertEquals(1, endpoint.resourceSnapshot().acceptedCount());

        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);

        assertEquals(0, endpoint.resourceSnapshot().acceptedCount());
        assertEquals(1, endpoint.resourceSnapshot().runningCount());
        // Identity fields stay from first sight.
        assertEquals(30, confirmedView(1L).priority());
    }

    // ==================== calibrate: registry follows the reports ====================

    @Test
    void calibrate_keepsLocalRequestUntilFinished() {
        reserve(1L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);
        assertTrue(isConfirmed(1L));

        updateStatus(Map.of(), null, 10_000);

        assertTrue(isConfirmed(1L));
        assertEquals(1, endpoint.resourceSnapshot().acceptedCount());
        assertEquals(1, endpoint.resourceSnapshot().confirmed().size());
        assertEquals(1, endpoint.routingView().engineCapacityUsed());

        updateStatus(Map.of(), Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), 10_000);

        assertFalse(isConfirmed(1L));
        assertEquals(0, endpoint.routingView().engineCapacityUsed());
    }

    @Test
    void calibrate_dropsUntrackedRequestNoLongerReported() {
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);
        assertTrue(isConfirmed(1L));

        updateStatus(Map.of(), null, 10_000);

        assertFalse(isConfirmed(1L));
        assertTrue(endpoint.resourceSnapshot().confirmed().isEmpty());
        assertEquals(0, endpoint.routingView().engineCapacityUsed());
    }

    @Test
    void calibrate_finishedRemovesTrackedEntry() {
        reserve(1L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);
        assertTrue(isConfirmed(1L));

        // Same round lists it both running and finished: finished wins.
        TaskInfoPB.Builder finished = runningTask(1L, TaskPhase.RUNNING, 256);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)),
                Map.of("1", finished), 10_000);

        assertFalse(isConfirmed(1L));
    }

    @Test
    void purgingFinishedHistoryKeepsUnconfirmedAndConfirmedReservations() {
        reserve(1L, 500, 508, 30);
        reserve(2L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);

        endpoint.purgeFinishedRequests(-1L);

        assertTrue(isConfirmed(1L));
        assertTrue(endpoint.resourceSnapshot().reserved().containsKey("2"));
        assertEquals(2, endpoint.routingView().totalLoad());
    }

    @Test
    void finishedHistoryExpiryAllowsLaterWorkerReports() {
        reserve(1L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);
        long version = endpoint.routingView().admissionVersion();
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(101L, List.of(1L),
                        9L, 128, 136, 70));
        updateStatus(Map.of(), Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), 10_000);

        // A delayed Decode report cannot resurrect a recently canceled victim.
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);
        assertFalse(isConfirmed(1L));

        endpoint.purgeFinishedRequests(-1L);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256)), null, 10_000);
        assertTrue(isConfirmed(1L),
                "the cancel fence follows the configured terminal retention TTL");
    }

    // ==================== accounting invariants unchanged (iron rule 5) ====================

    @Test
    void accounting_invariantsStayPhase4Equivalent() {
        reserve(1L, 500, 508, 30);
        reserve(2L, 300, 308, 40);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);

        // realKvAvailable = reportedKvAvailable - remaining shadow hard KV.
        assertEquals(10_000 - 300, endpoint.realKvAvailable());
        assertEquals(300, endpoint.routingView().inputKvReserved());
        // totalLoad = confirmed Engine-owned count + reserved inflight count.
        assertEquals(2, endpoint.routingView().totalLoad());
        assertEquals(1, endpoint.getInflightCount());
    }

    // ==================== token-fenced weak-ACK preemption ====================

    @Test
    void beginPriorityPreemption_claimsVictimAndProvisionallyReservesIncoming() {
        reserve(2L, 400, 408, 30);
        updateStatus(Map.of("2", runningTask(2L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);
        long version = endpoint.routingView().admissionVersion();

        DecodeEndpoint.PreemptionBeginResult result = beginPreemption(
                101L, List.of(2L), 9L, 700, 708, 70);

        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS, result);
        // Preparing the route does not release the victim's existing capacity.
        assertTrue(isConfirmed(2L));
        assertTrue(confirmedView(2L).claimedForPreemption());
        assertTrue(endpoint.resourceSnapshot().reserved().containsKey("9"));
        assertEquals(700, endpoint.routingView().inputKvReserved());
        assertTrue(isConfirmed(2L));
        assertTrue(endpoint.routingView().admissionVersion() > version);
    }

    @Test
    void returnedPreemptionKeepsVictimCapacityUntilItsOwnInactivityExpiry() {
        var victim = reserve(2L, 400L, 408L, 30);
        updateStatus(Map.of("2", runningTask(2L, TaskPhase.RUNNING, 256)), null, 10_000);
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(101L, List.of(2L), 9L, 700L, 708L, 70));
        var incoming = endpoint.reservationHandle("9");
        assertTrue(incoming != null);
        var before = endpoint.resourceSnapshot();
        assertEquals(1, before.runningCount());
        assertEquals(700L, before.routing().inputKvReserved());
        assertEquals(708L, before.routing().inputAndMaxOutputKvReserved());
        assertEquals(before.engineCapacityUsed(), endpoint.resourceSnapshot().engineCapacityUsed());
        assertEquals(before.routing().inputAndMaxOutputKvReserved(), endpoint.routingView().inputAndMaxOutputKvReserved());
        assertEquals(1, endpoint.resourceSnapshot().runningCount());

        // Expiring the incoming request must not release the victim still running on Decode.
        assertTrue(endpoint.release(incoming, DecodeEndpoint.ReleaseReason.EXPIRED).released());
        assertEquals(0L, endpoint.routingView().inputKvReserved());
        assertEquals(0L, endpoint.routingView().inputAndMaxOutputKvReserved());
        assertEquals(1, endpoint.resourceSnapshot().runningCount());
        assertEquals(1, endpoint.routingView().engineCapacityUsed());
        assertTrue(endpoint.release(victim, DecodeEndpoint.ReleaseReason.EXPIRED).released());
        assertFalse(endpoint.release(victim, DecodeEndpoint.ReleaseReason.EXPIRED).released());
        var after = endpoint.resourceSnapshot();
        assertEquals(0, after.runningCount());
        assertEquals(0, after.acceptedCount());
        assertEquals(0, after.activeDispatchPermits());
        assertEquals(0, after.engineCapacityUsed());
        assertTrue(after.reserved().isEmpty());
        assertTrue(after.confirmed().isEmpty());
    }

    @Test
    void beginPriorityPreemption_exactIdentityMismatch_appliesNothing() {
        reserve(1L, 500, 508, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);

        DecodeEndpoint.ReservationHandle exact = reservations.get(1L);
        DecodeEndpoint.ReservationHandle stale =
                new DecodeEndpoint.ReservationHandle(
                        exact.endpointGenerationId(),
                        exact.requestId(),
                        exact.reservationToken() + 1L);
        DecodeEndpoint.PreemptionBeginResult result = endpoint.beginReturnedPreemption(101L, List.of(stale), "9", 700,
                708, 70, new CapacityLimit(1, 100));

        assertEquals(DecodeEndpoint.PreemptionBeginResult.VICTIM_GONE, result);
        assertFalse(confirmedView(1L).claimedForPreemption());
        assertFalse(endpoint.resourceSnapshot().reserved().containsKey("9"));
    }

    @Test
    void beginPriorityPreemption_victimGone_isAllOrNothing() {
        reserve(2L, 400, 408, 30);
        updateStatus(Map.of("2", runningTask(2L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);
        long version = endpoint.routingView().admissionVersion();

        assertEquals(DecodeEndpoint.PreemptionBeginResult.VICTIM_GONE,
                beginPreemption(101L, List.of(2L, 999L),
                        9L, 700, 708, 70));
        assertFalse(confirmedView(2L).claimedForPreemption());
        assertFalse(endpoint.resourceSnapshot().reserved().containsKey("9"));
        assertEquals(version, endpoint.routingView().admissionVersion());
    }

    @Test
    void beginPriorityPreemption_acceptsRunningAndRejectsAlreadyClaimedVictims() {
        reserve(1L, 500, 508, 30);
        reserve(2L, 400, 408, 30);
        updateStatus(Map.of("1", runningTask(1L, TaskPhase.RUNNING, 256),
                "2", runningTask(2L, TaskPhase.KV_ALLOCATED, 256)), null, 10_000);

        // RUNNING is engine-owned too and follows the same cancel path.
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(101L, List.of(1L),
                        9L, 700, 708, 70));

        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(102L, List.of(2L),
                        10L, 700, 708, 70));
        assertEquals(DecodeEndpoint.PreemptionBeginResult.VICTIM_ALREADY_CLAIMED,
                beginPreemption(103L, List.of(2L),
                        11L, 700, 708, 70));
    }

    @Test
    void purgingFinishedHistoryKeepsClaimedEngineVisibleShadow() {
        reserve(1L, 500, 508, 30);
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(101L, List.of(1L),
                        9L, 700, 708, 70));

        endpoint.purgeFinishedRequests(-1L);
        assertTrue(endpoint.resourceSnapshot().reserved().containsKey("1"),
                "finished history cleanup must not deduct a claimed victim");
        assertEquals(1_200, endpoint.routingView().inputKvReserved(),
                "victim and provisional incoming remain fully charged");

        assertTrue(endpoint.release(endpoint.reservationHandle("9"), DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK)
                .released());
        assertTrue(endpoint.release(reservations.get(1L), DecodeEndpoint.ReleaseReason.NOT_SENT).released());
        assertFalse(endpoint.resourceSnapshot().reserved().containsKey("1"));
        assertEquals(0, endpoint.routingView().inputKvReserved());
    }

    // ==================== snapshot capture: layered lists ====================

    @Test
    void returnSnapshot_splitsLayersAndExcludesIneligibleRequests() {
        reserve(1L, 500, 508, 30);
        reserve(2L, 400, 408, 30);
        reserve(3L, 300, 308, 30);
        endpoint.reserve("4", 100, 108, 20);
        reserve(5L, 100, 108, 70);
        updateStatus(Map.of("2", runningTask(2L, TaskPhase.KV_ALLOCATED, 256),
                "3", runningTask(3L, TaskPhase.RUNNING, 512),
                "6", runningTask(6L, TaskPhase.RUNNING, 128)), null, 10_000);

        DecodeEndpointSnapshot snapshot = returnSnapshot(6L);
        assertNotNull(snapshot);
        assertEquals(endpoint.routingView().placementUsage(), snapshot.usage());
        assertEquals(List.of("1"), ids(snapshot.reserved()));
        assertEquals(List.of("2"), ids(snapshot.accepted()));
        assertEquals(List.of("3"), ids(snapshot.running()));
        DecodeRequestView accepted = snapshot.accepted().get(0);
        assertEquals(DecodeTaskPhase.ACCEPTED_NOT_RUNNING, accepted.phase());
        assertEquals(256, accepted.kvTokens());

        // A cancel-requested entry is claimed by an in-flight eviction and
        // must not be offered to planning again.
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(101L, List.of(2L), 20L, 64, 72, 70));
        assertEquals(DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                beginPreemption(102L, List.of(3L), 30L, 64, 72, 70));
        DecodeEndpointSnapshot after = returnSnapshot(6L);
        assertNotNull(after);
        assertTrue(after.accepted().isEmpty());
        assertTrue(after.running().isEmpty());
    }

    @Test
    void returnSnapshot_skipsWhenPlacementFitsOrThereAreNoVictims() {
        var lowerPriority = reserve(1L, 100, 108, 30);
        reserve(2L, 100, 108, 70);

        assertNull(returnSnapshot(3L));
        assertNotNull(returnSnapshot(2L));

        endpoint.release(lowerPriority, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        assertNull(returnSnapshot(1L));
    }

    private DecodeEndpointSnapshot returnSnapshot(long maxEngineRequests) {
        var requirement = new DecodeRequirement("99", 70, 64L, 72L, new CapacityLimit(maxEngineRequests, 90L),
                DecodeMode.PREEMPT_AT_PLACEMENT, null);
        return DecodeEndpointSnapshot.captureForReturn(endpoint, requirement);
    }

    // ==================== helpers ====================

    private static List<String> ids(List<DecodeRequestView> entries) {
        return entries.stream().map(DecodeRequestView::requestId).toList();
    }

    private DecodeEndpoint.DecodeRequestView confirmedView(long requestId) {
        return endpoint.resourceSnapshot().confirmed().stream()
                .filter(view -> view.requestId().equals(Long.toString(requestId)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("request " + requestId + " not tracked"));
    }

    private void updateStatus(Map<String, TaskInfoPB.Builder> running,
                              Map<String, TaskInfoPB.Builder> finished,
                              long availableKvCacheTokens) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        WorkerStatusPBs.setRunningTasks(response, running);
        WorkerStatusPBs.setFinishedTasks(response, finished);
        response.setAvailableKvCache(availableKvCacheTokens);
        response.setTotalKvCache(20_000L);
        EndpointTestSupport.applyStatus(endpoint, response);
    }

    private DecodeEndpoint.ReservationHandle reserve(long requestId, long requiredKv, long kvBudget, int priority) {
        DecodeEndpoint.ReservationHandle reservation =
                endpoint.reserveUnqueued(Long.toString(requestId), requiredKv, kvBudget, priority);
        reservations.put(requestId, reservation);
        return reservation;
    }

    private boolean isConfirmed(long requestId) {
        return endpoint.resourceSnapshot().confirmed().stream()
                .anyMatch(view -> view.requestId().equals(Long.toString(requestId)));
    }

    private DecodeEndpoint.PreemptionBeginResult beginPreemption(long attemptToken,
                                                                 List<Long> victimIds,
                                                                 long incomingRequestId,
                                                                 long requiredKv,
                                                                 long kvBudget,
                                                                 int priority) {
        List<DecodeEndpoint.ReservationHandle> victims = victimIds.stream()
                .map(reservations::get)
                .toList();
        return endpoint.beginReturnedPreemption(attemptToken, victims, Long.toString(incomingRequestId), requiredKv,
                kvBudget, priority,
                new CapacityLimit(Math.max(1, endpoint.routingView().totalLoad()), 100));
    }

    private static TaskInfoPB.Builder runningTask(long requestId, TaskPhase phase, long inputLength) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(Long.toString(requestId));
        WorkerStatusPBs.phase(task, phase);
        task.setInputLength(inputLength);
        return task;
    }
}

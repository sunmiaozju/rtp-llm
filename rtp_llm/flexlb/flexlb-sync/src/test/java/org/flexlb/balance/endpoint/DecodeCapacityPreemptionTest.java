package org.flexlb.balance.endpoint;

import org.flexlb.balance.eviction.DecodeEndpointSnapshot;
import org.flexlb.balance.eviction.DecodeEvictionProposal;
import org.flexlb.balance.eviction.EvictionPlanner;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.route.CapacityDeficit;
import org.flexlb.dao.route.CapacityLimit;
import org.flexlb.dao.route.DecodeRequirement;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.DecodeMode;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercise one demand and budget through selection, planning and authoritative commit. */
class DecodeCapacityPreemptionTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void outputBudgetRejectionProducesAPlanAndCommitRechecksCurrentUsage(boolean usageChanged) {
        DecodeEndpoint endpoint = endpoint(400L);
        CapacityLimit policy = new CapacityLimit(0L, 90L);
        long requiredKvTokens = 100L;
        long kvBudgetTokens = 250L;
        DecodeEndpoint.ReservationHandle victim;
        victim = endpoint.reserveUnqueued("1", 100L, 200L, 30);
        var reservation = endpoint.reserve("9", requiredKvTokens, kvBudgetTokens, 70);
        assertNotNull(reservation);
        assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.CAPACITY_FULL,
                endpoint.acquireDispatchPermit(reservation, policy).status());
        endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        var view = endpoint.routingView();
        assertTrue(view.realKvAvailable() >= requiredKvTokens);
        assertFalse(policy.evaluate(view.dispatchUsage(), requiredKvTokens, kvBudgetTokens).hasCapacity());
        DecodeEvictionProposal proposal = plan(endpoint, requiredKvTokens, kvBudgetTokens, policy, VictimStage.DECODE_ENGINE_OWNED);
        assertEquals(DecodeEvictionProposal.CASE_KV, proposal.evictionCase());
        assertEquals(List.of("1"), proposal.victims().stream().map(DecodeEndpoint.DecodeRequestView::requestId).toList());
        if (usageChanged) { updateCapacity(endpoint, 250L); }
        assertEquals(
                usageChanged
                        ? DecodeEndpoint.PreemptionBeginResult.INFEASIBLE
                        : DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                endpoint.beginReturnedPreemption(1L, List.of(victim), "9", requiredKvTokens, kvBudgetTokens, 70,
                        policy));
        assertNotNull(endpoint.reservationHandle("1"));
        if (usageChanged) { assertNull(endpoint.reservationHandle("9")); }
    }

    @Test
    void queuedReservationReleasesThePlacementRequestCharge() {
        DecodeEndpoint endpoint = endpoint(1000L);
        CapacityLimit policy = new CapacityLimit(1L, 90L);
        long requiredKvTokens = 100L;
        long kvBudgetTokens = 100L;
        DecodeEndpoint.ReservationHandle victim;
        victim = endpoint.reserve("1", 100L, 200L, 30);
        assertNull(endpoint.reserve("9", requiredKvTokens, kvBudgetTokens, 70, policy));
        // The same queued reservation remains soft for ordinary Engine dispatch.
        assertTrue(policy.evaluate(endpoint.routingView().dispatchUsage(), requiredKvTokens, kvBudgetTokens)
                .hasCapacity());
        DecodeEvictionProposal proposal = plan(endpoint, requiredKvTokens, kvBudgetTokens, policy, VictimStage.DECODE_RESERVED);
        assertEquals(DecodeEvictionProposal.CASE_SLOT, proposal.evictionCase());
        assertTrue(endpoint.replaceQueuedRequests(List.of(victim), "9", requiredKvTokens, kvBudgetTokens, 70, policy));
        assertNull(endpoint.reservationHandle("1"));
        assertNotNull(endpoint.reservationHandle("9"));
    }

    @Test
    void zeroPromptVictimCanReleaseItsFullOutputReservation() {
        DecodeEndpoint endpoint = endpoint(1000L);
        CapacityLimit policy = new CapacityLimit(0L, 90L);
        long requiredKvTokens = 100L;
        long kvBudgetTokens = 200L;
        DecodeEndpoint.ReservationHandle victim;
        victim = endpoint.reserve("1", 0L, 900L, 30);
        DecodeEvictionProposal proposal = plan(endpoint, requiredKvTokens, kvBudgetTokens, policy, VictimStage.DECODE_RESERVED);
        assertEquals(List.of("1"), proposal.victims().stream().map(DecodeEndpoint.DecodeRequestView::requestId).toList());
        assertTrue(endpoint.replaceQueuedRequests(List.of(victim), "9", requiredKvTokens, kvBudgetTokens, 70, policy));
        assertEquals(200L, endpoint.routingView().inputAndMaxOutputKvReserved());
    }

    @ParameterizedTest
    @CsvSource({"2, 90", "0, 60", "2, 60"})
    void returnSnapshotMatchesFullSnapshotForSlotAndKvDeficits(long maxEngineRequests, long maxKvUsagePercent) {
        DecodeEndpoint endpoint = endpoint(800L);
        endpoint.reserveUnqueued("1", 60L, 120L, 30);
        endpoint.reserveUnqueued("2", 60L, 120L, 40);
        CapacityLimit policy = new CapacityLimit(maxEngineRequests, maxKvUsagePercent);
        var requirement = new DecodeRequirement("9", 70, 100L, 200L, policy, DecodeMode.PREEMPT_AT_PLACEMENT, null);

        var fullPlan = plan(endpoint, 100L, 200L, policy, VictimStage.DECODE_ENGINE_OWNED);
        var returnSnapshot = DecodeEndpointSnapshot.captureForReturn(endpoint, requirement);

        assertNotNull(returnSnapshot);
        assertEquals(fullPlan, plan(returnSnapshot, 100L, 200L, VictimStage.DECODE_ENGINE_OWNED));
    }

    @Test
    void returnSnapshotPlansEnoughVictimsForPhysicalKvDeficit() {
        DecodeEndpoint endpoint = endpoint(40L);
        assertNotNull(endpoint.reserveUnqueued("1", 40L, 40L, 30));
        assertNotNull(endpoint.reserveUnqueued("2", 70L, 70L, 40));
        var status = WorkerStatusPB.newBuilder()
                .setTotalKvCache(1000L)
                .setAvailableKvCache(40L)
                .addRunningTaskInfo(WorkerStatusPBs.task("1", TaskPhase.KV_ALLOCATED).setInputLength(40L))
                .addRunningTaskInfo(WorkerStatusPBs.task("2", TaskPhase.RUNNING).setInputLength(70L));
        EndpointTestSupport.applyStatus(endpoint, status).run();
        CapacityLimit policy = new CapacityLimit(0L, 100L);
        var requirement = new DecodeRequirement("9", 70, 100L, 130L, policy, DecodeMode.PREEMPT_AT_PLACEMENT, null);

        var snapshot = DecodeEndpointSnapshot.captureForReturn(endpoint, requirement);

        assertNotNull(snapshot);
        var usage = snapshot.usage();
        long requiredKv = requirement.inputKvTokens();
        long budgetKv = requirement.inputAndMaxOutputKvTokens();
        assertEquals(new CapacityDeficit(0L, 60L, 90L), policy.evaluate(usage, requiredKv, budgetKv));
        var proposal = plan(snapshot, requiredKv, budgetKv, VictimStage.DECODE_ENGINE_OWNED);
        assertEquals(DecodeEvictionProposal.CASE_KV, proposal.evictionCase());
        assertEquals(List.of("1", "2"), proposal.victims().stream().map(DecodeEndpoint.DecodeRequestView::requestId).toList());
        var firstRelease = proposal.victims().getFirst().placementRelease();
        assertEquals(20L, policy.evaluate(usage, requiredKv, budgetKv, firstRelease).requiredKvTokens());
        var combinedRelease = firstRelease.plus(proposal.victims().getLast().placementRelease());
        assertTrue(policy.evaluate(usage, requiredKv, budgetKv, combinedRelease).hasCapacity());
    }

    private static DecodeEvictionProposal plan(DecodeEndpoint endpoint,
                                               long requiredKvTokens,
                                               long kvBudgetTokens,
                                               CapacityLimit policy,
                                               VictimStage stage) {
        var resources = endpoint.resourceSnapshot();
        var snapshot = new DecodeEndpointSnapshot(endpoint, endpoint.ipPort(), policy, resources.routing().placementUsage(),
                List.copyOf(resources.reserved().values()), List.of(), List.of());
        return plan(snapshot, requiredKvTokens, kvBudgetTokens, stage);
    }

    private static DecodeEvictionProposal plan(DecodeEndpointSnapshot snapshot,
                                               long requiredKvTokens,
                                               long kvBudgetTokens,
                                               VictimStage stage) {
        PreemptionConfig preemption = new PreemptionConfig();
        preemption.setAllowedVictimStages(EnumSet.of(stage));
        var failures = new HashMap<String, String>();
        DecodeEvictionProposal proposal = EvictionPlanner.planDecode(70, requiredKvTokens, kvBudgetTokens,
                List.of(snapshot), preemption, failures);
        assertNotNull(proposal, failures.toString());
        return proposal;
    }

    private static DecodeEndpoint endpoint(long availableKv) {
        DecodeEndpoint endpoint = new DecodeEndpoint(
                EndpointTestSupport.workerStatus(RoleType.DECODE, "10.0.0.1", 8080, 8081),
                EndpointTestSupport.noopEventSink());
        updateCapacity(endpoint, availableKv);
        return endpoint;
    }

    private static void updateCapacity(DecodeEndpoint endpoint, long availableKv) {
        var status = WorkerStatusPB.newBuilder().setTotalKvCache(1000L).setAvailableKvCache(availableKv);
        EndpointTestSupport.applyStatus(endpoint, status).run();
    }
}

package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryBlockReason;
import org.flexlb.balance.delivery.DeliveryGroup;
import org.flexlb.balance.delivery.DeliveryMetrics;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.delivery.DeliveryTransaction;
import org.flexlb.balance.delivery.NonBatchTransaction;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.prefill.CommitResult;
import org.flexlb.balance.endpoint.prefill.single.SingleReservation;
import org.flexlb.balance.estimate.PrefillTtftEstimate;
import org.flexlb.balance.estimate.WorkView;
import org.flexlb.balance.prediction.DecodeCostFormula;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.scheduler.RequestSlot.DeliveryClaim;
import org.flexlb.balance.scheduler.decode.DecodeDecideInfo;
import org.flexlb.balance.scheduler.decode.DecodeMode;
import org.flexlb.balance.scheduler.decode.DecodeRequirement;
import org.flexlb.balance.scheduler.decode.DecodeSelection;
import org.flexlb.dao.route.RoleType;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

/** Exact-capability fakes for final delivery-strategy tests. */
public final class DeliveryStrategyTestSupport {

    static final PrefillTimePredictor.Evaluator EVALUATOR =
            new PrefillTimePredictor.Evaluator() {
                @Override
                public long estimateMs(long inputTokens, long computeTokens, long hitCacheTokens) {
                    return computeTokens;
                }

                @Override
                public double predictBatchMs(PrefillBatchFeatures features) {
                    return features.items().stream()
                            .mapToLong(PrefillBatchFeatures.Item::seqLen)
                            .sum();
                }
            };

    public static void stubRouteDelivery(RequestRegistry requests,
                                         java.util.function.BiConsumer<DeliveryClaim, DeliveryResult> completed) {
        Mockito.doAnswer(invocation -> invocation.getArgument(1, NonBatchTransaction.class)
                .append(invocation.getArgument(0), invocation.getArgument(2)))
                .when(requests)
                .prepareRouteMember(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            if (!invocation.getArgument(1, PrefillAdmissionResources.CommittedAdmissionOwner.class)
                    .transferToEndpoint(invocation.getArgument(0))) { return null; }
            DeliveryClaim claim = Mockito.mock(DeliveryClaim.class);
            ReflectionTestUtils.setField(claim, "item", invocation.getArgument(0));
            Mockito.doAnswer(inv -> { completed.accept(claim, DeliveryResult.delivered()); return null; })
                    .when(requests).publishRoute(Mockito.eq(claim), Mockito.any(), Mockito.anyLong());
            return claim;
        }).when(requests).claimRouteDelivery(Mockito.any(), Mockito.any());
    }

    private DeliveryStrategyTestSupport() {
    }

    static ScheduledRequest item(long requestId) {
        return item(requestId, 50, requestId, 100L, 10L);
    }

    static ScheduledRequest item(
            long requestId,
            int priority,
            long enqueuedAtMs,
            long seqLen,
            long hitCache) {
        return item(Long.toString(requestId), priority, enqueuedAtMs, seqLen, hitCache);
    }

    static ScheduledRequest item(
            String requestId,
            int priority,
            long enqueuedAtMs,
            long seqLen,
            long hitCache) {
        ScheduledRequest item = Mockito.mock(ScheduledRequest.class);
        Mockito.when(item.ctx()).thenReturn(new org.flexlb.dao.BalanceContext(SchedulingTestConfig.routeConfig()));
        Mockito.when(item.requestId()).thenReturn(requestId);
        Mockito.when(item.priority()).thenReturn(priority);
        Mockito.when(item.enqueuedAtMs()).thenReturn(enqueuedAtMs);
        Mockito.when(item.seqLen()).thenReturn(seqLen);
        Mockito.when(item.hitCache()).thenReturn(hitCache);
        return item;
    }

    record TestBoundary(ScheduledRequest item, DeliveryBlockReason result) {
    }

    static final class TestContext {

        private boolean commit = true;
        private DeliveryTransaction preparedSelection;
        private TestBoundary committedBoundary;
        private TestBoundary emptyBoundary;

        String deliver(DeliveryStrategy strategy,
                       List<ScheduledRequest> candidates,
                       String decisionReason,
                       int remainingQueueDepth,
                       OptionalLong plannedPredictionMs) {
            try (DeliveryTransaction transaction = strategy.prepare(new DeliveryGroup(candidates, EVALUATOR, plannedPredictionMs))) {
                if (transaction.requests().isEmpty()) {
                    emptyBoundary = new TestBoundary(
                            transaction.blocked().item(),
                            transaction.blocked().reason());
                    return "BOUNDARY";
                }
                preparedSelection = transaction;
                if (transaction.blocked() != null) {
                    committedBoundary = new TestBoundary(
                            transaction.blocked().item(),
                            transaction.blocked().reason());
                }
                if (!commit) {
                    return "NOT_COMMITTED";
                }
                WorkView precedingWork = transaction.commit();
                transaction.deliver(decisionReason, remainingQueueDepth, precedingWork);
                return "COMMITTED";
            }
        }

        void commit(boolean value) {
            commit = value;
        }

        DeliveryTransaction preparedSelection() {
            return preparedSelection;
        }

        TestBoundary committedBoundary() {
            return committedBoundary;
        }

        TestBoundary emptyBoundary() {
            return emptyBoundary;
        }

    }

    /** Concrete endpoint capabilities used by the real delivery transaction. */
    static final class TestEndpointCapabilities {
        private WorkView precedingWork = new WorkView(1_000L, List.of(), 0L);
        private final PrefillEndpoint prefill = Mockito.mock(PrefillEndpoint.class);
        private final PrefillEndpoint.RouteCommitAdmission routeCommit =
                Mockito.mock(PrefillEndpoint.RouteCommitAdmission.class);
        private final DecodeEndpoint decode = Mockito.mock(DecodeEndpoint.class);
        private final Map<ScheduledRequest, SingleReservation>
                singleReservations = new IdentityHashMap<>();
        private final Map<ScheduledRequest, DecodeEndpoint.EngineDispatchPermit>
                permits = new IdentityHashMap<>();
        private final Map<String, ScheduledRequest> itemsByRequestId =
                new HashMap<>();
        private final List<CommitResult> commitResults = new ArrayList<>();
        private int permitAttempt;
        private int rejectPermitAt = -1;

        TestEndpointCapabilities() {
            Mockito.when(prefill.canDeliver(Mockito.anyLong(), Mockito.anyInt())).thenReturn(true);
            Mockito.when(prefill.tryBeginRouteCommitAdmission())
                    .thenReturn(routeCommit);
            Mockito.when(routeCommit.commit(Mockito.anyList(), Mockito.anyList()))
                    .thenAnswer(invocation -> commitResults(1).getFirst());
            Mockito.when(decode.acquireDispatchPermit(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> acquirePermit(
                            ((DecodeEndpoint.ReservationHandle) invocation.getArgument(0)).requestId()));
        }

        void bind(ScheduledRequest... items) {
            for (ScheduledRequest item : items) {
                String requestId = item.requestId();
                itemsByRequestId.put(requestId, item);
                DecodeEndpoint.ReservationHandle reservation = Mockito.mock(
                        DecodeEndpoint.ReservationHandle.class);
                Mockito.when(reservation.requestId()).thenReturn(requestId);
                SingleReservation singleReservation = Mockito.mock(
                        SingleReservation.class);
                singleReservations.put(item, singleReservation);
                Mockito.when(item.prefillEp()).thenReturn(prefill);
                Mockito.when(item.requiresSingleReservation()).thenReturn(true);
                Mockito.when(item.publishedSingleReservation())
                        .thenReturn(singleReservation);
                Mockito.when(item.takePublishedSingleReservation(singleReservation))
                        .thenReturn(true);
                DecodeRequirement requirement = new DecodeRequirement(requestId, item.priority(), item.seqLen(),
                        item.seqLen(), new DecodeEndpoint.CapacityLimit(0L, 100L), DecodeMode.WAIT_AT_PLACEMENT,
                        DecodeCostFormula.parse("kvcache_used_ratio"));
                DecodeDecideInfo decide =
                        new DecodeDecideInfo(requirement, new DecodeSelection(null, decode, reservation));
                Mockito.when(item.decodeDecideInfo()).thenReturn(decide);
            }
        }

        void rejectPermitAt(int preparedSize) {
            rejectPermitAt = preparedSize;
        }

        void precedingWork(WorkView prediction) {
            precedingWork = prediction;
        }

        SingleReservation singleReservation(ScheduledRequest item) {
            return singleReservations.get(item);
        }

        PrefillEndpoint prefill() {
            return prefill;
        }

        PrefillEndpoint.RouteCommitAdmission routeCommit() {
            return routeCommit;
        }

        DecodeEndpoint.EngineDispatchPermit permit(ScheduledRequest item) {
            return permits.get(item);
        }

        List<CommitResult> commitResults() {
            return List.copyOf(commitResults);
        }

        private DecodeEndpoint.EngineDispatchPermitAcquisition acquirePermit(String requestId) {
            if (permitAttempt++ == rejectPermitAt) {
                return new DecodeEndpoint.EngineDispatchPermitAcquisition(
                        DecodeEndpoint.EngineDispatchPermitAcquireStatus.CAPACITY_FULL,
                        null);
            }
            ScheduledRequest item = itemsByRequestId.get(requestId);
            DecodeEndpoint.EngineDispatchPermit permit = Mockito.mock(
                    DecodeEndpoint.EngineDispatchPermit.class);
            Mockito.when(permit.dispatch()).thenReturn(
                    DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED);
            if (item != null) {
                permits.put(item, permit);
            }
            return new DecodeEndpoint.EngineDispatchPermitAcquisition(
                    DecodeEndpoint.EngineDispatchPermitAcquireStatus.SUCCESS, permit);
        }

        private List<CommitResult> commitResults(int count) {
            List<CommitResult> committed = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                CommitResult commitResult = Mockito.mock(CommitResult.class);
                Mockito.when(commitResult.precedingWork()).thenReturn(precedingWork);
                commitResults.add(commitResult);
                committed.add(commitResult);
            }
            return committed;
        }
    }

    static final class TestRequestRegistry {

        private final RequestRegistry requests = Mockito.mock(RequestRegistry.class);
        private final List<ScheduledRequest> prepared = new ArrayList<>();
        private final List<ScheduledRequest> committed = new ArrayList<>();
        private final List<ClaimIdentity> identities = new ArrayList<>();
        private final List<CompletionEvent> completions = new ArrayList<>();
        private final Map<ScheduledRequest, WorkView> precedingWork = new IdentityHashMap<>();
        private final Map<ScheduledRequest, Long> unstartedWorkMs = new IdentityHashMap<>();
        private final List<ScheduledRequest> failedPrepared = new ArrayList<>();
        private final List<Throwable> preparedFailures = new ArrayList<>();
        private final List<String> events = new ArrayList<>();
        private ScheduledRequest preparationLostFor;
        private ScheduledRequest commitLostFor;
        private ScheduledRequest throwCommitFor;
        private ScheduledRequest throwCompletionFor;
        private Runnable beforeCompletion = () -> { };

        TestRequestRegistry() {
            Mockito.doAnswer(invocation -> {
                ScheduledRequest item = invocation.getArgument(0);
                if (item == preparationLostFor) { return DeliveryBlockReason.Attempt.rejected(DeliveryBlockReason.OWNERSHIP_LOST); }
                prepared.add(item);
                return invocation.getArgument(1, NonBatchTransaction.class).append(item, invocation.getArgument(2));
            }).when(requests).prepareRouteMember(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.doAnswer(invocation -> claim(invocation.getArgument(0), DeliveryClaimKind.ROUTE_DECISION, 0L,
                    () -> invocation.getArgument(1, PrefillAdmissionResources.CommittedAdmissionOwner.class)
                            .transferToEndpoint(invocation.getArgument(0))))
                    .when(requests).claimRouteDelivery(Mockito.any(), Mockito.any());
            Mockito.doAnswer(invocation -> {
                failDeliveryPreparation(invocation.getArgument(0), invocation.getArgument(1));
                return null;
            }).when(requests).failDeliveryPreparation(Mockito.any(), Mockito.any());
        }

        private DeliveryClaim claim(
                ScheduledRequest exactItem,
                DeliveryClaimKind kind,
                long correlationId,
                BooleanSupplier endpointHandoff) {
            committed.add(exactItem);
            identities.add(new ClaimIdentity(kind, correlationId));
            if (exactItem == throwCommitFor) {
                throw new IllegalStateException(
                        "synthetic slot commit failure "
                                + exactItem.requestId());
            }
            if (exactItem == commitLostFor) {
                return null;
            }
            if (!endpointHandoff.getAsBoolean()) {
                return null;
            }
            events.add("point-of-no-return-" + exactItem.requestId());
            DeliveryClaim claim =
                    Mockito.mock(DeliveryClaim.class);
            ReflectionTestUtils.setField(claim, "item", exactItem);
            Mockito.doAnswer(inv -> {
                precedingWork.put(exactItem, inv.getArgument(1));
                unstartedWorkMs.put(exactItem, inv.getArgument(2));
                complete(claim, DeliveryResult.delivered());
                return null;
            }).when(requests).publishRoute(Mockito.eq(claim), Mockito.any(), Mockito.anyLong());
            return claim;
        }

        private void complete(
                DeliveryClaim exactClaim,
                DeliveryResult completion) {
            ScheduledRequest item = (ScheduledRequest) ReflectionTestUtils.getField(exactClaim, "item");
            beforeCompletion.run();
            completions.add(new CompletionEvent(item, completion));
            events.add("complete-" + item.requestId());
            if (item == throwCompletionFor) {
                throw new IllegalStateException(
                        "synthetic completion failure "
                                + item.requestId());
            }
        }

        private void failDeliveryPreparation(ScheduledRequest exactItem, Throwable cause) {
            failedPrepared.add(exactItem);
            preparedFailures.add(cause);
        }

        void preparationLostFor(ScheduledRequest item) {
            preparationLostFor = item;
        }

        void commitLostFor(ScheduledRequest item) {
            commitLostFor = item;
        }

        void throwCommitFor(ScheduledRequest item) {
            throwCommitFor = item;
        }

        void throwCompletionFor(ScheduledRequest item) {
            throwCompletionFor = item;
        }

        void beforeCompletion(Runnable check) {
            beforeCompletion = check;
        }

        List<ScheduledRequest> prepared() {
            return List.copyOf(prepared);
        }

        List<ScheduledRequest> committed() {
            return List.copyOf(committed);
        }

        List<ClaimIdentity> identities() {
            return List.copyOf(identities);
        }

        List<CompletionEvent> completions() {
            return List.copyOf(completions);
        }

        Map<ScheduledRequest, Long> unstartedWorkMs() {
            return Map.copyOf(unstartedWorkMs);
        }

        WorkView precedingWork(ScheduledRequest item) {
            return precedingWork.get(item);
        }

        OptionalLong remainingWorkMsAt(ScheduledRequest item, long nowMs) {
            OptionalLong precedingMs = precedingWork.get(item).totalRemainingWorkMsAt(nowMs);
            if (precedingMs.isEmpty()) { return OptionalLong.empty(); }
            long additionalMs = unstartedWorkMs.get(item);
            return OptionalLong.of(precedingMs.getAsLong() > Long.MAX_VALUE - additionalMs
                    ? Long.MAX_VALUE : precedingMs.getAsLong() + additionalMs);
        }

        List<ScheduledRequest> failedPrepared() {
            return List.copyOf(failedPrepared);
        }

        List<Throwable> preparedFailures() {
            return List.copyOf(preparedFailures);
        }

        List<String> events() {
            return List.copyOf(events);
        }

        RequestRegistry requests() {
            return requests;
        }
    }

    record ClaimIdentity(DeliveryClaimKind kind, long correlationId) {
    }

    record CompletionEvent(
            ScheduledRequest item,
            DeliveryResult completion) {
    }

    static final class TestTelemetry {

        private final List<List<ScheduledRequest>> routes = new ArrayList<>();
        private final DeliveryMetrics metrics =
                Mockito.mock(DeliveryMetrics.class);

        TestTelemetry() {
            Mockito.doAnswer(invocation -> {
                routesDelivered(invocation.getArgument(0), invocation.getArgument(1));
                return null;
            }).when(metrics).routesDelivered(
                    org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyList());
        }

        private void routesDelivered(
                int remainingQueueDepth,
                List<ScheduledRequest> exactItems) {
            routes.add(List.copyOf(exactItems));
        }

        List<List<ScheduledRequest>> routes() {
            return List.copyOf(routes);
        }

        DeliveryMetrics metrics() {
            return metrics;
        }
    }

    static DeliveryBlockReason unavailable() {
        return DeliveryBlockReason.unavailable(
                new DeliveryBlockReason.Availability() {
                    @Override
                    public boolean isAvailable() {
                        return false;
                    }

                    @Override
                    public void addListener(Runnable listener) {
                    }

                    @Override
                    public void removeListener(Runnable listener) {
                    }
                },
                new PrefillTtftEstimate.AdmissionBlockSemantics(
                        "TEST_BLOCK",
                        PrefillTtftEstimate.AfterProbeAdmission.BLOCKED,
                        "TEST_BLOCK",
                        RoleType.PREFILL));
    }
}

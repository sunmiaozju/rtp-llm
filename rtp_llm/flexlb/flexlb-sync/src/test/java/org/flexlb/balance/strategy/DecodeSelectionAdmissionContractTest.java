package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.QueueOrderingConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.route.CapacityLimit;
import org.flexlb.dao.route.CapacityUsage;
import org.flexlb.dao.route.DecodeRequirement;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.WorkerDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Selection filters and ranks nodes; only locked reservation acquires placement capacity.
 */
class DecodeSelectionAdmissionContractTest {
    private static final String QUEUED_IP = "127.0.0.1";
    private static final String FREE_IP = "127.0.0.2";
    private static final long PROMPT_TOKENS = 100L;
    private static final long EXPECTED_TOKENS = 300L;

    @BeforeEach
    void clearEndpointRegistry() {
        for (RoleType role : RoleType.values()) {
            var endpoints = EndpointRegistry.getEndpointsByRole(role);
            if (endpoints != null) {
                endpoints.clear();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("preemptionCases")
    void cheaperFullWorkerIsSkippedEvenWhenPreemptionIsEnabled(Policy policy, CapacityDimension dimension) {
        try (Fixture fixture = new Fixture(policy, dimension)) {
            fixture.assertPlacementAndDispatchDisagree();
            var queuedUsage = fixture.queued.routingView().placementUsage();
            var freeUsage = fixture.free.routingView().placementUsage();

            for (long requestId = 100L; requestId < 106L; requestId++) {
                BalanceContext context = fixture.context(requestId);
                SelectedDecode result = fixture.strategy.select(context, null);
                assertNotNull(result);
                assertFalse(result.allCandidatesFull());
                assertEquals(FREE_IP, result.endpoint().getIp());
                fixture.assertSelectionHasNoReservation(requestId, queuedUsage, freeUsage);
                DecodeRequirement request = context.getDecodeRequirement();
                assertNull(fixture.queued.reserveForDispatch(request.requestId(), request.inputKvTokens(),
                        request.inputAndMaxOutputKvTokens(), request.priority(), request.capacity()));
                fixture.assertSelectionHasNoReservation(requestId, queuedUsage, freeUsage);
            }
        }
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("dispatchAdmissionCases")
    void selectionDoesNotReserveOnTheAvailableWorker(Policy policy, CapacityDimension dimension) {
        try (Fixture fixture = new Fixture(policy, dimension)) {
            fixture.assertPlacementAndDispatchDisagree();
            var queuedUsage = fixture.queued.routingView().placementUsage();
            var freeUsage = fixture.free.routingView().placementUsage();
            Map<String, Integer> selectedCounts = new HashMap<>();

            for (long requestId = 200L; requestId < 206L; requestId++) {
                SelectedDecode result = fixture.strategy.select(fixture.context(requestId), null);
                assertNotNull(result);
                assertFalse(result.allCandidatesFull());
                selectedCounts.merge(result.endpoint().getIp(), 1, Integer::sum);
                fixture.assertSelectionHasNoReservation(requestId, queuedUsage, freeUsage);
            }

            assertEquals(Map.of(FREE_IP, 6), selectedCounts);
        }
    }

    private static Stream<Arguments> dispatchAdmissionCases() {
        return Stream.of(Policy.FIFO, Policy.PRIORITY_ONLY)
                .flatMap(policy -> Stream.of(CapacityDimension.values())
                        .map(dimension -> Arguments.of(policy, dimension)));
    }

    private enum CapacityDimension { EXPECTED_KV, REQUESTS }
    private static Stream<Arguments> preemptionCases() {
        return Stream.of(Policy.PRIORITY_DEFAULT, Policy.PREEMPTIVE)
                .flatMap(policy -> Stream.of(CapacityDimension.values()).map(dimension -> Arguments.of(policy, dimension)));
    }

    private enum Policy { FIFO, PRIORITY_ONLY, PRIORITY_DEFAULT, PREEMPTIVE }

    private static final class Fixture implements AutoCloseable {
        private final FlexlbConfig config = StrategyTestSupport.config();
        private final EndpointRegistry endpoints;
        private final DecodeSelector strategy;
        private final DecodeEndpoint queued;
        private final DecodeEndpoint free;
        private final CapacityLimit limits;

        private Fixture(Policy policy, CapacityDimension dimension) {
            config.getRouter().getRoles().getDecode().getCostEstimator().setExpression("0 - running_size");
            if (policy != Policy.FIFO) {
                QueueOrderingConfig ordering = QueueOrderingConfig.priority();
                if (policy == Policy.PRIORITY_ONLY) { ordering.setPreemption(null); }
                if (policy == Policy.PREEMPTIVE) {
                    PreemptionConfig preemption = new PreemptionConfig();
                    preemption.setAllowedVictimStages(EnumSet.of(VictimStage.DECODE_RESERVED));
                    ordering.setPreemption(preemption);
                }
                config.queueScheduler().setOrdering(ordering);
            }
            long maxRequests = dimension == CapacityDimension.REQUESTS ? 2L : 0L;
            var availability = config.getRouter().getRoles().getDecode().getAvailability();
            availability.setMaxEngineRequests(maxRequests == 0L ? null : maxRequests);
            availability.setMaxKvUsagePercent(90L);
            limits = new CapacityLimit(maxRequests, 90L);
            ConfigService configs = mock(ConfigService.class);
            when(configs.loadBalanceConfig()).thenReturn(config);
            endpoints = StrategyTestSupport.endpointRegistry(configs);
            queued = publish(QUEUED_IP);
            free = publish(FREE_IP);
            strategy = new DecodeSelector(new WorkerDirectory(endpoints));

            long kvBudget = dimension == CapacityDimension.EXPECTED_KV ? 400L : 0L;
            assertNotNull(queued.reserve("1", 0L, kvBudget, 50));
            assertNotNull(queued.reserve("2", 0L, kvBudget, 50));
        }

        private DecodeEndpoint publish(String ip) {
            var worker = StrategyTestSupport.workerStatus(RoleType.DECODE, null, ip, 8080, 9090, true, 1_000L, 1_000L);
            StrategyTestSupport.publish(worker,
                    StrategyTestSupport
                            .response(RoleType.DECODE, true, 1_000L, 1_000L,
                                    worker.appliedStatusCursor().statusVersion() + 1L)
                            .setRunningQueryLen(1));
            return (DecodeEndpoint) StrategyTestSupport.publishEndpoint(endpoints, RoleType.DECODE, ip + ":8080",
                    worker);
        }

        private BalanceContext context(long requestId) {
            Request request = new Request();
            request.setRequestId(Long.toString(requestId));
            request.setSeqLen(100L);
            request.setMaxNewTokens(200);
            request.setPriority(70);
            BalanceContext context = new BalanceContext(config);
            context.setRequest(request);
            return context;
        }

        private void assertPlacementAndDispatchDisagree() {
            assertTrue(limits.evaluate(queued.routingView().dispatchUsage(), PROMPT_TOKENS, EXPECTED_TOKENS)
                    .hasCapacity());
            assertFalse(limits.evaluate(queued.routingView().placementUsage(), PROMPT_TOKENS, EXPECTED_TOKENS)
                    .hasCapacity());
            assertTrue(
                    limits.evaluate(free.routingView().dispatchUsage(), PROMPT_TOKENS, EXPECTED_TOKENS).hasCapacity());
            assertTrue(
                    limits.evaluate(free.routingView().placementUsage(), PROMPT_TOKENS, EXPECTED_TOKENS).hasCapacity());
        }

        private void assertSelectionHasNoReservation(long requestId,
                                                     CapacityUsage queuedUsage,
                                                     CapacityUsage freeUsage) {
            assertEquals(queuedUsage, queued.routingView().placementUsage());
            assertEquals(freeUsage, free.routingView().placementUsage());
            assertFalse(queued.resourceSnapshot().isQueued(Long.toString(requestId)));
            assertFalse(free.resourceSnapshot().isQueued(Long.toString(requestId)));
        }

        @Override
        public void close() {
            endpoints.close();
        }
    }
}

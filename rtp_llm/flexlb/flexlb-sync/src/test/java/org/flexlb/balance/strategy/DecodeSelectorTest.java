package org.flexlb.balance.strategy;

import lombok.extern.slf4j.Slf4j;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.QueueOrderingConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.CapacityLimit;
import org.flexlb.dao.route.CapacityRelease;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.sync.status.WorkerDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class DecodeSelectorTest {

    private ConfigService configService;
    private Map<String, WorkerStatus> decodeStatuses;

    @BeforeEach
    void setUp() {
        for (RoleType role : RoleType.values()) {
            var endpoints = EndpointRegistry.getEndpointsByRole(role);
            if (endpoints != null) {
                endpoints.clear();
            }
        }
        configService = Mockito.mock(ConfigService.class);
        Mockito.when(configService.loadBalanceConfig()).thenReturn(StrategyTestSupport.config());
        decodeStatuses = new HashMap<>();
    }

    WorkerStatus createWorkerStatus(String ip) {
        return createWorkerStatus(ip, null);
    }

    WorkerStatus createWorkerStatus(String ip, String group) {
        return StrategyTestSupport.workerStatus(
                RoleType.DECODE, group, ip, 8080, 9090,
                true, 0L, 0L);
    }

    /** Create an EndpointRegistry with DecodeEndpoints registered for each WorkerStatus entry. */
    private EndpointRegistry createDecodeRegistry(Map<String, WorkerStatus> workerMap) {
        EndpointRegistry registry = StrategyTestSupport.endpointRegistry(configService);
        for (Map.Entry<String, WorkerStatus> entry : workerMap.entrySet()) {
            WorkerStatus ws = entry.getValue();
            StrategyTestSupport.publishEndpoint(registry,
                    RoleType.DECODE, entry.getKey(), ws);
        }
        return registry;
    }

    private WorkerStatus registerWorker(String ip, long totalKv, long availableKv) {
        WorkerStatus worker = createWorkerStatus(ip);
        setKv(worker, totalKv, availableKv);
        decodeStatuses.put(ip + ":8080", worker);
        return worker;
    }

    private WorkerStatus dpWorker(String ip, String group, String masterAddress, long rank) {
        WorkerStatus worker = createWorkerStatus(ip, group);
        WorkerStatusPB.Builder response = StrategyTestSupport.response(RoleType.DECODE, true, 10_000L, 10_000L, 2L);
        response.setDpSize(2);
        response.setDpRank(rank);
        if (masterAddress != null) {
            response.setDpMasterAddress(masterAddress);
        }
        StrategyTestSupport.publish(worker, response);
        return worker;
    }

    private EndpointRegistry decodeRegistry() {
        return createDecodeRegistry(decodeStatuses);
    }

    private DecodeSelector availableStrategy(EndpointRegistry registry) {
        return new DecodeSelector(new WorkerDirectory(registry));
    }

    @Test
    void selectedDecodePreservesLogicalEngineIdentity() {
        WorkerStatus worker = WorkerStatus.createDiscovered(RoleType.DECODE, null, "127.0.0.1", 8080, 9090, "test-site",
                "deployment", 1, 2, false);
        StrategyTestSupport.publish(worker, StrategyTestSupport.response(
                RoleType.DECODE, true, 10_000L, 10_000L, 1L));
        decodeStatuses.put(worker.getLogicalIpPort(), worker);
        EndpointRegistry registry = decodeRegistry();
        try {
            DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context(100L, 1L), null);

            assertNotNull(selected);
            assertEquals(1, selected.getStatus().topologySnapshot().engineIndex());
            assertEquals("127.0.0.1:8080@1", selected.ipPort());
        } finally {
            registry.close();
        }
    }

    @Test
    void recordsMinimumCostSelectionReason() {
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        try {
            BalanceContext context = context(100L, 2L);
            DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context, null);

            assertNotNull(selected);
            assertEquals("DECODE_MIN_COST", context.selectionReason(RoleType.DECODE));
        } finally {
            registry.close();
        }
    }

    @Test
    void recordsMinimumCostSelectionReasonForTies() {
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        registerWorker("127.0.0.2", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        try {
            BalanceContext context = context(100L, 3L);
            DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context, null);

            assertNotNull(selected);
            assertEquals("DECODE_MIN_COST", context.selectionReason(RoleType.DECODE));
        } finally {
            registry.close();
        }
    }

    private BalanceContext context(long sequenceLength, long requestId) {
        Request request = new Request();
        request.setSeqLen(sequenceLength);
        request.setRequestId(Long.toString(requestId));
        BalanceContext context = new BalanceContext(configService.loadBalanceConfig());
        context.setRequest(request);
        return context;
    }

    @Test
    void admissionSummaryIsReusedAndTracksEngineOwnershipSeparatelyFromQueuedKv() {
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        try {
            DecodeEndpoint endpoint = decodeEndpoint(registry, "127.0.0.1:8080");
            DecodeEndpoint.AdmissionSummary empty = endpoint.admissionSummary();
            assertSame(empty, endpoint.admissionSummary());
            var reservation = endpoint.reserve("42", 128L, 256L, 70);
            var queued = endpoint.admissionSummary();
            assertNotSame(empty, queued);
            assertSame(queued, endpoint.admissionSummary());
            assertEquals(new CapacityRelease(1L, 128L, 256L), queued.placementOccupancy(70));
            assertEquals(CapacityRelease.NONE, queued.engineOccupancy(70));

            var acquired = endpoint.acquireDispatchPermit(reservation, new CapacityLimit(10L, 100L));
            assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.SUCCESS, acquired.status());
            var dispatching = endpoint.admissionSummary();
            assertNotSame(queued, dispatching);
            assertEquals(queued.placementOccupancy(70), dispatching.engineOccupancy(70));
            assertEquals(CapacityRelease.NONE, queued.engineOccupancy(70),
                    "a published summary must remain immutable after ownership changes");
            assertTrue(acquired.permit().release());
            endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            assertEquals(CapacityRelease.NONE, endpoint.admissionSummary().placementOccupancy(70));
        } finally {
            registry.close();
        }
    }

    @Test
    void should_handle_empty_worker_map_when_no_workers_available() {
        EndpointRegistry emptyRegistry = StrategyTestSupport.endpointRegistry(configService);
        WorkerDirectory engineWorkerStatus = new WorkerDirectory(emptyRegistry);
        DecodeSelector decodeSelector = new DecodeSelector(engineWorkerStatus);

        BalanceContext balanceContext = context(1_000, 1_000L);

        DecodeEndpoint status = selectEndpoint(decodeSelector, balanceContext, null);

        assertNull(status);
    }

    @Test
    void rescansForAValidHigherCostWhenTheLowestCostDpGroupIsIncomplete() {
        var incomplete = dpWorker("127.0.0.1", "routing-a", "10.0.0.1", 0);
        decodeStatuses.put(incomplete.getLogicalIpPort(), incomplete);
        WorkerStatus independent = createWorkerStatus("127.0.0.2", "routing-a");
        setKv(independent, 10_000L, 8_000L);
        decodeStatuses.put(independent.getLogicalIpPort(), independent);
        EndpointRegistry registry = decodeRegistry();
        try {
            reserveQueued(decodeEndpoint(registry, incomplete.getLogicalIpPort()), 91L, 0L, 0L, 50);
            var entries = Mockito.spy(new ArrayList<>(registry.decodeRoutingEntries()));
            WorkerDirectory directory = Mockito.mock(WorkerDirectory.class);
            Mockito.when(directory.decodeRoutingEntries()).thenReturn(entries);
            var selected = selectEndpoint(new DecodeSelector(directory), context(100L, 1_001L), "routing-a");
            assertSame(decodeEndpoint(registry, independent.getLogicalIpPort()), selected);
            Mockito.verify(entries, Mockito.times(2)).iterator();
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvcache_used_ratio", "running_size",
            "running_size / max_running_size + kvcache_used_ratio"})
    void equalCostsSelectOnlyTiedWorkers(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 10_000, 9_000);
        registerWorker("127.0.0.2", 10_000, 9_000);
        registerWorker("127.0.0.3", 10_000, 9_000);
        DecodeSelector strategy = availableStrategy(decodeRegistry());
        for (int i = 0; i < 12; i++) {
            DecodeEndpoint selected = selectEndpoint(strategy, context(1_000L, 1_000L + i), null);
            assertTrue(List.of("127.0.0.1", "127.0.0.2", "127.0.0.3").contains(selected.getIp()));
        }
    }

    @Test
    void should_handle_group_selection_when_group_parameter_provided() {
        WorkerStatus worker1 = createWorkerStatus("127.0.0.1", "group-a");

        decodeStatuses.put("127.0.0.1:8080", worker1);

        DecodeSelector decodeSelector = availableStrategy(decodeRegistry());
        BalanceContext balanceContext = context(1_000, 1_000L);

        DecodeEndpoint status = selectEndpoint(decodeSelector, balanceContext, "group-a");

        assertNotNull(status);
        assertEquals("127.0.0.1", status.getIp());
    }

    @Test
    void missingDpRankBlocksItsGroupButIndependentDecodeCanBeSelected() {
        WorkerStatus rankZero = dpWorker("127.0.0.1", "routing-a", "10.0.0.1", 0);
        WorkerStatus rankOne = dpWorker("127.0.0.2", "routing-a", "10.0.0.1", 1);
        WorkerStatus independent = dpWorker("127.0.0.3", "routing-b", null, 0);
        EndpointRegistry registry = StrategyTestSupport.endpointRegistry(configService);
        try {
            StrategyTestSupport.publishEndpoint(registry, RoleType.DECODE, rankZero.getLogicalIpPort(), rankZero);
            StrategyTestSupport.publishEndpoint(registry, RoleType.DECODE, independent.getLogicalIpPort(), independent);
            DecodeSelector selector = availableStrategy(registry);

            assertNull(selectEndpoint(selector, context(100, 1L), "routing-a"));
            assertEquals("127.0.0.3", selectEndpoint(selector, context(100, 2L), null).getIp());

            StrategyTestSupport.publishEndpoint(registry, RoleType.DECODE, rankOne.getLogicalIpPort(), rankOne);

            DecodeEndpoint selected = selectEndpoint(selector, context(100, 3L), "routing-a");
            assertNotNull(selected);
            assertTrue(List.of("127.0.0.1", "127.0.0.2").contains(selected.getIp()));
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @CsvSource(value = {"kvcache_used_ratio | 127.0.0.1", "kvcache_used / kvcache_capacity | 127.0.0.1",
            "running_size | 127.0.0.2", "running_size / max_running_size + 2 * kvcache_used_ratio | 127.0.0.1",
            "2 * running_size / max_running_size + kvcache_used_ratio | 127.0.0.2",
            "pow(running_size / max_running_size, 2) + kvcache_used_ratio | 127.0.0.1"}, delimiter = '|')
    void costFormulaSupportsPureWeightedAndNonlinearLoadCosts(String expression, String expectedIp) {
        configureCost(expression);
        registerWorker("127.0.0.1", 10_000, 9_500);
        registerWorker("127.0.0.2", 10_000, 8_500);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.1:8080"), 91L, 0L, 0L, 50);
        reserveQueued(decodeEndpoint(registry, "127.0.0.1:8080"), 92L, 0L, 0L, 50);
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 93L, 0L, 0L, 50);
        // The fixed request cap is 10: running ratios are .2/.1, KV ratios .05/.15.
        DecodeSelector strategy = availableStrategy(registry);
        for (int i = 0; i < 10; i++) {
            DecodeEndpoint selected = selectEndpoint(strategy, context(1_000L, 1_000L + i), null);
            assertEquals(expectedIp, selected.getIp());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvcache_used_ratio", "running_size",
            "running_size / max_running_size + kvcache_used_ratio"})
    void retainsAllMinimumCostWorkersBeyondTheInitialListCapacity(String expression) {
        configureCost(expression);
        for (int i = 1; i <= 20; i++) {
            registerWorker("127.0.0." + i, 1_000_000, 900_000);
        }
        registerWorker("127.0.0.21", 1_000_000, 800_000);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.21:8080"), 91L, 0L, 0L, 50);
        DecodeSelector strategy = availableStrategy(registry);
        assertFalse("127.0.0.21".equals(selectEndpoint(strategy, context(1L, 10_000L), null).getIp()));
        assertEquals(20,
                org.springframework.test.util.ReflectionTestUtils.getField(strategy, "lastLowestCostWorkerPeakSize"));
    }

    @Test
    void defaultCostUsesKvUsageRatioAcrossDifferentWorkerCapacities() {
        var estimator = configService.loadBalanceConfig().getRouter().getRoles().getDecode().getCostEstimator();
        assertEquals("kvcache_used_ratio", estimator.getExpression());
        registerWorker("127.0.0.1", 1_000L, 500L);
        registerWorker("127.0.0.2", 10_000L, 9_000L);

        DecodeEndpoint selected = selectEndpoint(availableStrategy(decodeRegistry()), context(100L, 1_001L), null);

        assertEquals("127.0.0.2", selected.getIp(),
                "10% KV usage is cheaper than 50%, despite using more absolute tokens");
    }

    @Test
    void cacheCostIncludesExpectedLocalReservationsBeyondTheHardPromptReservation() {
        registerWorker("127.0.0.1", 10_000L, 9_500L);
        registerWorker("127.0.0.2", 10_000L, 8_500L);
        EndpointRegistry registry = decodeRegistry();
        DecodeEndpoint endpoint = decodeEndpoint(registry, "127.0.0.1:8080");
        reserveQueued(endpoint, 91L, 100L, 2_000L, 50);
        assertEquals(100L, endpoint.routingView().inputKvReserved());
        assertEquals(2_500L, endpoint.routingView().realKvUsed());

        DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context(100L, 1_001L), null);

        assertEquals("127.0.0.2", selected.getIp());
    }

    @Test
    void workersOverKvBudgetAreOnlyPreemptionCandidates() {
        registerWorker("127.0.0.1", 1_000L, 100L);
        registerWorker("127.0.0.2", 1_000L, 100L);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.1:8080"), 91L, 0L, 1_200L, 50);
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 92L, 0L, 200L, 50);
        DecodeSelector strategy = availableStrategy(registry);
        for (int i = 0; i < 4; i++) {
            var selected = strategy.select(context(100L, 1_001L + i), null);
            assertTrue(selected.allCandidatesFull());
            assertTrue(List.of("127.0.0.1", "127.0.0.2").contains(selected.endpoint().getIp()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"1 / running_size", "sqrt(running_size - 1)"})
    void nonFiniteCostIsSkippedWhenAnotherWorkerHasAFiniteCost(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        registerWorker("127.0.0.2", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 91L, 0L, 0L, 50);

        DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context(100L, 1_001L), null);

        assertEquals("127.0.0.2", selected.getIp());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1 / running_size", "sqrt(running_size - 1)"})
    void allNonFiniteCostsFailExplicitlyInsteadOfWaitingForCapacity(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        registerWorker("127.0.0.2", 10_000L, 10_000L);
        DecodeSelector strategy = availableStrategy(decodeRegistry());

        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> selectEndpoint(strategy, context(100L, 1_001L), null));

        assertTrue(error.getMessage().contains("Decode cost formula produced no finite score"));
    }

    @Test
    void unnormalizedRunningSizeFormulaDoesNotRequireAnEngineRequestCap() {
        var decode = configService.loadBalanceConfig().getRouter().getRoles().getDecode();
        decode.getCostEstimator().setExpression("running_size");
        assertNull(decode.getAvailability().getMaxEngineRequests());
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        registerWorker("127.0.0.2", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.1:8080"), 91L, 0L, 0L, 50);

        DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context(100L, 1_001L), null);

        assertEquals("127.0.0.2", selected.getIp());
    }

    @Test
    void filtersCapacityAndScoresInOneScan() {
        registerWorker("127.0.0.1", 1_000L, 50L);
        registerWorker("127.0.0.2", 1_000L, 150L);
        EndpointRegistry registry = decodeRegistry();
        try {
            var entries = Mockito.spy(new ArrayList<>(registry.decodeRoutingEntries()));
            WorkerDirectory directory = Mockito.mock(WorkerDirectory.class);
            Mockito.when(directory.decodeRoutingEntries()).thenReturn(entries);
            var context = context(40L, 1_001L);
            var selected = new DecodeSelector(directory).select(context, null);

            assertFalse(selected.selectedAsEmpty());
            assertFalse(selected.allCandidatesFull());
            assertEquals("127.0.0.2", selected.endpoint().getIp());
            Mockito.verify(entries).iterator();
            assertNull(selected.endpoint().reservationHandle(context.getRequestId()));
        } finally {
            registry.close();
        }
    }

    @Test
    void runningCostIncludesConfirmedEngineRequestsAndAllLocalReservations() {
        configureCost("running_size / max_running_size");
        WorkerStatus confirmed = registerWorker("127.0.0.1", 10_000L, 9_000L);
        var response = StrategyTestSupport.response(RoleType.DECODE, true, 9_000L, 10_000L,
                confirmed.appliedStatusCursor().statusVersion() + 1L);
        WorkerStatusPBs.setRunningTasks(response,
                Map.of("81", task(81L, TaskPhase.KV_ALLOCATED), "82", task(82L, TaskPhase.RUNNING)));
        StrategyTestSupport.publish(confirmed, response);
        registerWorker("127.0.0.2", 10_000L, 9_000L);
        EndpointRegistry registry = decodeRegistry();
        DecodeEndpoint local = decodeEndpoint(registry, "127.0.0.2:8080");
        reserveHeld(local, 91L, 0L, 0L, 50);
        assertEquals(2, decodeEndpoint(registry, "127.0.0.1:8080").routingView().totalLoad());
        DecodeSelector strategy = availableStrategy(registry);
        for (int i = 0; i < 4; i++) {
            assertEquals("127.0.0.2", selectEndpoint(strategy, context(100L, 1_001L + i), null).getIp());
        }

        reserveQueued(local, 92L, 0L, 0L, 50);
        reserveQueued(local, 93L, 0L, 0L, 50);
        assertEquals(1, local.routingView().engineLoad());
        assertEquals(3, local.routingView().totalLoad());
        for (int i = 0; i < 4; i++) {
            assertEquals("127.0.0.1", selectEndpoint(strategy, context(100L, 2_001L + i), null).getIp(),
                    "queued reservations count toward running cost before they face the Engine");
        }
    }

    @Test
    void availabilityCountsAllReservationsForQueue() {
        configureCost("running_size / max_running_size");
        configService.loadBalanceConfig().getRouter().getRoles().getDecode()
                .getAvailability().setMaxEngineRequests(1L);
        registerWorker("127.0.0.1", 10_000L, 10_000L);
        registerWorker("127.0.0.2", 10_000L, 10_000L);
        EndpointRegistry registry = decodeRegistry();
        reserveHeld(decodeEndpoint(registry, "127.0.0.1:8080"), 91L, 0L, 0L, 50);
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 92L, 0L, 0L, 50);
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 93L, 0L, 0L, 50);

        var selected = availableStrategy(registry).select(context(100L, 1_001L), null);

        assertTrue(selected.allCandidatesFull());
        assertTrue(List.of("127.0.0.1", "127.0.0.2").contains(selected.endpoint().getIp()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvcache_used_ratio", "running_size"})
    void filtersWorkersWithoutEnoughKvBeforeScoring(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 1_000, 100);
        registerWorker("127.0.0.2", 1_000, 800);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 91L, 0L, 0L, 50);
        DecodeSelector decodeSelector = availableStrategy(registry);
        BalanceContext balanceContext = context(500, 2_000L);

        var selected = decodeSelector.select(balanceContext, null);

        assertFalse(selected.allCandidatesFull());
        assertEquals("127.0.0.2", selected.endpoint().getIp());
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvcache_used_ratio", "running_size"})
    void lockedReservationRejectsARequestThatExceedsTheSelectedWorkersBudget(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 128L, 128L);
        registerWorker("127.0.0.2", 256L, 256L);
        DecodeSelector strategy = availableStrategy(decodeRegistry());
        BalanceContext context = context(257L, 3_050L);
        context.getRequest().setMaxNewTokens(1);

        var selected = strategy.select(context, null);
        assertFalse(selected.selectedAsEmpty());
        assertTrue(selected.allCandidatesFull());
        var request = context.getDecodeRequirement();
        assertNull(selected.endpoint()
                .reserveForDispatch(request.requestId(), request.inputKvTokens(), request.inputAndMaxOutputKvTokens(),
                        request.priority(), request.capacity()));
    }

    @Test
    void unknownPhysicalCapacityDoesNotBecomeATerminalRejection() {
        registerWorker("127.0.0.1", 0L, 0L);
        DecodeSelector strategy = availableStrategy(decodeRegistry());

        assertNotNull(selectEndpoint(strategy, context(1_000_000L, 3_051L), null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvcache_used_ratio", "running_size",
            "running_size / max_running_size + kvcache_used_ratio"})
    void unknownKvCapacityDoesNotProduceAnUnselectableCost(String expression) {
        configureCost(expression);
        registerWorker("127.0.0.1", 0L, 0L);
        registerWorker("127.0.0.2", 10_000L, 9_000L);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 91L, 0L, 0L, 50);

        DecodeEndpoint selected = selectEndpoint(availableStrategy(registry), context(100L, 3_052L), null);

        assertEquals("127.0.0.1", selected.getIp());
    }

    @Test
    void queuePlanningSelectsExactEndpointWithoutTakingCapacity() {
        registerWorker("127.0.0.1", 1_000, 1_000);
        EndpointRegistry registry = decodeRegistry();
        DecodeEndpoint endpoint = decodeEndpoint(
                registry, "127.0.0.1:8080");
        reserveQueued(endpoint, 1L, 400, 700, 50);
        reserveQueued(endpoint, 2L, 400, 700, 50);

        assertTrue(endpoint.routingView().realKvUsed() > new CapacityLimit(0L, 90L)
                .kvBudget(endpoint.routingView().totalKv()));
        assertEquals(0L, endpoint.routingView().engineFacingKvUsed());

        DecodeSelector strategy = new DecodeSelector(new WorkerDirectory(registry));

        BalanceContext context = context(100, 3L);
        assertSame(endpoint, selectEndpoint(strategy, context, null));
        assertFalse(endpoint.resourceSnapshot().isQueued("3"),
                "selection must not mutate Decode reservation ownership");

        QueueOrderingConfig preemptiveOrdering =
                QueueOrderingConfig.priority();
        preemptiveOrdering.setPreemption(preemption());
        configService.loadBalanceConfig().queueScheduler()
                .setOrdering(preemptiveOrdering);
        assertSame(endpoint, selectEndpoint(strategy, context(100L, 4L), null));
        assertFalse(endpoint.resourceSnapshot().isQueued("4"),
                "priority planning must leave capacity acquisition to commit");
    }

    @Test
    void runningCostCanSelectACheaperBusyWorker() {
        configureCost("running_size / max_running_size");
        registerWorker("127.0.0.1", 1_000, 500);
        registerWorker("127.0.0.2", 1_000, 1_000);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(
                decodeEndpoint(registry, "127.0.0.2:8080"),
                91L, 0L, 0L, 50);
        DecodeSelector strategy = availableStrategy(registry);
        BalanceContext context = context(300L, 5L);

        DecodeEndpoint selected = selectEndpoint(strategy, context, null);

        assertNotNull(selected);
        assertEquals("127.0.0.1", selected.getIp());
    }

    @ParameterizedTest
    @EnumSource(QueuePolicy.class)
    void queueSelectsFeasibleEndpointsBeforeWaitingForTransientCapacity(QueuePolicy policy) {
        configureQueue(policy);
        configureCost("running_size / max_running_size");
        registerWorker("127.0.0.1", 1000, 900);
        registerWorker("127.0.0.2", 1000, 850);
        EndpointRegistry registry = decodeRegistry();
        DecodeSelector strategy = availableStrategy(registry);
        for (int index = 0; index < 10; index++) {
            long requestId = 123L + index;
            var selected = strategy.select(context(100L, requestId), null);
            assertNotNull(selected);
            assertFalse(selected.allCandidatesFull());
            assertTrue(List.of("127.0.0.1", "127.0.0.2").contains(selected.endpoint().getIp()));
            assertFalse(selected.endpoint().resourceSnapshot().isQueued(Long.toString(requestId)),
                    "selection cannot claim capacity");
        }
    }

    @ParameterizedTest
    @EnumSource(QueuePolicy.class)
    void oversizedRequestMarksAllCandidatesFull(QueuePolicy policy) {
        configureQueue(policy);
        configureCost("running_size / max_running_size");
        registerWorker("127.0.0.1", 100L, 100L);
        registerWorker("127.0.0.2", 1000L, 0L);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(decodeEndpoint(registry, "127.0.0.2:8080"), 91L, 0L, 0L, 50);
        DecodeSelector strategy = availableStrategy(registry);
        for (long requestId = 200L; requestId < 204L; requestId++) {
            BalanceContext context = context(200L, requestId);
            var selected = strategy.select(context, null);
            assertNotNull(selected);
            assertTrue(selected.allCandidatesFull());
            assertEquals("DECODE_ALL_CANDIDATES_FULL", context.selectionReason(RoleType.DECODE));
        }
    }

    private enum QueuePolicy { FIFO, PRIORITY, PREEMPTIVE }

    private void configureQueue(QueuePolicy policy) {
        QueueOrderingConfig ordering = policy == QueuePolicy.FIFO
                ? new QueueOrderingConfig() : QueueOrderingConfig.priority();
        if (policy == QueuePolicy.PRIORITY) { ordering.setPreemption(null); }
        if (policy == QueuePolicy.PREEMPTIVE) { ordering.setPreemption(preemption()); }
        configService.loadBalanceConfig().queueScheduler().setOrdering(ordering);
    }

    private static PreemptionConfig preemption() {
        PreemptionConfig config = new PreemptionConfig();
        config.setAllowedVictimStages(EnumSet.of(VictimStage.DECODE_RESERVED));
        return config;
    }

    @Test
    void cacheCostKeepsZeroKvReservationsTiedAcrossWorkers() {
        registerWorker("127.0.0.1", 1_000, 1_000);
        registerWorker("127.0.0.2", 1_000, 1_000);
        EndpointRegistry registry = decodeRegistry();
        reserveQueued(
                decodeEndpoint(registry, "127.0.0.2:8080"),
                91L, 0L, 0L, 50);
        DecodeSelector strategy = availableStrategy(registry);
        BalanceContext context = context(0L, 30_000L);
        for (int index = 0; index < 20; index++) {
            context.getRequest().setRequestId(Long.toString(30_000L + index));
            DecodeEndpoint selected = selectEndpoint(strategy, context, null);
            assertTrue(List.of("127.0.0.1", "127.0.0.2").contains(selected.getIp()));
            assertEquals("DECODE_MIN_COST", context.selectionReason(RoleType.DECODE));
        }
    }

    @Test
    void highLoadRemainsAdmissibleBelowTheConfiguredCapacity() {
        WorkerStatus worker = createWorkerStatus("127.0.0.1");
        setKv(worker, 10_000, 10_000);
        decodeStatuses.put("127.0.0.1:8080", worker);

        EndpointRegistry registry = createDecodeRegistry(decodeStatuses);
        DecodeEndpoint endpoint = decodeEndpoint(registry, "127.0.0.1:8080");
        for (int i = 0; i < 6; i++) {
            reserveHeld(endpoint, 400L + i, 0, 0, 50);
        }

        DecodeSelector strategy = availableStrategy(registry);

        Request request = new Request();
        request.setSeqLen(1);
        request.setRequestId("500");
        BalanceContext context = new BalanceContext(configService.loadBalanceConfig());
        context.setRequest(request);

        DecodeEndpoint status = selectEndpoint(strategy, context, null);
        assertNotNull(status);
        assertEquals("127.0.0.1", status.getIp());
    }

    @Test
    void emptyWorkerBypassesKvBudgetAndTakesPriorityOverWorkersWithCapacity() {
        WorkerStatus empty = createWorkerStatus("127.0.0.1");
        StrategyTestSupport.publish(empty, StrategyTestSupport.response(RoleType.DECODE, true, 100L, 100L, 2L));
        decodeStatuses.put(empty.getLogicalIpPort(), empty);
        registerWorker("127.0.0.2", 20_000L, 20_000L);
        BalanceContext context = context(500L, 991L);
        context.getRequest().setMaxNewTokens(10_000);
        SelectedDecode selected = availableStrategy(decodeRegistry()).select(context, null);
        assertNotNull(selected);
        assertEquals(empty.getLogicalIpPort(), selected.endpoint().ipPort());
        assertTrue(selected.selectedAsEmpty());
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, 0L, 101L})
    void invalidKvPercentageFailsBeforeWorkerSelection(long percent) {
        configService.loadBalanceConfig().getRouter().getRoles().getDecode()
                .getAvailability().setMaxKvUsagePercent(percent);
        assertThrows(IllegalArgumentException.class, () -> context(1L, 992L).getDecodeRequirement());
    }

    private void configureCost(String expression) {
        var decode = configService.loadBalanceConfig().getRouter().getRoles().getDecode();
        decode.getCostEstimator().setExpression(expression);
        decode.getAvailability().setMaxEngineRequests(10L);
    }

    private static TaskInfoPB.Builder task(long requestId, TaskPhase phase) {
        TaskInfoPB.Builder task = TaskInfoPB.newBuilder();
        task.setRequestIdStr(Long.toString(requestId));
        WorkerStatusPBs.phase(task, phase);
        return task;
    }

    private static void setKv(WorkerStatus worker, long totalKv, long availableKv) {
        StrategyTestSupport.publish(worker,
                StrategyTestSupport
                        .response(RoleType.DECODE, true, availableKv, totalKv,
                                Math.max(1L, worker.appliedStatusCursor().statusVersion() + 1L))
                        .setRunningQueryLen(1));
    }

    private static void reserveQueued(DecodeEndpoint endpoint,
                                      long requestId,
                                      long kvTokens,
                                      long kvBudgetTokens,
                                      int priority) {
        endpoint.reserve(Long.toString(requestId), kvTokens, kvBudgetTokens, priority);
    }

    private static DecodeEndpoint decodeEndpoint(
            EndpointRegistry registry,
            String address) {
        return (DecodeEndpoint) registry.get(RoleType.DECODE, address);
    }

    /** Non-queued inflight reservation: each call raises engineLoad by one. */
    private static void reserveHeld(DecodeEndpoint endpoint,
                                    long requestId,
                                    long kvTokens,
                                    long kvBudgetTokens,
                                    int priority) {
        endpoint.reserveUnqueued(Long.toString(requestId), kvTokens, kvBudgetTokens, priority);
    }

    private static DecodeEndpoint selectEndpoint(DecodeSelector strategy, BalanceContext context, String group) {
        SelectedDecode result = strategy.select(context, group);
        return result == null ? null : result.endpoint();
    }
}

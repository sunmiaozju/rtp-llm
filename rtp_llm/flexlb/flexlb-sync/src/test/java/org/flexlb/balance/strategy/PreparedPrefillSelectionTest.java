package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.BalanceMetrics.PrefillRouteMetrics;
import org.flexlb.dao.cache.HostCacheMatch;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.WorkerDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PreparedPrefillSelectionTest {

    private final FlexlbConfig config = StrategyTestSupport.config();
    private final ConfigService configService = mock(ConfigService.class);
    private final CacheAwareService cache = mock(CacheAwareService.class);
    private final EndpointRegistry registry;
    private final PrefillQueueRegistry queues;
    private final PrefillPicker picker;
    private final PrefillEndpoint first;
    private final PrefillEndpoint second;

    PreparedPrefillSelectionTest() {
        for (RoleType role : RoleType.values()) {
            var endpoints = EndpointRegistry.getEndpointsByRole(role);
            if (endpoints != null) {
                endpoints.clear();
            }
        }
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH_V2);
        when(configService.loadBalanceConfig()).thenReturn(config);
        registry = StrategyTestSupport.endpointRegistry(configService);
        queues = new PrefillQueueRegistry();
        first = publish("10.0.0.1", "10");
        second = publish("10.0.0.2", "15");
        when(cache.findMatchingEngines(any())).thenReturn(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC));
        CostBasedPrefillStrategy strategy =
                new CostBasedPrefillStrategy(new WorkerDirectory(registry), cache, mock(EngineHealthReporter.class));
        picker = new PrefillPicker(mock(DefaultRouter.class), strategy);
    }

    @AfterEach
    void close() {
        registry.close();
    }

    @Test
    void twoPreparedRequestsChooseUsingLatestQueueWithoutRepeatingCacheIo() throws Exception {
        Request a = request("1");
        Request b = request("2");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        RequestWorkLoadEstimateResult aInputs;
        RequestWorkLoadEstimateResult bInputs;
        try {
            var aFuture = workers.submit(() -> picker.prepare(a.ctx(), RoleType.PREFILL, "g"));
            var bFuture = workers.submit(() -> picker.prepare(b.ctx(), RoleType.PREFILL, "g"));
            aInputs = aFuture.get(5, TimeUnit.SECONDS);
            bInputs = bFuture.get(5, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
        }
        assertEquals(2, aInputs.candidates().size());
        assertEquals(10L,
                aInputs.candidates()
                        .stream()
                        .filter(c -> c.endpoint() == first)
                        .findFirst()
                        .orElseThrow()
                        .predictedMs());
        doThrow(new AssertionError("cache IO must stay in preparation")).when(cache).findMatchingEngines(any());

        a.setSelectedPrefill(picker.pick(a, RoleType.PREFILL, "g", aInputs));
        assertSame(first, a.selectedPrefill().endpoint());
        a.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        queues.addWaiting(a);

        // B 的材料早已准备好，但最终决策要看到 A 新增的 10ms 排队负载：P1=20ms，P2=15ms。
        b.setSelectedPrefill(picker.pick(b, RoleType.PREFILL, "g", bInputs));
        assertSame(second, b.selectedPrefill().endpoint());
        assertEquals(15L, b.selectedPrefill().serverStatus().getPrefillTime());
        verify(cache, times(2)).findMatchingEngines(any());
    }

    @Test
    void reselectionReplacesEffectiveHitCountsAndAffinityDiagnostics() {
        RoutingConfig.CacheAffinityConfig affinity = new RoutingConfig.CacheAffinityConfig();
        affinity.setMaxExtraTtftMs(0L);
        affinity.setMinPrefixHitPercent(0.0);
        config.getRouter().getRoles().getPrefill().setCacheAffinity(affinity);
        when(cache.findMatchingEngines(any())).thenReturn(
                new CacheMatchResult(Map.of(first.ipPort(), new HostCacheMatch(2L, 2L),
                        second.ipPort(), new HostCacheMatch(8L, 8L)), CacheMatchSource.KVCM, 0L, 100L),
                new CacheMatchResult(Map.of(second.ipPort(), new HostCacheMatch(10L, 10L)),
                        CacheMatchSource.KVCM, 0L, 100L));
        Request request = request("1");
        request.ctx().getRequest().setSeqLen(1_000L);

        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        SelectedPrefill initialChoice = picker.pick(request, RoleType.PREFILL, "g", inputs);
        assertSame(first, initialChoice.endpoint());
        assertEquals(200L, initialChoice.estimatedPrefillComputeTokens());
        assertNull(request.ctx().getMetrics().getEstimatedPrefillComputeTokens());
        assertEquals(new PrefillRouteMetrics(200L, 800L, "OVER_CAP", 5L),
                request.ctx().getMetrics().getPrefillRouteMetrics());

        request.setSelectedPrefill(initialChoice);
        assertTrue(request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
        first.close();
        assertTrue(request.moveTo(Stage.PREFILL_WAITING, Stage.MAIN_WAITING));
        request.setSelectedPrefill(null);
        RequestWorkLoadEstimateResult refreshed = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        SelectedPrefill choice = picker.pick(request, RoleType.PREFILL, "g", refreshed);
        assertSame(second, choice.endpoint());
        assertEquals(900L, choice.hitCacheTokens());
        assertEquals(new PrefillRouteMetrics(900L, 900L, "FASTEST_IS_CACHE_BEST", null),
                request.ctx().getMetrics().getPrefillRouteMetrics());
        var decision = request.ctx().getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertEquals(900L, decision.candidates().getFirst().effectiveHitTokens());

        request.setSelectedPrefill(choice);
        assertTrue(request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
        assertTrue(request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING));
        var metrics = request.ctx().getMetrics();
        assertNull(metrics.getEstimatedPrefillComputeTokens());
        var publishedEstimate = request.future().thenApply(response -> metrics.getEstimatedPrefillComputeTokens());
        assertTrue(request.publish(Stage.DECODE_WAITING, List.of(choice.serverStatus())));
        assertEquals(100L, publishedEstimate.join());
    }

    @Test
    void newlyRegisteredEndpointOnlyParticipatesAfterPreparation() {
        Request request = request("1");
        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        PrefillEndpoint added = publish("10.0.0.3", "5");

        assertSame(first, picker.pick(request, RoleType.PREFILL, "g", inputs).endpoint());
        verify(cache).findMatchingEngines(any());

        RequestWorkLoadEstimateResult refreshed = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        assertEquals(3, refreshed.candidates().size());
        assertSame(added, picker.pick(request, RoleType.PREFILL, "g", refreshed).endpoint());
        verify(cache, times(2)).findMatchingEngines(any());
    }

    @Test
    void selectionReportsOnlyPreparedCandidatesAfterGroupAndRetirementFiltering() {
        PrefillEndpoint third = publish("10.0.0.3", "20");
        publish("10.0.0.4", "1", "other");
        List<PrefillEndpoint> expected = registry.prefillRoutingSnapshot(RoleType.PREFILL)
                .stream()
                .map(EndpointRegistry.PrefillRoutingEntry::endpoint)
                .filter(endpoint -> endpoint == first || endpoint == third)
                .toList();
        second.close();
        Request request = request("1");

        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");

        assertEquals(expected, inputs.candidates().stream().map(PrefillCandidateWork::endpoint).toList());
        assertSame(first, picker.pick(request, RoleType.PREFILL, "g", inputs).endpoint());
        var decision = request.ctx().getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertNull(decision.totalWorkerCount());
        assertEquals(2, decision.candidateWorkerCount());
        verify(cache).findMatchingEngines(any());
    }

    @Test
    void preparationWithoutGroupKeepsOtherGroupsAndExcludesRetiredEndpoints() {
        PrefillEndpoint other = publish("10.0.0.3", "5", "other");
        first.close();
        Request request = request("1");
        List<PrefillEndpoint> expected = registry.prefillRoutingSnapshot(RoleType.PREFILL)
                .stream()
                .map(EndpointRegistry.PrefillRoutingEntry::endpoint)
                .filter(endpoint -> endpoint == second || endpoint == other)
                .toList();

        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, null);

        assertEquals(expected, inputs.candidates().stream().map(PrefillCandidateWork::endpoint).toList());
        assertSame(other, picker.pick(request, RoleType.PREFILL, null, inputs).endpoint());
        var decision = request.ctx().getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertNull(decision.totalWorkerCount());
        assertEquals(2, decision.candidateWorkerCount());
    }

    @Test
    void preparationKeepsCandidateMissingFromKvcmResults() {
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of(second.ipPort(), new HostCacheMatch(1L, 1L)), CacheMatchSource.KVCM, 0L, 100L));
        Request request = request("1");
        request.ctx().getRequest().setSeqLen(1_000L);

        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");

        assertEquals(2, inputs.candidates().size());
        assertTrue(inputs.candidates()
                .stream()
                .anyMatch(candidate -> candidate.endpoint() == first && candidate.cacheHitTokens() == 0L));
        assertTrue(inputs.candidates()
                .stream()
                .anyMatch(candidate -> candidate.endpoint() == second && candidate.cacheHitTokens() == 100L));
    }

    @Test
    void preparationKeepsAllBusyCandidatesBeforeCacheMatching() {
        config.getDispatcher().setMaxInflightPerPrefillWorker(1);
        for (PrefillEndpoint endpoint : List.of(first, second)) {
            var status = endpoint.getStatus();
            StrategyTestSupport.publish(status, StrategyTestSupport.response(RoleType.PREFILL, true, 0L, 10_000L,
                    status.appliedStatusCursor().statusVersion() + 1L));
            Request queued = request(endpoint == first ? "10" : "20");
            V2TestSupport.choose(queued, endpoint, 0L, 100L);
            assertTrue(queued.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
            queues.addWaiting(queued);
        }
        Request request = request("1");

        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");

        assertEquals(2, inputs.candidates().size());
        assertTrue(inputs.candidates().stream().anyMatch(candidate -> candidate.endpoint() == first));
        assertTrue(inputs.candidates().stream().anyMatch(candidate -> candidate.endpoint() == second));
        verify(cache).findMatchingEngines(any());
    }

    @Test
    void selectionUsesPreparedEndpointWithoutAcquiringHold() {
        Request request = request("1");
        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        first.close();

        assertSame(first, picker.pick(request, RoleType.PREFILL, "g", inputs).endpoint());
        verify(cache).findMatchingEngines(any());
    }

    @Test
    void preparedSelectionKeepsLocalAffinityButQueuesDeduplicatedGlobalWork() {
        first.close();
        second.close();
        RoutingConfig.CacheAffinityConfig affinity = new RoutingConfig.CacheAffinityConfig();
        affinity.setMaxExtraTtftMs(5L);
        affinity.setRemoteDiscount(0.2);
        config.getRouter().getRoles().getPrefill().setCacheAffinity(affinity);
        PrefillEndpoint remote = publish("10.0.0.3", "sum(computeTokens)");
        PrefillEndpoint local = publish("10.0.0.4", "sum(computeTokens) + 5");
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of(remote.ipPort(), new HostCacheMatch(2L, 8L), local.ipPort(), new HostCacheMatch(6L, 8L)),
                CacheMatchSource.KVCM, 0L, 100L));
        Request request = request("1");
        request.ctx().getRequest().setSeqLen(1_000L);
        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");

        request.setSelectedPrefill(picker.pick(request, RoleType.PREFILL, "g", inputs));
        assertEquals(800L, request.ctx().getGlobalHitTokens());
        assertSame(local, request.selectedPrefill().endpoint());
        assertEquals(640L, request.selectedPrefill().hitCacheTokens());
        assertEquals(360L, request.selectedPrefill().kvTokens(request.seqLen()));
        assertEquals(205L, request.selectedPrefill().predictedMs());
        assertEquals(200L, request.selectedPrefill().estimatedPrefillComputeTokens());
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        queues.addWaiting(request);
        Request next = request("2");
        next.ctx().getRequest().setSeqLen(1_000L);
        RequestWorkLoadEstimateResult nextInputs = picker.prepare(next.ctx(), RoleType.PREFILL, "g");
        SelectedPrefill choice = picker.pick(next, RoleType.PREFILL, "g", nextInputs);
        assertSame(remote, choice.endpoint());
        assertEquals(200L, choice.serverStatus().getPrefillTime());
    }

    private Request request(String id) {
        Request request = V2TestSupport.request(config, id, 50, 16L);
        request.ctx().getRequest().setBlockSize(100L);
        request.setQueueSeq(Long.parseLong(id));
        return request;
    }

    @Test
    void reorderedMaterialsStillUseEachEndpointsOwnPrediction() {
        Request request = request("1");
        RequestWorkLoadEstimateResult inputs = picker.prepare(request.ctx(), RoleType.PREFILL, "g");
        var reordered = RequestWorkLoadEstimateResult.success(inputs.cacheMatch(), inputs.candidates().reversed());

        SelectedPrefill choice = picker.pick(request, RoleType.PREFILL, "g", reordered);
        assertSame(first, choice.endpoint());
        assertEquals(10L, choice.serverStatus().getPrefillTime());
    }

    private PrefillEndpoint publish(String ip, String expression) {
        return publish(ip, expression, "g");
    }

    private PrefillEndpoint publish(String ip, String expression, String group) {
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator().setExpression(expression);
        PrefillEndpoint endpoint = (PrefillEndpoint) StrategyTestSupport.publishEndpoint(registry, RoleType.PREFILL,
                ip + ":8080",
                StrategyTestSupport.workerStatus(RoleType.PREFILL, group, ip, 8080, 8081, true, 10_000L, 10_000L));
        V2TestSupport.register(queues, endpoint);
        return endpoint;
    }
}

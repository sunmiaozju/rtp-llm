package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.cache.domain.CacheMatchQuery;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.cache.HostCacheMatch;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.pv.PvLogData;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.WorkerDirectory;
import org.flexlb.util.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CostBasedPrefillSelectionMetricTest {
    private static final int FULL_FLEET_SIZE = 750;

    private FlexlbConfig config;
    private EndpointRegistry registry;
    private CacheAwareService cache;
    private EngineHealthReporter reporter;
    private CostBasedPrefillStrategy strategy;
    private BalanceContext context;

    @BeforeEach
    void setUp() {
        clearRegisteredEndpoints();
        config = StrategyTestSupport.config();
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator()
                .setExpression("sum(computeTokens)");
        ConfigService configService = mock(ConfigService.class);
        when(configService.loadBalanceConfig()).thenReturn(config);
        registry = StrategyTestSupport.endpointRegistry(configService);
        publish("10.0.0.1", 8080);

        cache = mock(CacheAwareService.class);
        when(cache.findMatchingEngines(any()))
                .thenReturn(CacheMatchResult.empty(CacheMatchSource.KVCM));
        reporter = mock(EngineHealthReporter.class);
        strategy = new CostBasedPrefillStrategy(
                new WorkerDirectory(registry), cache, reporter);

        Request request = new Request();
        request.setRequestId("10001");
        request.setSeqLen(1_000L);
        request.setBlockSize(100L);
        request.setPriority(50);
        request.setBlockCacheKeys(List.of());
        context = new BalanceContext(config);
        context.setRequest(request);
        context.setSchedulingMetadata(SchedulingMetadata.explicit(
                50, System.currentTimeMillis() + 60_000L));
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void pvCapturesRealCacheQueryAndSelectionAcrossRetries() {
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of(), CacheMatchSource.KVCM, 37L, 100L));
        for (int attempt = 0; attempt < 2; attempt++) {
            var selected = select();
            var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
            assertEquals("CostBasedPrefill", decision.strategy());
            assertEquals("10.0.0.1:8080@0", decision.selectedEndpoint());
            assertEquals(1, decision.candidates().size());
            assertTrue(decision.candidates().getFirst().selected());
            assertEquals(selected.serverStatus().getPrefillTime(),
                    decision.candidates().getFirst().estimatedTtftMs());
            assertEquals(decision.selectionReason(), context.getRoutingTelemetry().selectionReasons().get(RoleType.PREFILL));
        }
        assertEquals("KVCM", context.getRoutingTelemetry().cacheSource());
        assertEquals(2, context.getRoutingTelemetry().cacheQueryCount());
        assertEquals(37L, context.getRoutingTelemetry().cacheQueryUs());
        assertEquals("10.0.0.1", context.getRoutingTelemetry().cacheSelections().get(RoleType.PREFILL).selectedIp());
        assertEquals(1, context.getRoutingTelemetry().routingDecisions().size());
        assertEquals(2, context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL).routingAttempt());
    }

    @Test
    void invalidPredictionCannotSilentlyRemoveARegisteredPrefill() {
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator()
                .setExpression("-1");
        publish("10.0.0.2", 8080);

        assertThrows(org.flexlb.balance.prediction.InvalidPrefillPredictionException.class,
                () -> strategy.prepare(context, RoleType.PREFILL, null));
        verify(cache).findMatchingEngines(any());
    }

    @Test
    void pvShowsKvcmTopThreeAndTheSelectedWorkersHitEvenWhenItIsNotInTheTopThree() throws Exception {
        configureFocusedAffinity();
        Map<String, HostCacheMatch> hits = new LinkedHashMap<>();
        hits.put("10.0.0.5:8080@0", HostCacheMatch.local(2));
        hits.put("10.0.0.1:8080@0", new HostCacheMatch(0, 1));
        hits.put("10.0.0.4:8080@0", HostCacheMatch.local(3));
        hits.put("10.0.0.3:8080@0", HostCacheMatch.local(4));
        hits.put("10.0.0.2:8080@0", HostCacheMatch.local(5));
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                hits, CacheMatchSource.KVCM, 4L, 100L));

        var selected = select();
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertEquals("10.0.0.1", selected.serverStatus().getServerIp());
        assertEquals("FASTEST_IS_CACHE_BEST", decision.cacheAffinityDecision());
        var json = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(JsonUtils.toStringOrEmpty(new PvLogData(context)))
                .path("routingDecisions").get(0);
        assertEquals("10.0.0.1:8080@0", json.path("selectedEndpoint").asText());
        assertEquals("FASTEST_IS_CACHE_BEST", json.path("cacheAffinityDecision").asText());
        assertEquals(5, json.path("kvcmMatches").path("reportedNodeCount").asInt());
        assertEquals(100, json.path("kvcmMatches").path("selectedHitTokens").asLong());
        var topHits = json.path("kvcmMatches").path("topHits");
        assertEquals(3, topHits.size());
        assertEquals(500, topHits.path("10.0.0.2:8080@0").asLong());
        assertEquals(400, topHits.path("10.0.0.3:8080@0").asLong());
        assertEquals(300, topHits.path("10.0.0.4:8080@0").asLong());
    }

    @Test
    void pvDistinguishesKvcmWithoutHitsFromLocalCacheMatching() throws Exception {
        configureFocusedAffinity();
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080@0", HostCacheMatch.local(0)),
                CacheMatchSource.KVCM, 1L, 100L));

        select();
        var decision = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(JsonUtils.toStringOrEmpty(new PvLogData(context)))
                .path("routingDecisions").get(0);
        assertEquals("NO_CACHE_ANYWHERE_KVCM",
                decision.path("cacheAffinityDecision").asText());
        assertEquals(1, decision.path("kvcmMatches").path("reportedNodeCount").asInt());
        assertTrue(decision.path("kvcmMatches").path("topHits").isEmpty());
        assertEquals(0, decision.path("kvcmMatches").path("selectedHitTokens").asLong());

        when(cache.findMatchingEngines(any())).thenReturn(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC));
        select();
        decision = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(JsonUtils.toStringOrEmpty(new PvLogData(context)))
                .path("routingDecisions").get(0);
        assertFalse(decision.has("kvcmMatches"));
    }

    @ParameterizedTest
    @CsvSource({
            "1791, KVCM, true, -1, NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE",
            "1791, KVCM, true, 0, NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE",
            "1792, KVCM, true, 0, NO_CACHE_ANYWHERE_KVCM",
            "1793, KVCM, true, 0, NO_CACHE_ANYWHERE_KVCM",
            "1792, KVCM, true, -1, NO_CACHE_ANYWHERE_KVCM",
            "1792, KVCM, false, -1, NO_CACHE_ANYWHERE",
            "1792, KVCM, true, 1, NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT",
            "1792, LOCAL_SYNC, true, 0, NO_CACHE_ANYWHERE",
            "1791, LOCAL_SYNC, true, 0, NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE",
            "1791, KVCM, false, 0, NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE",
            "1791, KVCM, true, 1, NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE"
    })
    void distinguishesKvcmMisses(long inputTokens, CacheMatchSource source, boolean querySucceeded,
                                long matchedBlocks, String expectedDecision) {
        configureFocusedAffinity();
        context.getRequest().setSeqLen(inputTokens);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                matchedBlocks < 0 ? Map.of() : Map.of("10.0.0.1:8080@0", HostCacheMatch.local(matchedBlocks)),
                source, 1L, 1792L, querySucceeded));

        select();
        assertEquals(expectedDecision, context.getRoutingTelemetry().routingDecisions()
                .get(RoleType.PREFILL).cacheAffinityDecision());
        verify(reporter, never()).reportCacheAffinityDecision(any(), anyString(), anyString());
        assertEquals(expectedDecision, context.getMetrics().getPrefillRouteMetrics().affinityDecision());
    }

    @ParameterizedTest
    @CsvSource({
            "10.0.0.1:8080@0, 0, NO_CACHE_ANYWHERE_KVCM",
            "10.0.0.2:8080@0, 0, NO_CACHE_ANYWHERE_KVCM",
            "10.0.0.1:8080@0, 1, NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT",
            "10.0.0.2:8080@0, 1, NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT"
    })
    void checksGlobalKvcmHitsAcrossAllReportedNodes(String workerKey, long globalBlocks, String expectedDecision) {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setRemoteDiscount(0.0);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080", HostCacheMatch.local(0),
                        workerKey, new HostCacheMatch(0, globalBlocks)),
                CacheMatchSource.KVCM, 1L, 100L));

        select();
        assertEquals(expectedDecision, context.getRoutingTelemetry().routingDecisions()
                .get(RoleType.PREFILL).cacheAffinityDecision());
        assertEquals(expectedDecision, context.getMetrics().getPrefillRouteMetrics().affinityDecision());
    }

    @Test
    void selectionDoesNotOwnTheRequestDeadline() {
        context.setSchedulingMetadata(SchedulingMetadata.explicit(
                50, System.currentTimeMillis() - 1L));

        var ignored = select();
        assertTrue(ignored.serverStatus().isSuccess());
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.1:8080@0", "10.0.0.1:8080"})
    void selectedPrefillDefersHitMetricsUntilWorkerFeedback(String cacheWorkerKey) {
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of(cacheWorkerKey, HostCacheMatch.local(2)),
                CacheMatchSource.KVCM, 0L, 100L));

        var selected = select();
        verify(reporter, never()).reportCacheHitMetrics(
                any(), anyString(), anyLong(), anyLong(), anyDouble());
        verify(reporter, never()).reportKvcmSelectedMatch(
                any(), anyString(), anyLong(), anyLong(), anyLong());
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertEquals(200L, decision.kvcmMatches().selectedHitTokens());
        assertEquals(Map.of(cacheWorkerKey, 200L), decision.kvcmMatches().topHits());
    }

    @ParameterizedTest
    @CsvSource({"2, 8, 0.0, 800, 200, 200", "2, 8, 0.2, 800, 320, 320", "2, 8, 1.0, 800, 800, 800",
            "0, 8, 0.0, 800, 0, 0", "2, 10, 0.2, 900, 360, 360", "10, 10, 0.2, 900, 900, 1000",
            "2, 2, 0.2, 200, 200, 200", "0, 0, 0.2, 0, 0, 0", "0, 9223372036854775807, 0.2, 900, 900, 1000"})
    void globalHitsDriveWorkWhileAffinityAndRoutingMetricsKeepRemoteDiscount(long localBlocks,
                                                                             long globalBlocks,
                                                                             double remoteDiscount,
                                                                             long computeHitTokens,
                                                                             long affinityHitTokens,
                                                                             long routingHitTokens) {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setRemoteDiscount(remoteDiscount);
        when(cache.findMatchingEngines(any())).thenReturn(
                new CacheMatchResult(Map.of("10.0.0.1:8080@0", new HostCacheMatch(localBlocks, globalBlocks)),
                        CacheMatchSource.KVCM, 0L, 100L));

        RequestWorkLoadEstimateResult inputs = strategy.prepare(context, RoleType.PREFILL, null);
        PrefillCandidateWork prepared = inputs.candidates().getFirst();
        assertEquals(affinityHitTokens, prepared.cacheHitTokens());
        assertEquals(routingHitTokens, prepared.routingCacheMatchTokens());
        assertEquals(1_000L - computeHitTokens, prepared.predictedMs());

        var selected = select();
        assertEquals(affinityHitTokens, selected.serverStatus().getDebugInfo().getHitCacheLen());
        assertEquals(1_000L - computeHitTokens, selected.predictedMs());
        assertEquals(affinityHitTokens, selected.hitCacheTokens());
        assertEquals(routingHitTokens, context.getMetrics().getPrefillRouteMetrics().selectedHitTokens());
    }

    @ParameterizedTest
    @CsvSource({"1, 1, 200", "0.5, 0.2, 780", "0, 1, 400", "1, 0, 800", "0, 0, 1000", "2, 2, 100", "0.333, 0, 933",
            "1.7976931348623157E308, 1, 100"})
    void weightsOnlyChangePredictedWork(double localWeight, double remoteWeight, long expectedWork) {
        configureFocusedAffinity();
        var estimator = config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator();
        estimator.setLocalWeight(localWeight);
        estimator.setRemoteWeight(remoteWeight);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080@0", new HostCacheMatch(2L, 8L)), CacheMatchSource.KVCM, 0L, 100L));
        assertEquals(expectedWork,
                strategy.prepare(context, RoleType.PREFILL, null).candidates().getFirst().predictedMs());
        var selected = select();
        assertEquals(expectedWork, selected.predictedMs());
        assertEquals(320L, selected.serverStatus().getDebugInfo().getHitCacheLen());
        assertEquals(320L, context.getMetrics().getPrefillRouteMetrics().selectedHitTokens());
    }

    @ParameterizedTest
    @CsvSource({"1000,100,8,0,1,1,200", "1000,100,9,0,1,1,100", "1000,100,9,1,1.5,1,100", "1000,100,9,1,2,1,100",
            "1000,100,9,3,1.5,1,100", "134920,2304,58,0,2,1,2304", "134920,2304,58,1,2,1,2304", "80,100,1,0,1,1,80"})
    void predictedCacheCreditAlwaysLeavesOneBlock(long seqLen,
                                                  long blockSize,
                                                  long globalBlocks,
                                                  long localBlocks,
                                                  double localWeight,
                                                  double remoteWeight,
                                                  long expectedWorkMs) {
        configureFocusedAffinity();
        var estimator = config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator();
        estimator.setLocalWeight(localWeight);
        estimator.setRemoteWeight(remoteWeight);
        context.getRequest().setSeqLen(seqLen);
        context.getRequest().setBlockSize(blockSize);
        when(cache.findMatchingEngines(any())).thenReturn(
                new CacheMatchResult(Map.of("10.0.0.1:8080@0", new HostCacheMatch(localBlocks, globalBlocks)),
                        CacheMatchSource.KVCM, 0L, blockSize));

        assertEquals(expectedWorkMs,
                strategy.prepare(context, RoleType.PREFILL, null).candidates().getFirst().predictedMs());
        var selected = select();
        assertEquals(expectedWorkMs, selected.predictedMs());
    }

    @ParameterizedTest
    @CsvSource({"1,1,100,100,100,100,100", "0.5,0.2,670,700,730,760,820", "1,0,500,600,700,800,1000"})
    void sharedGlobalHitsCreditOnlyTheMissingPrefixOfEachCandidate(double localWeight,
                                                                   double remoteWeight,
                                                                   long firstWork,
                                                                   long secondWork,
                                                                   long thirdWork,
                                                                   long fourthWork,
                                                                   long missingHostWork) {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setRemoteDiscount(0.0);
        var estimator = config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator();
        estimator.setLocalWeight(localWeight);
        estimator.setRemoteWeight(remoteWeight);
        for (int i = 2; i <= 5; i++) {
            publish("10.0.0." + i, 8080);
        }
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080@0", new HostCacheMatch(5L, 8L), "10.0.0.2:8080@0", new HostCacheMatch(4L, 9L),
                        "10.0.0.3:8080@0", new HostCacheMatch(3L, 7L), "10.0.0.4:8080@0", HostCacheMatch.local(2L)),
                CacheMatchSource.KVCM, 0L, 100L));

        RequestWorkLoadEstimateResult inputs = strategy.prepare(context, RoleType.PREFILL, null);

        assertEquals(900L, context.getGlobalHitTokens());
        assertEquals(5, inputs.candidates().size());
        long[] work = {firstWork, secondWork, thirdWork, fourthWork, missingHostWork};
        for (PrefillCandidateWork candidate : inputs.candidates()) {
            int index = Integer.parseInt(candidate.endpoint().getIp().substring("10.0.0.".length())) - 1;
            assertEquals(work[index], candidate.predictedMs());
            long localHit = index < 4 ? (5L - index) * 100L : 0L;
            assertEquals(localHit, candidate.cacheHitTokens());
            assertEquals(localHit, candidate.routingCacheMatchTokens());
        }
        var selected = select();
        assertEquals("10.0.0.1", selected.serverStatus().getServerIp());
        assertEquals(firstWork, selected.predictedMs());
        assertEquals(500L, selected.serverStatus().getDebugInfo().getHitCacheLen());
        for (var candidate : context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL).candidates()) {
            int index = Integer.parseInt(
                    candidate.endpoint().substring("10.0.0.".length(), candidate.endpoint().indexOf(':'))) - 1;
            assertEquals(work[index], candidate.incomingPrefillMs());
        }
    }

    @ParameterizedTest
    @CsvSource({"KVCM,true", "KVCM,false", "LOCAL_SYNC,true", "LOCAL_STANDBY,true"})
    void nextCacheQueryClearsPreviousGlobalHits(CacheMatchSource source, boolean querySucceeded) {
        configureFocusedAffinity();
        when(cache.findMatchingEngines(any()))
                .thenReturn(
                        new CacheMatchResult(Map.of("10.0.0.1:8080@0", new HostCacheMatch(2L, 8L)),
                                CacheMatchSource.KVCM, 0L, 100L),
                        new CacheMatchResult(Map.of(), source, 0L, 100L, querySucceeded));
        strategy.prepare(context, RoleType.PREFILL, null);
        assertEquals(800L, context.getGlobalHitTokens());

        RequestWorkLoadEstimateResult inputs = strategy.prepare(context, RoleType.PREFILL, null);

        assertEquals(0L, context.getGlobalHitTokens());
        assertEquals(1_000L, inputs.candidates().getFirst().predictedMs());
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 2_304L})
    void sharedGlobalPrefixDoesNotCountTargetLocalHitsTwice(long requestBlockSize) {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setRemoteDiscount(0.0);
        config.getRouter()
                .getRoles()
                .getPrefill()
                .getExecutionTimeEstimator()
                .setExpression("1000 + 0.069*sum(computeTokens) + 0.0008*sum(hitCacheTokens)");
        registry.get(RoleType.PREFILL, "10.0.0.1:8080@0").close();
        for (int i = 2; i <= 4; i++) {
            publish("10.0.0." + i, 8080);
        }
        context.getRequest().setSeqLen(194_662L);
        context.getRequest().setBlockSize(requestBlockSize);
        context.getRequest().setCacheKeyBlockSize(2_304L);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.2:8080@0", HostCacheMatch.local(55L), "10.0.0.3:8080@0", new HostCacheMatch(7L, 55L)),
                CacheMatchSource.KVCM, 0L, 2_304L));

        RequestWorkLoadEstimateResult inputs = strategy.prepare(context, RoleType.PREFILL, null);

        assertEquals(126_720L, context.getGlobalHitTokens());
        assertEquals(3, inputs.candidates().size());
        for (PrefillCandidateWork candidate : inputs.candidates()) {
            assertEquals(5_789L, candidate.predictedMs());
            long localHit = switch (candidate.endpoint().getIp()) {
                case "10.0.0.2" -> 126_720L;
                case "10.0.0.3" -> 16_128L;
                default -> 0L;
            };
            assertEquals(localHit, candidate.cacheHitTokens());
            assertEquals(localHit, candidate.routingCacheMatchTokens());
        }
        verify(cache).findMatchingEngines(new CacheMatchQuery(context.getRequestId(),
                context.getRequest().getBlockCacheKeys(), 2_304L, RoleType.PREFILL, null));
    }

    @ParameterizedTest
    @CsvSource({"false,100,200,300,900,900", "true,100,200,300,900,900", "false,0,200,300,800,800",
            "true,0,200,300,800,800", "false,0,0,300,700,700", "true,0,0,300,700,700", "false,0,0,0,600,500",
            "true,0,0,0,600,500"})
    void bothSelectionPathsKeepBlockSizePriorityAndPerWorkerFallback(boolean prepared,
                                                                     long resultBlockSize,
                                                                     long requestBlockSize,
                                                                     long cacheKeyBlockSize,
                                                                     long firstWorkMs,
                                                                     long secondWorkMs) {
        publish("10.0.0.2", 8080);
        for (var entry : new WorkerDirectory(registry).getPrefillCandidates(RoleType.PREFILL)) {
            long workerBlockSize = entry.endpoint().getIp().equals("10.0.0.1") ? 400L : 500L;
            CacheStatus cacheStatus = new CacheStatus();
            cacheStatus.setBlockSize(workerBlockSize);
            WorkerStatus status = entry.endpoint().getStatus();
            status.lock.lock();
            try {
                status.publishCacheStatus(cacheStatus);
            } finally {
                status.lock.unlock();
            }
        }
        context.getRequest().setBlockSize(requestBlockSize);
        context.getRequest().setCacheKeyBlockSize(cacheKeyBlockSize);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080@0", HostCacheMatch.local(1L), "10.0.0.2:8080@0", HostCacheMatch.local(1L)),
                CacheMatchSource.LOCAL_SYNC, 0L, resultBlockSize));

        Map<String, Long> predictedMs;
        if (prepared) {
            predictedMs = strategy.prepare(context, RoleType.PREFILL, null)
                    .candidates()
                    .stream()
                    .collect(Collectors.toMap(PrefillCandidateWork::address, PrefillCandidateWork::predictedMs));
        } else {
            select();
            predictedMs = context.getRoutingTelemetry()
                    .routingDecisions()
                    .get(RoleType.PREFILL)
                    .candidates()
                    .stream()
                    .collect(Collectors.toMap(candidate -> candidate.endpoint(),
                            candidate -> candidate.incomingPrefillMs()));
        }
        assertEquals(Map.of("10.0.0.1:8080@0", firstWorkMs, "10.0.0.2:8080@0", secondWorkMs), predictedMs);
    }

    @ParameterizedTest
    @CsvSource({"LOCAL_SYNC,1,1,800", "LOCAL_STANDBY,1,1,800", "LOCAL_SYNC,0.5,0.2,900", "LOCAL_STANDBY,0.5,0.2,900"})
    void localCacheSourcesKeepLocalWorkCredit(CacheMatchSource source,
                                              double localWeight,
                                              double remoteWeight,
                                              long expectedWork) {
        configureFocusedAffinity();
        var estimator = config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator();
        estimator.setLocalWeight(localWeight);
        estimator.setRemoteWeight(remoteWeight);
        when(cache.findMatchingEngines(any())).thenReturn(
                new CacheMatchResult(Map.of("10.0.0.1:8080@0", HostCacheMatch.local(2L)), source, 0L, 100L));

        PrefillCandidateWork candidate = strategy.prepare(context, RoleType.PREFILL, null).candidates().getFirst();

        assertEquals(0L, context.getGlobalHitTokens());
        assertEquals(expectedWork, candidate.predictedMs());
        assertEquals(200L, candidate.cacheHitTokens());
        assertEquals(200L, candidate.routingCacheMatchTokens());
    }

    @Test
    void telemetryFailureNeverChangesAValidSelection() {
        doThrow(new IllegalStateException("metrics unavailable"))
                .when(reporter).reportPrefillSelectedEstimates(
                        any(), any(), any(), Mockito.anyLong(), Mockito.anyLong());

        var selected = select();
        assertTrue(selected.serverStatus().isSuccess());
        assertEquals("10.0.0.1", selected.serverStatus().getServerIp());
    }

    @Test
    void cacheLeaderInsideTtftCapOverridesTheBaselineCandidate() {
        configureAffinity(600L, 5.0);

        var selected = select();
        assertEquals("10.0.0.2", selected.serverStatus().getServerIp());
        assertEquals("CACHE_LEADER", context.getMetrics().getPrefillRouteMetrics().affinityDecision());
        assertEquals("CACHE_LEADER", context.getRoutingTelemetry().routingDecisions()
                .get(RoleType.PREFILL).cacheAffinityDecision());
    }

    @Test
    void transientAndTotalPrefillKvShortageDoNotHideTheCacheLeaderFromQueueSelection() {
        configureAffinity(600L, 5.0);
        PrefillEndpoint cached = (PrefillEndpoint) registry.get(RoleType.PREFILL, "10.0.0.2:8080@0");
        WorkerStatus worker = cached.getStatus();
        StrategyTestSupport.publish(worker, StrategyTestSupport.response(
                RoleType.PREFILL, true, 0L, 500L, worker.appliedStatusCursor().statusVersion() + 1L));

        var selected = select();
        assertEquals("10.0.0.2", selected.serverStatus().getServerIp());
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertEquals(2, decision.candidateWorkerCount());
        assertEquals("CACHE_LEADER", decision.cacheAffinityDecision());
    }

    @Test
    void cacheLeaderAtEndOfFullFleetRemainsEligible() {
        configureAffinity(600L, 5.0);
        String cacheLeader = null;
        for (int index = 2; index < FULL_FLEET_SIZE; index++) {
            String ip = "10.2." + index / 250 + '.' + index % 250;
            publish(ip, 8080);
            cacheLeader = ip;
        }
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of(cacheLeader + ":8080", HostCacheMatch.local(5)),
                CacheMatchSource.KVCM, 0L, 100L));

        var selected = select();
        assertEquals(cacheLeader, selected.serverStatus().getServerIp(),
                "cache-first must inspect the complete 750-node fleet");
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertTrue(decision.snapshotTruncated());
        assertEquals(5, decision.candidates().size());
        assertEquals(5, decision.candidates().stream().map(candidate -> candidate.endpoint()).distinct().count());
        assertEquals(cacheLeader + ":8080@0", decision.candidates().getFirst().endpoint());
        assertTrue(decision.candidates().getFirst().selected());
    }

    @Test
    void exactCostTiesRotateWithoutShrinkingTheCandidateFleet() {
        publish("10.0.0.2", 8080);
        publish("10.0.0.3", 8080);

        Set<String> selectedIps = new HashSet<>();
        for (int index = 0; index < 3; index++) {
            context.getRequest().setRequestId(Long.toString(30_000L + index));
            var selected = select();
            selectedIps.add(selected.serverStatus().getServerIp());
        }

        assertEquals(Set.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), selectedIps);
    }

    @ParameterizedTest
    @CsvSource({"400,5.0,OVER_CAP", "600,60.0,LOW_CACHE_HIT"})
    void cacheAffinityGateUsesTheConfiguredBaselineCandidate(
            long maxExtraTtftMs, double minPrefixHitPercent, String reason) {
        configureAffinity(maxExtraTtftMs, minPrefixHitPercent);

        var selected = select();
        assertEquals("10.0.0.1", selected.serverStatus().getServerIp());
        assertEquals(reason, context.getMetrics().getPrefillRouteMetrics().affinityDecision());
        assertEquals(reason, context.getRoutingTelemetry().routingDecisions()
                .get(RoleType.PREFILL).cacheAffinityDecision());
    }

    @Test
    void overCapUsesTheSmallestIncreaseAmongHigherHitCandidatesMeetingTheHitRate() {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setMinPrefixHitPercent(20.0);
        PrefillCandidateSet candidates = candidates(
                new long[] {100L, 111L, 180L, 145L, 130L},
                new long[] {0L, 100L, 900L, 300L, 200L});

        assertEquals(0, strategy.selectBestCandidate(
                candidates, 100L, RoleType.PREFILL, null, 1_000L, config));

        assertEquals("OVER_CAP", candidates.affinityDecision());
        assertEquals("10.1.0.5:8080", candidates.cacheAffinity().fastestHigherHitEndpoint());
        assertEquals(20L, candidates.cacheAffinity().overCapMs());
        candidates.reset(1);
        assertTrue(candidates.cacheAffinity() == null);
    }

    @ParameterizedTest
    @CsvSource({"500,5.0,CACHE_LEADER", "400,60.0,LOW_CACHE_HIT"})
    void nonOverCapDecisionsDoNotReportAnExcessSample(
            long maxExtraTtftMs, double minPrefixHitPercent, String reason) {
        configureAffinity(maxExtraTtftMs, minPrefixHitPercent);

        select();
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertEquals(reason, decision.cacheAffinityDecision());
        assertTrue(decision.cacheAffinity().overCapMs() == null);
        verify(reporter, never()).reportCacheAffinityOverCap(any(), anyString(), anyLong());
    }

    @Test
    void unknownHigherHitTtftDoesNotProduceAnOverCapAmount() {
        configureFocusedAffinity();
        PrefillCandidateSet candidates = candidates(new long[] {100L, -1L}, new long[] {0L, 500L});

        strategy.selectBestCandidate(candidates, 100L, RoleType.PREFILL, null, 1_000L, config);

        assertEquals("OVER_CAP", candidates.affinityDecision());
        assertTrue(candidates.cacheAffinity().fastestHigherHitTtftMs() == null);
        assertTrue(candidates.cacheAffinity().overCapMs() == null);
    }

    @Test
    void saturatedTtftCutoffDoesNotCreateAFalseOverCapAmount() {
        configureFocusedAffinity();
        config.getRouter().getRoles().getPrefill().getCacheAffinity().setMaxExtraTtftMs(Long.MAX_VALUE);
        PrefillCandidateSet candidates = candidates(
                new long[] {100L, Long.MAX_VALUE}, new long[] {0L, 500L});

        strategy.selectBestCandidate(candidates, 100L, RoleType.PREFILL, null, 1_000L, config);

        assertEquals("CACHE_LEADER", candidates.affinityDecision());
        assertEquals(Long.MAX_VALUE, candidates.cacheAffinity().ttftCutoffMs());
        assertTrue(candidates.cacheAffinity().overCapMs() == null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rotatesOnlyAmongWorkersTiedOnTtftAndLocalHits(boolean useCacheAffinity) {
        configureFocusedAffinity();
        var affinity = config.getRouter().getRoles().getPrefill().getCacheAffinity();
        affinity.setMaxExtraTtftMs(useCacheAffinity ? 10L : 0L);
        affinity.setRemoteDiscount(1.0);
        for (int i = 2; i <= 5; i++) {
            publish("10.0.0." + i, 8080);
        }
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.1:8080@0", HostCacheMatch.local(1L), "10.0.0.2:8080@0", new HostCacheMatch(2L, 5L),
                        "10.0.0.3:8080@0", HostCacheMatch.local(5L), "10.0.0.4:8080@0", HostCacheMatch.local(5L),
                        "10.0.0.5:8080@0", HostCacheMatch.local(8L)),
                CacheMatchSource.KVCM, 0L, 100L));
        var prepared = strategy.prepare(context, RoleType.PREFILL, null);
        assertNotNull(prepared);

        for (String expectedIp : List.of("10.0.0.3", "10.0.0.4", "10.0.0.3")) {
            var selected = strategy.selectPrepared(context, RoleType.PREFILL, null, prepared,
                    candidate -> switch (candidate.endpoint().getIp()) {
                        case "10.0.0.1" -> useCacheAffinity ? 4L : 5L;
                        case "10.0.0.5" -> 20L;
                        default -> 5L;
                    });
            assertEquals(expectedIp, selected.serverStatus().getServerIp());
        }
    }

    @Test
    void equalMaximumCacheHitPreservesBestOnlySelection() {
        configureFocusedAffinity();
        PrefillCandidateSet candidates = candidates(
                new long[] {100L, 105L, 1_000L},
                new long[] {500L, 500L, 0L});

        int selectedIndex = strategy.selectBestCandidate(
                candidates, 100L, RoleType.PREFILL, null, 1_000L, config);

        assertEquals(0, selectedIndex,
                "equal cache hit must preserve the minimum-TTFT baseline");
        assertEquals("FASTEST_IS_CACHE_BEST", candidates.affinityDecision());
        verify(reporter, never()).reportCacheAffinityDecision(any(), anyString(), anyString());
    }

    @Test
    void zeroCacheHitEverywhereReportsNoCacheAnywhere() {
        configureFocusedAffinity();
        PrefillCandidateSet candidates = candidates(
                new long[] {100L, 105L, 1_000L},
                new long[] {0L, 0L, 0L});

        int selectedIndex = strategy.selectBestCandidate(
                candidates, 100L, RoleType.PREFILL, null, 1_000L, config);

        assertEquals(0, selectedIndex);
        assertEquals("NO_CACHE_ANYWHERE", candidates.affinityDecision());
    }

    @Test
    void strictlyGreaterCacheHitOverridesTheBaselineCandidate() {
        configureFocusedAffinity();
        PrefillCandidateSet candidates = candidates(
                new long[] {100L, 105L, 1_000L},
                new long[] {500L, 600L, 0L});

        int selectedIndex = strategy.selectBestCandidate(
                candidates, 100L, RoleType.PREFILL, null, 1_000L, config);

        assertEquals(1, selectedIndex);
        assertEquals("CACHE_LEADER", candidates.affinityDecision());
    }

    @Test
    void cacheLeaderRemainsPreferredAcrossRepeatedSelections() {
        configureAffinity(600L, 5.0);

        var selected = select();
        assertEquals("10.0.0.2", selected.serverStatus().getServerIp());
        context.getRequest().setRequestId("20002");

        selected = select();
        assertEquals("10.0.0.2", selected.serverStatus().getServerIp());
        assertEquals("CACHE_LEADER", context.getMetrics().getPrefillRouteMetrics().affinityDecision());
    }

    @Test
    void unknownEngineWorkDoesNotFabricateAnEstimate() {
        configureAffinity(600L, 5.0);
        var endpoint = (PrefillEndpoint) registry.get(RoleType.PREFILL, "10.0.0.2:8080@0");
        var status = endpoint.getStatus();
        var response = StrategyTestSupport.response(RoleType.PREFILL, true,
                1_000_000L, 1_000_000L, status.appliedStatusCursor().statusVersion() + 1L);
        var task = TaskInfoPB.newBuilder();
        task.setRequestIdStr("4");
        WorkerStatusPBs.phase(task, TaskPhase.RUNNING);
        task.setInputLength(100L);
        WorkerStatusPBs.setRunningTasks(response, Map.of("4", task));
        status.lock.lock();
        try {
            endpoint.applyPreparedStatus(status,
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build())))
                    .run();
        } finally {
            status.lock.unlock();
        }
        assertEquals(-1L, endpoint.prefillQueue().estimatedWorkMs(0L));

        var selected = select();
        assertEquals("10.0.0.1", selected.serverStatus().getServerIp());
        var decision = context.getRoutingTelemetry().routingDecisions().get(RoleType.PREFILL);
        assertNull(decision.candidates().stream().filter(candidate -> candidate.endpoint().equals(endpoint.ipPort()))
                .findFirst().orElseThrow().estimatedTtftMs());
    }

    private org.flexlb.balance.schedulev2.request.SelectedPrefill select() {
        var prepared = strategy.prepare(context, RoleType.PREFILL, null);
        assertNotNull(prepared);
        return strategy.selectPrepared(context, RoleType.PREFILL, null, prepared,
                candidate -> candidate.endpoint().prefillQueue().estimatedWorkMs(candidate.predictedMs()));
    }

    private void configureFocusedAffinity() {
        RoutingConfig.CacheAffinityConfig affinity =
                new RoutingConfig.CacheAffinityConfig();
        affinity.setMaxExtraTtftMs(10L);
        affinity.setMinPrefixHitPercent(0.0);
        config.getRouter().getRoles().getPrefill().setCacheAffinity(affinity);
    }

    private PrefillCandidateSet candidates(long[] ttftMs, long[] hitTokens) {
        assertEquals(ttftMs.length, hitTokens.length);
        PrefillCandidateSet candidates = new PrefillCandidateSet();
        candidates.reset(ttftMs.length);
        for (int i = 0; i < ttftMs.length; i++) {
            String ip = "10.1.0." + (i + 1);
            PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
            when(endpoint.getIp()).thenReturn(ip);
            WorkerStatus status = mock(WorkerStatus.class);
            when(status.getMetricIpPort()).thenReturn(ip + ":8080");
            when(endpoint.getStatus()).thenReturn(status);
            candidates.addCandidate(ip + ":8080", endpoint, ttftMs[i], Math.max(0L, ttftMs[i]), hitTokens[i],
                    hitTokens[i], 0L, i + 1L);
        }
        return candidates;
    }

    private void configureAffinity(long maxExtraTtftMs, double minPrefixHitPercent) {
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator()
                .setExpression("sum(computeTokens) + 2*sum(hitCacheTokens)");
        // Affinity gate cases compare local-cache costs without shared remote work savings.
        config.getRouter().getRoles().getPrefill().getExecutionTimeEstimator().setRemoteWeight(0.0);
        RoutingConfig.CacheAffinityConfig affinity =
                new RoutingConfig.CacheAffinityConfig();
        affinity.setMaxExtraTtftMs(maxExtraTtftMs);
        affinity.setMinPrefixHitPercent(minPrefixHitPercent);
        config.getRouter().getRoles().getPrefill().setCacheAffinity(affinity);

        publish("10.0.0.2", 8080);
        context.getRequest().setRequestId("20001");
        context.getRequest().setBlockCacheKeys(List.of(1L, 2L, 3L, 4L, 5L));
        context.getRequest().setCacheKeyBlockSize(100L);
        when(cache.findMatchingEngines(any())).thenReturn(new CacheMatchResult(
                Map.of("10.0.0.2:8080", HostCacheMatch.local(5)),
                CacheMatchSource.KVCM, 0L, 100L));
    }

    private static void clearRegisteredEndpoints() {
        for (RoleType role : RoleType.values()) {
            var endpoints = EndpointRegistry.getEndpointsByRole(role);
            if (endpoints != null) {
                endpoints.clear();
            }
        }
    }

    private void publish(String ip, int port) {
        WorkerStatus status = StrategyTestSupport.workerStatus(
                RoleType.PREFILL, null, ip, port, port + 1,
                true, 1_000_000L, 1_000_000L);
        StrategyTestSupport.publishEndpoint(
                registry, RoleType.PREFILL, status.getLogicalIpPort(), status);
    }
}

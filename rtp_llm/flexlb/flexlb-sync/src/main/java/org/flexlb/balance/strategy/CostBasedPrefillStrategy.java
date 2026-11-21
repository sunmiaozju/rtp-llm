package org.flexlb.balance.strategy;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.estimate.EndpointPrefillLoadProvider;
import org.flexlb.balance.estimate.PrefillLoadProvider;
import org.flexlb.balance.estimate.PrefillTtftEstimate;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.cache.domain.CacheMatchQuery;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.BalanceMetrics.PrefillRouteMetrics;
import org.flexlb.dao.cache.HostCacheMatch;
import org.flexlb.dao.loadbalance.DebugInfo;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.pv.RoutingDecision;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.WorkerDirectory;
import org.flexlb.util.CommonUtils;
import org.flexlb.util.Logger;
import org.flexlb.util.ReadOnlyListFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

@Component
public class CostBasedPrefillStrategy {

    private static final ThreadLocal<PrefillCandidateSet> CANDIDATES =
            ThreadLocal.withInitial(PrefillCandidateSet::new);

    private final WorkerDirectory workerDirectory;
    private final CacheAwareService cacheAwareService;
    private final EngineHealthReporter engineHealthReporter;
    private final PrefillLoadProvider loadProvider;
    private final EndpointRoundRobin tieRotation = new EndpointRoundRobin();

    public CostBasedPrefillStrategy(WorkerDirectory workerDirectory,
                                    CacheAwareService cacheAwareService,
                                    EngineHealthReporter engineHealthReporter) {
        this(workerDirectory, cacheAwareService, engineHealthReporter, new EndpointPrefillLoadProvider());
    }

    @Autowired
    public CostBasedPrefillStrategy(WorkerDirectory workerDirectory,
                                    CacheAwareService cacheAwareService,
                                    EngineHealthReporter engineHealthReporter,
                                    PrefillLoadProvider loadProvider) {
        this.workerDirectory = workerDirectory;
        this.cacheAwareService = cacheAwareService;
        this.engineHealthReporter = engineHealthReporter;
        this.loadProvider = loadProvider;
    }

    public PlacementResult<SelectedRole, RoleType> select(BalanceContext balanceContext,
                                                          RoleType roleType,
                                                          String group) {
        balanceContext.beginRoutingAttempt(roleType);
        String requestId = balanceContext.getRequestId();
        FlexlbConfig config = balanceContext.getConfig();
        long inflightRequestLimit = config.getDispatcher().getType() == DispatcherConfig.Type.NON_BATCH
                ? config.getDispatcher().getMaxInflightPerPrefillWorker() : 0L;

        EndpointDiscovery discovery = discoverAvailableEndpoints(roleType, group);
        if (discovery.candidates().isEmpty()) {
            Logger.debug("Prefill select failed: no registered endpoint matches group, request_id={}",
                    requestId);
            balanceContext.recordSelectionReason(roleType, "NO_MATCHING_ENDPOINTS");
            return classifyAdmissionFailure(discovery, balanceContext.getPriority(), roleType, group, inflightRequestLimit);
        }

        CacheMatchResult cacheMatchResult = getCacheMatchResult(balanceContext, roleType, group);

        PrefillCandidateSet survivors = evaluateCandidates(discovery, balanceContext, cacheMatchResult);

        return finishSelection(balanceContext, roleType, group, discovery.registeredCount(), cacheMatchResult,
                survivors);
    }

    /**
     * 并行准备完整候选集合的缓存匹配和请求自身耗时。排队负载留到调度线程作最终决策时再读取。
     */
    public RequestWorkLoadEstimateResult prepare(BalanceContext context, RoleType role, String group) {
        context.beginRoutingAttempt(role);
        EndpointDiscovery discovery = discoverActiveEndpoints(role, group);
        if (discovery.candidates().isEmpty()) {
            return null;
        }
        CacheMatchResult cacheMatch = getCacheMatchResult(context, role, group);
        PrefillCacheMatchInputs cacheInputs = PrefillCacheMatchInputs.build(context, cacheMatch);
        List<PrefillCandidateWork> candidates = new ArrayList<>(discovery.candidates().size());
        for (EndpointRegistry.PrefillRoutingEntry entry : discovery.candidates()) {
            CacheTokenMatch match = calculateCacheMatch(entry.endpoint(), cacheInputs);
            long computeTokens = Math.max(0L, cacheInputs.seqLen() - match.computeHitTokens());
            long predictedMs = PrefillPredictionBoundary.predictSingleRequestMs(
                    entry.endpoint().getPredictor().evaluator(), cacheInputs.seqLen(), computeTokens,
                    match.computeHitTokens());
            candidates.add(new PrefillCandidateWork(entry.address(), entry.endpoint(), match.effectiveHitTokens(),
                    match.routingHitTokens(), predictedMs));
        }
        return RequestWorkLoadEstimateResult.success(cacheMatch, candidates);
    }

    /**
     * 只在准备阶段确定的候选中选点；评分使用当前队列的累计预测工作量。
     */
    public SelectedPrefill selectPrepared(BalanceContext context,
                                          RoleType role,
                                          String group,
                                          RequestWorkLoadEstimateResult inputs,
                                          ToLongFunction<PrefillCandidateWork> ttft) {
        List<PrefillCandidateWork> preparedCandidates = inputs.candidates();
        PrefillCandidateSet candidates = new PrefillCandidateSet();
        candidates.reset(preparedCandidates.size());
        for (PrefillCandidateWork candidate : preparedCandidates) {
            long estimatedMs = ttft.applyAsLong(candidate);
            PrefillTtftEstimate.Candidate estimate = new PrefillTtftEstimate.Candidate(
                    estimatedMs < 0L
                            ? PrefillTtftEstimate.Candidate.State.UNMODELED_ENGINE_WORK
                            : PrefillTtftEstimate.Candidate.State.MODELED,
                    estimatedMs, candidate.predictedMs(), PrefillTtftEstimate.Candidate.InitialHeadDisposition.NONE, "",
                    null, candidate.cacheHitTokens(), candidate.routingCacheMatchTokens());
            candidates.addCandidate(candidate.address(), candidate.endpoint(), estimate, candidate.cacheHitTokens(),
                    candidate.endpoint().placementVersion());
        }
        CacheMatchResult cacheMatch = inputs.cacheMatch();
        int selectedIndex = selectPrefillIndex(context, role, group, cacheMatch, candidates);
        PrefillEndpoint endpoint = candidates.endpoint(selectedIndex);
        long hitCacheTokens = candidates.cacheHit(selectedIndex);
        ServerStatus status = buildPrefillStatus(endpoint, role, context.getRequestId(),
                candidates.estimatedTtftMs(selectedIndex), hitCacheTokens);

        recordSelectedPrefill(context, role, group, null, cacheMatch, candidates, selectedIndex);
        recordV2PrefillMetrics(context, candidates, selectedIndex);

        long seqLen = context.getRequest().getSeqLen();
        return new SelectedPrefill(endpoint, status, Math.max(0L, Math.min(seqLen, hitCacheTokens)),
                candidates.prefillMs(selectedIndex));
    }

    /**
     * V2 只排除已开始下线的节点，所有仍可接收请求的候选都参与缓存匹配。
     */
    private EndpointDiscovery discoverActiveEndpoints(RoleType role, String group) {
        return discoverEndpoints(role, group, true);
    }

    private PlacementResult<SelectedRole, RoleType> finishSelection(BalanceContext balanceContext,
                                                                    RoleType roleType,
                                                                    String group,
                                                                    Integer totalWorkerCount,
                                                                    CacheMatchResult cacheMatchResult,
                                                                    PrefillCandidateSet survivors) {
        int selectedIndex = selectPrefillIndex(balanceContext, roleType, group, cacheMatchResult, survivors);
        PrefillEndpoint best = survivors.endpoint(selectedIndex);
        WorkerEndpoint.EndpointHold selectedHold =
                workerDirectory.holdEndpoint(roleType, survivors.endpointAddress(selectedIndex));
        if (selectedHold == null || selectedHold.endpoint() != best) {
            recordDecision(balanceContext, roleType, group, totalWorkerCount, survivors, -1,
                    "ENDPOINT_GENERATION_CHANGED", cacheMatchResult);
            if (selectedHold != null) {
                selectedHold.close();
            }
            // The planning endpoint retired or was replaced after the
            // full-fleet snapshot. Re-enter the queue rather than
            // transferring ownership for a stale generation.
            return PlacementResult.blocked(roleType);
        }
        SelectedRole selectedRole = buildSelectedRole(best, roleType, balanceContext.getRequestId(),
                survivors.estimatedTtftMs(selectedIndex), survivors.prefillMs(selectedIndex),
                survivors.cacheHit(selectedIndex), survivors.ownershipVersion(selectedIndex), selectedHold);
        recordSelectedPrefill(balanceContext, roleType, group, totalWorkerCount, cacheMatchResult, survivors,
                selectedIndex);
        if (balanceContext.getConfig().getDispatcher().getType() == DispatcherConfig.Type.NON_BATCH_V2) {
            recordV2PrefillMetrics(balanceContext, survivors, selectedIndex);
            return PlacementResult.success(selectedRole);
        }
        return PlacementResult.success(selectedRole.onCommit(commitTelemetry(roleType, best,
                balanceContext.getRequest().getSeqLen(), survivors.routingCacheMatchTokens(selectedIndex),
                survivors.maximumRoutingCacheMatchTokens, survivors.affinityDecision(), survivors.cacheAffinity())));
    }

    private int selectPrefillIndex(BalanceContext context,
                                   RoleType role,
                                   String group,
                                   CacheMatchResult cacheMatch,
                                   PrefillCandidateSet candidates) {
        long seqLen = context.getRequest().getSeqLen();
        int selectedIndex =
                selectBestCandidate(candidates, candidates.minimumTtftRank, role, group, seqLen, context.getConfig());
        if ("NO_CACHE_ANYWHERE".equals(candidates.affinityDecision())) {
            if (seqLen < cacheMatch.blockSize()) {
                candidates.recordAffinityDecision("NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE");
            } else if (cacheMatch.source() == CacheMatchSource.KVCM && cacheMatch.querySucceeded()) {
                boolean kvcmHasHit =
                        cacheMatch.hostMatches().values().stream().anyMatch(match -> match.globalMatchBlocks() > 0L);
                candidates.recordAffinityDecision(
                        kvcmHasHit ? "NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT" : "NO_CACHE_ANYWHERE_KVCM");
            }
        }
        return selectedIndex;
    }

    private void recordSelectedPrefill(BalanceContext balanceContext,
                                       RoleType roleType,
                                       String group,
                                       Integer totalWorkerCount,
                                       CacheMatchResult cacheMatchResult,
                                       PrefillCandidateSet candidates,
                                       int selectedIndex) {
        PrefillEndpoint best = candidates.endpoint(selectedIndex);
        long bestCacheHit = candidates.cacheHit(selectedIndex);
        if (balanceContext.selectionReason(roleType) == null) {
            balanceContext.recordSelectionReason(roleType, "COST_BASED");
        }
        cacheAwareService.trackRoutingPrediction(balanceContext.getRequestId(), roleType, group, best.getStatus(),
                balanceContext.getRequest().getSeqLen(), bestCacheHit, cacheMatchResult,
                balanceContext.getTheoryHitTokens());
        balanceContext.recordCacheSelection(roleType, best.getIp(), bestCacheHit);
        recordDecision(balanceContext, roleType, group, totalWorkerCount, candidates, selectedIndex,
                balanceContext.selectionReason(roleType), cacheMatchResult);
        long selectedTtft = candidates.estimatedTtftMs(selectedIndex);
        if (selectedTtft >= 0L) {
            reportSelectedEstimates(roleType, best, balanceContext.getConfig(), selectedTtft,
                    candidates.prefillMs(selectedIndex));
        }
    }

    private void recordV2PrefillMetrics(BalanceContext context, PrefillCandidateSet candidates, int selectedIndex) {
        String decision = candidates.affinityDecision();
        Long overCapMs = "OVER_CAP".equals(decision) ? candidates.cacheAffinity().overCapMs() : null;
        if (decision != null && candidates.endpoint(selectedIndex).getStatus().getMetricIpPort() == null) {
            decision = null;
        }
        context.getMetrics()
                .setPrefillRouteMetrics(new PrefillRouteMetrics(candidates.routingCacheMatchTokens(selectedIndex),
                        candidates.maximumRoutingCacheMatchTokens, decision, overCapMs));
    }

    /**
     * Cache counters for the selected route, captured now and reported once the route commits.
     */
    private Runnable commitTelemetry(RoleType roleType,
                                     PrefillEndpoint best,
                                     long seqLen,
                                     long selectedRoutingHitTokens,
                                     long candidateMaxRoutingHitTokens,
                                     String affinityDecision,
                                     RoutingDecision.CacheAffinity cacheAffinity) {
        String metricIpPort = best.getStatus().getMetricIpPort();
        return () -> {
            reportRoutingCacheMatchMetrics(
                    roleType, selectedRoutingHitTokens, candidateMaxRoutingHitTokens, seqLen);
            if (affinityDecision != null) {
                reportCacheAffinityDecision(roleType, metricIpPort, affinityDecision);
            }
            if ("OVER_CAP".equals(affinityDecision) && cacheAffinity.overCapMs() != null) {
                engineHealthReporter.reportCacheAffinityOverCap(
                        roleType, metricIpPort, cacheAffinity.overCapMs());
            }
        };
    }

    private void recordDecision(BalanceContext context,
                                RoleType role,
                                String group,
                                Integer totalWorkerCount,
                                PrefillCandidateSet candidates,
                                int selectedIndex,
                                String reason,
                                CacheMatchResult cacheMatchResult) {
        List<RoutingDecision.Candidate> snapshot = new ArrayList<>(Math.min(5, candidates.size()));
        if (selectedIndex >= 0) {
            snapshot.add(snapshotCandidate(candidates, selectedIndex, true));
        }
        for (int index = 0; index < candidates.size() && snapshot.size() < 5; index++) {
            if (index != selectedIndex) {
                snapshot.add(snapshotCandidate(candidates, index, false));
            }
        }
        context.recordRoutingDecision(
                new RoutingDecision(role, group, "CostBasedPrefill", reason == null ? "COST_BASED" : reason,
                        selectedIndex < 0 ? null : candidates.affinityDecision(), candidates.cacheAffinity(),
                        kvcmMatches(cacheMatchResult, context.getRequest().getSeqLen(),
                                selectedIndex < 0 ? null : candidates.endpoint(selectedIndex).getStatus()),
                        System.currentTimeMillis(), context.routingAttempt(role),
                        selectedIndex < 0 ? null : candidates.endpointAddress(selectedIndex), totalWorkerCount,
                        candidates.size(), candidates.size() > snapshot.size(), Map.of(), snapshot, null));
    }

    private static RoutingDecision.KvcmMatches kvcmMatches(CacheMatchResult result,
                                                           long inputTokens,
                                                           WorkerStatus selectedWorker) {
        if (result.source() != CacheMatchSource.KVCM || !result.querySucceeded()) {
            return null;
        }
        List<Map.Entry<String, Long>> largestHits = new ArrayList<>(3);
        for (Map.Entry<String, HostCacheMatch> match : result.hostMatches().entrySet()) {
            long hitTokens = CacheMatchResult.matchedTokens(
                    match.getValue().globalMatchBlocks(), result.blockSize(), inputTokens);
            if (hitTokens == 0L) {
                continue;
            }
            int position = 0;
            while (position < largestHits.size()
                    && (largestHits.get(position).getValue() > hitTokens
                        || largestHits.get(position).getValue() == hitTokens
                            && largestHits.get(position).getKey().compareTo(match.getKey()) < 0)) {
                position++;
            }
            if (position < 3) {
                largestHits.add(position, Map.entry(match.getKey(), hitTokens));
                if (largestHits.size() > 3) {
                    largestHits.remove(3);
                }
            }
        }
        Map<String, Long> topHits = new LinkedHashMap<>();
        for (Map.Entry<String, Long> hit : largestHits) {
            topHits.put(hit.getKey(), hit.getValue());
        }
        HostCacheMatch selectedMatch = selectedWorker == null ? null : result.hostMatch(selectedWorker);
        Long selectedHitTokens = null;
        if (selectedWorker != null) {
            selectedHitTokens = selectedMatch == null ? 0L
                    : CacheMatchResult.matchedTokens(selectedMatch.globalMatchBlocks(), result.blockSize(), inputTokens);
        }
        return new RoutingDecision.KvcmMatches(result.hostMatches().size(), topHits, selectedHitTokens);
    }

    private static RoutingDecision.Candidate snapshotCandidate(
            PrefillCandidateSet candidates, int index, boolean selected) {
        long estimatedTtftMs = candidates.estimatedTtftMs(index);
        return new RoutingDecision.Candidate(
                candidates.endpointAddress(index),
                selected,
                estimatedTtftMs < 0L ? null : estimatedTtftMs,
                null,
                candidates.prefillMs(index),
                candidates.cacheHit(index),
                candidates.routingCacheMatchTokens(index),
                0L,
                null,
                null,
                null,
                estimatedTtftMs < 0L ? "UNMODELED_ENGINE_WORK" : "MODELED",
                candidates.ownershipVersion(index));
    }

    private static PlacementResult<SelectedRole, RoleType> classifyAdmissionFailure(EndpointDiscovery discovery,
                                                                                    int priority,
                                                                                    RoleType role,
                                                                                    String group,
                                                                                    long inflightRequestLimit) {
        RejectReason reason = null;
        int workerCount = 0;
        long observedRequestCount = 0;
        for (var entry : discovery.directory()) {
            PrefillEndpoint endpoint = entry.endpoint();
            if (group != null && !group.equals(endpoint.getStatus().topologySnapshot().group())) {
                continue;
            }
            workerCount++;
            observedRequestCount += endpoint.observedRequestCount();
            RejectReason workerReason = endpoint.rejectReason(priority, inflightRequestLimit);
            if (reason == null) {
                reason = workerReason;
            } else if (reason == RejectReason.UNSPECIFIED || workerReason == RejectReason.UNSPECIFIED) {
                reason = RejectReason.UNSPECIFIED;
            } else if (reason != workerReason) {
                reason = RejectReason.RESOURCE_EXHAUSTED;
            }
        }
        Response failure = reason == null ? Response.error(role.getErrorType()) : switch (reason) {
            case HIGHER_PRIORITY_AHEAD, SAME_PRIORITY_AHEAD ->
                    Response.error(StrategyErrorType.PRIORITY_ADMISSION_REJECTED, reason);
            case RESOURCE_EXHAUSTED -> Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
            case UNSPECIFIED -> Response.error(StrategyErrorType.ADMISSION_UNAVAILABLE);
        };
        return blockedWithDiagnostics(failure, role, workerCount, Map.of("observedRequests", observedRequestCount));
    }

    private static PlacementResult<SelectedRole, RoleType> blockedWithDiagnostics(Response failure,
                                                                                  RoleType role,
                                                                                  int workerCount,
                                                                                  Map<String, ?> details) {
        return PlacementResult.blocked(role, failure, Map.of("role", role.name(), "workers", workerCount,
                "observedAtMs", System.currentTimeMillis(), "details", details));
    }

    /**
     * Select one Prefill endpoint from the registered target-group candidates.
     */
    int selectBestCandidate(PrefillCandidateSet survivors,
                            long minEstimatedTtftMs,
                            RoleType roleType,
                            String group,
                            long seqLen,
                            FlexlbConfig config) {
        RoutingConfig.CacheAffinityConfig cacheAffinity = config.getRouter()
                .getRoles().getPrefill().getCacheAffinity();
        BitSet preferredCandidates = new BitSet(survivors.size());
        long affinityCutoffMs = 0L;
        String affinityReason = null;
        if (cacheAffinity != null && survivors.hasKnownTtft) {
            long referenceHitTokens = 0L;
            long maxHitTokens = survivors.maximumCacheHit;
            long maxExtraTtftMs = Math.max(0L, cacheAffinity.getMaxExtraTtftMs());
            double minimumHitRate = normalizedHitRate(cacheAffinity.getMinPrefixHitPercent());
            int fastestHigherHitIndex = -1;
            affinityCutoffMs = saturatingAdd(
                    minEstimatedTtftMs,
                    maxExtraTtftMs);
            affinityReason = maxHitTokens > 0L ? "FASTEST_IS_CACHE_BEST" : "NO_CACHE_ANYWHERE";
            for (int i = 0; i < survivors.size(); i++) {
                if (survivors.ttftRank(i)
                        == minEstimatedTtftMs) {
                    referenceHitTokens = Math.max(
                            referenceHitTokens,
                            survivors.cacheHit(i));
                }
            }
            if (maxHitTokens > referenceHitTokens) {
                boolean minimumHitRateMet = false;
                for (int i = 0; i < survivors.size(); i++) {
                    long hitTokens = survivors.cacheHit(i);
                    if (hitTokens <= referenceHitTokens
                            || minimumHitRate > 0.0
                                    && (seqLen <= 0L
                                        || hitTokens
                                                * RoutingConfig.PERCENTAGE_SCALE
                                                / seqLen
                                                < minimumHitRate)) {
                        continue;
                    }
                    minimumHitRateMet = true;
                    if (survivors.estimatedTtftMs(i) >= 0L
                            && (fastestHigherHitIndex < 0
                                || survivors.ttftRank(i) < survivors.ttftRank(fastestHigherHitIndex))) {
                        fastestHigherHitIndex = i;
                    }
                    if (survivors.estimatedTtftMs(i) >= 0L && survivors.ttftRank(i) <= affinityCutoffMs) {
                        preferredCandidates.set(i);
                    }
                }
                affinityReason = !preferredCandidates.isEmpty()
                        ? "CACHE_LEADER"
                        : minimumHitRateMet ? "OVER_CAP" : "LOW_CACHE_HIT";
            }
            Long fastestHigherHitTtftMs = fastestHigherHitIndex < 0
                    ? null : survivors.estimatedTtftMs(fastestHigherHitIndex);
            survivors.recordCacheAffinity(new RoutingDecision.CacheAffinity(
                    minEstimatedTtftMs, maxExtraTtftMs, minimumHitRate, affinityCutoffMs,
                    fastestHigherHitIndex < 0 ? null : survivors.endpointAddress(fastestHigherHitIndex),
                    fastestHigherHitTtftMs,
                    fastestHigherHitIndex < 0 ? null : survivors.cacheHit(fastestHigherHitIndex),
                    "OVER_CAP".equals(affinityReason) && fastestHigherHitTtftMs != null
                            ? fastestHigherHitTtftMs - affinityCutoffMs : null));
        }

        int selectedIndex;
        if (!preferredCandidates.isEmpty()) {
            selectedIndex = selectCacheLeader(survivors, preferredCandidates, roleType, group);
        } else {
            selectedIndex = selectBaselineCandidate(
                    survivors, minEstimatedTtftMs, roleType, group);
        }

        if (affinityReason != null) {
            survivors.recordAffinityDecision(affinityReason);
            if (Logger.isDebugEnabled()) {
                Logger.debug(
                        "CostBasedPrefill cache-affinity decision - role: {}, group: {}, "
                                + "selected: {}, minEstimatedTtftMs: {}, "
                                + "selectedEstimatedTtftMs: {}, ttftCutoffMs: {}, "
                                + "hitTokens: {}, reason: {}",
                        roleType,
                        group,
                        survivors.endpointAddress(selectedIndex),
                        minEstimatedTtftMs,
                        survivors.ttftRank(selectedIndex),
                        affinityCutoffMs,
                        survivors.cacheHit(selectedIndex),
                        affinityReason);
            }
        }
        return selectedIndex;
    }

    private static double normalizedHitRate(double configuredRate) {
        if (Double.isNaN(configuredRate)
                || configuredRate == Double.POSITIVE_INFINITY) {
            return RoutingConfig.PERCENTAGE_SCALE;
        }
        if (configuredRate == Double.NEGATIVE_INFINITY) {
            return 0.0;
        }
        return Math.min(
                RoutingConfig.PERCENTAGE_SCALE,
                Math.max(0.0, configuredRate));
    }

    private int selectBaselineCandidate(PrefillCandidateSet survivors, long minimumTtftMs,
                                        RoleType role, String group) {
        return tieRotation.next(role, group, survivors.size(),
                i -> survivors.ttftRank(i) == minimumTtftMs
                        && (!survivors.hasKnownTtft || survivors.estimatedTtftMs(i) >= 0L),
                survivors::endpointAddress);
    }

    /** Fairly rotate only endpoints with the same best cache hit and estimated TTFT. */
    private int selectCacheLeader(PrefillCandidateSet survivors,
                                  BitSet preferredCandidates,
                                  RoleType role,
                                  String group) {
        long bestHit = Long.MIN_VALUE;
        long bestEstimatedTtftMs = Long.MAX_VALUE;
        for (int candidateIndex = 0;
                candidateIndex < survivors.size(); candidateIndex++) {
            if (!contains(preferredCandidates, candidateIndex)) {
                continue;
            }
            long hit = survivors.cacheHit(candidateIndex);
            long estimatedTtftMs = survivors.ttftRank(candidateIndex);
            if (hit > bestHit
                    || hit == bestHit && estimatedTtftMs < bestEstimatedTtftMs) {
                bestHit = hit;
                bestEstimatedTtftMs = estimatedTtftMs;
            }
        }
        long selectedHit = bestHit;
        long selectedTtft = bestEstimatedTtftMs;
        return tieRotation.next(role, group, survivors.size(),
                i -> contains(preferredCandidates, i) && survivors.cacheHit(i) == selectedHit
                        && survivors.ttftRank(i) == selectedTtft,
                survivors::endpointAddress);
    }

    private static boolean contains(BitSet candidates, int index) {
        return candidates.get(index);
    }

    private record EndpointDiscovery(
            List<EndpointRegistry.PrefillRoutingEntry> directory,
            List<EndpointRegistry.PrefillRoutingEntry> candidates,
            int registeredCount) {

        /** Zero-copy address view over the exact fleet used by this decision. */
        private List<String> addresses() {
            return new AbstractList<>() {
                @Override
                public String get(int index) {
                    return candidates.get(index).address();
                }

                @Override
                public int size() {
                    return candidates.size();
                }
            };
        }
    }

    private PrefillCandidateSet evaluateCandidates(EndpointDiscovery discovery,
                                                   BalanceContext balanceContext,
                                                   CacheMatchResult cacheMatchResult) {
        Request request = balanceContext.getRequest();
        int eligibleSize = discovery.candidates().size();
        PrefillCandidateSet candidates = CANDIDATES.get();
        candidates.reset(eligibleSize);
        long planningAtMs = System.currentTimeMillis();
        PrefillTtftEstimate.Session estimateSession = PrefillTtftEstimate.session();
        PrefillCacheMatchInputs cacheInputs = PrefillCacheMatchInputs.build(balanceContext, cacheMatchResult);

        // Use one endpoint snapshot for both service prediction and decision-group planning.
        for (int i = 0; i < discovery.candidates().size(); i++) {
            EndpointRegistry.PrefillRoutingEntry routingEntry = discovery.candidates().get(i);

            PrefillEndpoint ep = routingEntry.endpoint();
            String endpointAddress = routingEntry.address();

            CacheTokenMatch cacheMatch = calculateCacheMatch(ep, cacheInputs);
            long cacheHit = cacheMatch.computeHitTokens();
            long routingCacheMatchTokens = cacheMatch.routingHitTokens();
            PrefillTtftEstimate.LoadView loadView = loadProvider.loadView(ep);
            PrefillTtftEstimate.CandidateView estimate = estimateSession.estimateViewForSelection(loadView,
                    request.getRequestId(), balanceContext.getPriority(), planningAtMs,
                    // RequestRegistry owns terminal deadlines.
                    // Selection scores endpoint work only; an expiry race
                    // must not be reported as a capacity blocker.
                    Long.MAX_VALUE, cacheInputs.seqLen(), cacheHit, routingCacheMatchTokens,
                    ep.getPredictor().evaluator(), ep.ttftEstimationRule(), planningAtMs);
            candidates.addCandidate(endpointAddress, ep, estimate, cacheMatch.effectiveHitTokens(),
                    loadView.ownershipVersion());
            if (Logger.isTraceEnabled()) {
                Logger.trace("Prefill estimate - ip: " + endpointAddress
                        + ", order: "
                        + (balanceContext.getConfig().isPriorityOrdering() ? "PRIORITY" : "FIFO")
                        + ", hitCache: " + cacheHit
                        + ", ttftMs: " + estimate.estimatedTtftMsValue());
            }
        }

        return candidates;
    }

    private EndpointDiscovery discoverAvailableEndpoints(RoleType roleType, String group) {
        return discoverEndpoints(roleType, group, false);
    }

    private EndpointDiscovery discoverEndpoints(RoleType roleType, String group, boolean excludeRetired) {
        List<EndpointRegistry.PrefillRoutingEntry> directory = workerDirectory.getPrefillCandidates(roleType);
        ReadOnlyListFilter<EndpointRegistry.PrefillRoutingEntry> filtered = new ReadOnlyListFilter<>(directory);
        int registered = 0;
        for (int index = 0; index < directory.size(); index++) {
            EndpointRegistry.PrefillRoutingEntry entry = directory.get(index);
            PrefillEndpoint endpoint = entry.endpoint();
            boolean matchesGroup = group == null || group.equals(endpoint.getStatus().topologySnapshot().group());
            if (matchesGroup) {
                // 同组但已退役的节点仍计入注册数，是否进入候选集合另行判断。
                registered++;
            }
            filtered.keepIf(index, matchesGroup && (!excludeRetired || !endpoint.isRetired()));
        }
        return new EndpointDiscovery(directory, filtered.toList(), registered);
    }

    private CacheMatchResult getCacheMatchResult(BalanceContext balanceContext, RoleType roleType, String group) {
        Request request = balanceContext.getRequest();
        long blockSize = request.getBlockSize() > 0L
                ? request.getBlockSize()
                : request.getCacheKeyBlockSize();
        CacheMatchResult result = cacheAwareService.findMatchingEngines(
                new CacheMatchQuery(
                        balanceContext.getRequestId(),
                        request.getBlockCacheKeys(),
                        blockSize,
                        roleType,
                        group));
        CacheMatchResult observed = result == null
                ? CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC)
                : result;
        long maxGlobalHitBlocks = 0L;
        if (observed.source() == CacheMatchSource.KVCM && observed.querySucceeded()) {
            for (HostCacheMatch match : observed.hostMatches().values()) {
                maxGlobalHitBlocks = Math.max(maxGlobalHitBlocks, match.globalMatchBlocks());
            }
        }
        balanceContext.setGlobalHitTokens(CacheMatchResult.matchedTokens(maxGlobalHitBlocks, blockSize, request.getSeqLen()));
        balanceContext.recordCacheQuery(observed.source().name(), observed.queryTimeUs());
        return observed;
    }

    private record CacheTokenMatch(long effectiveHitTokens, long routingHitTokens, long computeHitTokens) {
        private static final CacheTokenMatch NONE = new CacheTokenMatch(0L, 0L, 0L);
    }

    private CacheTokenMatch calculateCacheMatch(PrefillEndpoint ep, PrefillCacheMatchInputs inputs) {
        CacheMatchResult cacheMatchResult = inputs.cacheMatchResult();
        long seqLen = inputs.seqLen();
        if (cacheMatchResult == null || seqLen <= 0L) {
            return CacheTokenMatch.NONE;
        }
        WorkerStatus status = ep.getStatus();
        HostCacheMatch match = cacheMatchResult.hostMatch(status);
        long localMatchBlocks = match == null ? 0L : Math.max(0L, match.localMatchBlocks());
        long remoteMatchBlocks = match == null ? 0L : Math.max(0L, match.globalMatchBlocks() - localMatchBlocks);
        double effectiveMatchBlocks = localMatchBlocks + remoteMatchBlocks * inputs.remoteDiscount();
        long blockSize = inputs.blockSize();
        CacheStatus cacheStatus = status == null ? null : status.getCacheStatus();
        if (blockSize <= 0L && cacheStatus != null) {
            blockSize = cacheStatus.getBlockSize();
        }
        if (blockSize <= 0L) {
            return CacheTokenMatch.NONE;
        }
        double rawMatchTokens = blockSize * effectiveMatchBlocks;
        long rawHit = rawMatchTokens >= Long.MAX_VALUE
                ? Long.MAX_VALUE
                : Math.max(0L, Math.round(rawMatchTokens));
        long routingHit = Math.min(seqLen, rawHit);
        long effectiveHit = rawHit >= seqLen ? Math.max(0L, seqLen - blockSize) : routingHit;
        long localHitTokens = CacheMatchResult.matchedTokens(localMatchBlocks, blockSize, seqLen);
        long remoteHitTokens = CacheMatchResult.matchedTokens(remoteMatchBlocks, blockSize, seqLen);
        long remoteWorkHitTokens = inputs.kvcmSource() ? inputs.globalHitTokens() - localHitTokens : remoteHitTokens;

        long computeHit =
                Math.round(localHitTokens * inputs.localWeight() + remoteWorkHitTokens * inputs.remoteWeight());

        // 初次缓存预测至少保留一块计算量；不足一块时保留全部输入。
        // 例如输入 1000、块大小 100、global 命中 900，local/remote 权重为 1.5/1：
        // local 命中 100/300 时，加权命中为 950/1050，均按 900 抵扣，避免跨全长阈值后抵扣反而变少。
        computeHit = Math.min(computeHit, Math.max(0L, seqLen - blockSize));
        return new CacheTokenMatch(effectiveHit, routingHit, computeHit);
    }

    private void reportSelectedEstimates(
            RoleType roleType,
            PrefillEndpoint endpoint,
            FlexlbConfig config,
            long estimatedTtftMs,
            long executionTimeMs) {
        String deliveryMode = config.getDispatcher().typeName();
        try {
            engineHealthReporter.reportPrefillSelectedEstimates(
                    roleType,
                    endpoint.getStatus().getMetricIpPort(),
                    deliveryMode,
                    estimatedTtftMs,
                    executionTimeMs);
        } catch (RuntimeException telemetryFailure) {
            Logger.warn(
                    "Prefill selected-estimate metric failed: engine={}, delivery_mode={}",
                    endpoint.ipPort(), deliveryMode, telemetryFailure);
        }
    }

    private void reportRoutingCacheMatchMetrics(
            RoleType roleType,
            long selectedHitTokens,
            long candidateMaxHitTokens,
            long totalTokens) {
        engineHealthReporter.reportRoutingSelectedCacheMatchMetrics(roleType, selectedHitTokens);
        engineHealthReporter.reportRoutingCandidateMaxCacheMatchMetrics(
                roleType, candidateMaxHitTokens);
    }

    private SelectedRole buildSelectedRole(PrefillEndpoint ep,
                                           RoleType roleType,
                                           String requestId,
                                           long estimatedTtftMs,
                                           long selectedPrefillMs,
                                           long bestCacheHit,
                                           long placementVersion,
                                           WorkerEndpoint.EndpointHold selectedHold) {
        try {
            if (selectedHold == null || selectedHold.endpoint() != ep) {
                throw new IllegalStateException(
                        "selected Prefill endpoint generation changed before handoff");
            }
            ServerStatus result = buildPrefillStatus(ep, roleType, requestId, estimatedTtftMs, bestCacheHit);
            WorkerEndpoint.EndpointHold ownedHold = selectedHold;
            selectedHold = null;
            return SelectedRole.prefill(ownedHold, result, selectedPrefillMs, placementVersion);
        } finally {
            if (selectedHold != null) {
                selectedHold.close();
            }
        }
    }

    private ServerStatus buildPrefillStatus(PrefillEndpoint endpoint,
                                            RoleType role,
                                            String requestId,
                                            long estimatedTtftMs,
                                            long hitCacheTokens) {
        WorkerStatus workerStatus = endpoint.getStatus();
        WorkerStatus.TopologySnapshot topology = workerStatus.topologySnapshot();
        WorkerStatus.EngineObservation status = workerStatus.committedEngineObservation();
        // ScheduledRequest.hitCache() reads this field for batch metrics.
        DebugInfo debugInfo = new DebugInfo();
        debugInfo.setHitCacheLen(hitCacheTokens);
        ServerStatus result = new ServerStatus();
        result.setRole(role);
        result.setRequestId(requestId);
        if (estimatedTtftMs >= 0L) {
            result.setPrefillTime(estimatedTtftMs);
        }
        result.setGroup(topology.group());
        result.setServerIp(topology.ip());
        result.setHttpPort(topology.port());
        result.setGrpcPort(CommonUtils.toGrpcPort(topology.port()));
        result.setDpRank(status.dpRank());
        result.setDebugInfo(debugInfo);
        result.setSuccess(true);
        result.setSelectedEngineIndex(topology.engineIndex(), topology.multiEngineNum());
        return result;
    }

    private void reportCacheAffinityDecision(
            RoleType roleType, String engineIp, String decision) {
        engineHealthReporter.reportCacheAffinityDecision(roleType, engineIp, decision);
    }

    private static long saturatingAdd(long left, long right) {
        if (right > 0L && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        if (right < 0L && left < Long.MIN_VALUE - right) {
            return Long.MIN_VALUE;
        }
        return left + right;
    }

}

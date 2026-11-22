package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.cache.domain.CacheMatchQuery;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.BalanceMetrics.PrefillRouteMetrics;
import org.flexlb.dao.cache.HostCacheMatch;
import org.flexlb.dao.loadbalance.DebugInfo;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.pv.RoutingDecision;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.WorkerDirectory;
import org.flexlb.util.CommonUtils;
import org.flexlb.util.Logger;
import org.flexlb.util.ReadOnlyListFilter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

@Component
public class CostBasedPrefillStrategy {

    private final WorkerDirectory workerDirectory;
    private final CacheAwareService cacheAwareService;
    private final EngineHealthReporter engineHealthReporter;
    private final EndpointRoundRobin tieRotation;

    public CostBasedPrefillStrategy(WorkerDirectory workerDirectory,
                                    CacheAwareService cacheAwareService,
                                    EngineHealthReporter engineHealthReporter,
                                    EndpointRoundRobin tieRotation) {
        this.workerDirectory = workerDirectory;
        this.cacheAwareService = cacheAwareService;
        this.engineHealthReporter = engineHealthReporter;
        this.tieRotation = tieRotation;
    }

    /**
     * 并行准备完整候选集合的缓存匹配和请求自身耗时。排队负载留到调度线程作最终决策时再读取。
     */
    public RequestWorkLoadEstimateResult prepare(BalanceContext context, RoleType role, String group) {
        context.beginRoutingAttempt(role);
        var endpoints = discoverActiveEndpoints(role, group);
        if (endpoints.isEmpty()) {
            return null;
        }
        CacheMatchResult cacheMatch = getCacheMatchResult(context, role, group);
        PrefillCacheMatchInputs cacheInputs = PrefillCacheMatchInputs.build(context, cacheMatch);
        List<PrefillCandidateWork> candidates = new ArrayList<>(endpoints.size());
        for (EndpointRegistry.PrefillRoutingEntry entry : endpoints) {
            CacheTokenMatch match = calculateCacheMatch(entry.endpoint(), cacheInputs);
            long estimatedPrefillComputeTokens = Math.max(0L, cacheInputs.seqLen() - match.hitTokensForWorkload());
            long predictedMs =
                    PrefillPredictionBoundary.predictSingleRequestMs(entry.endpoint().getPredictor().evaluator(),
                            cacheInputs.seqLen(), estimatedPrefillComputeTokens, match.hitTokensForWorkload());
            candidates.add(new PrefillCandidateWork(entry.address(), entry.endpoint(), match.hitTokensForSelection(),
                    match.localHitTokens(), predictedMs, estimatedPrefillComputeTokens));
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
        PrefillCandidateSet candidates = new PrefillCandidateSet(preparedCandidates.size());
        for (PrefillCandidateWork candidate : preparedCandidates) {
            long estimatedMs = ttft.applyAsLong(candidate);
            candidates.addCandidate(new PrefillCandidateInfo(candidate.address(), candidate.endpoint(), estimatedMs,
                    candidate.predictedMs(), candidate.cacheHitTokens(), candidate.localHitTokens(),
                    candidate.endpoint().prefillQueue().statusVersion()));
        }
        CacheMatchResult cacheMatch = inputs.cacheMatch();
        int selectedIndex = selectPrefillIndex(context, role, group, cacheMatch, candidates);
        PrefillCandidateInfo selected = candidates.get(selectedIndex);
        PrefillEndpoint endpoint = selected.endpoint();
        long hitCacheTokens = selected.effectiveCacheHitTokens();
        ServerStatus status =
                buildPrefillStatus(endpoint, role, context.getRequestId(), selected.estimatedTtftMs(), hitCacheTokens);

        recordSelectedPrefill(context, role, group, null, cacheMatch, candidates, selected);
        recordV2PrefillMetrics(context, candidates, selected);

        long seqLen = context.getRequest().getSeqLen();
        return new SelectedPrefill(endpoint, status, Math.max(0L, Math.min(seqLen, hitCacheTokens)),
                selected.prefillMs(), preparedCandidates.get(selectedIndex).estimatedPrefillComputeTokens());
    }

    private int selectPrefillIndex(BalanceContext context,
                                   RoleType role,
                                   String group,
                                   CacheMatchResult cacheMatch,
                                   PrefillCandidateSet candidates) {
        long seqLen = context.getRequest().getSeqLen();
        int selectedIndex =
                selectBestCandidate(candidates, candidates.minimumTtftRank, role, group, seqLen, context.getConfig());
        if (candidates.affinityDecision() == CacheAffinityDecision.NO_CACHE_ANYWHERE) {
            if (seqLen < cacheMatch.blockSize()) {
                candidates.recordAffinityDecision(CacheAffinityDecision.NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE);
            } else if (cacheMatch.source() == CacheMatchSource.KVCM && cacheMatch.querySucceeded()) {
                boolean kvcmHasHit =
                        cacheMatch.hostMatches().values().stream().anyMatch(match -> match.globalMatchBlocks() > 0L);
                candidates.recordAffinityDecision(kvcmHasHit
                        ? CacheAffinityDecision.NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT
                        : CacheAffinityDecision.NO_CACHE_ANYWHERE_KVCM);
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
                                       PrefillCandidateInfo selected) {
        PrefillEndpoint best = selected.endpoint();
        long bestCacheHit = selected.effectiveCacheHitTokens();
        if (balanceContext.selectionReason(roleType) == null) {
            balanceContext.recordSelectionReason(roleType, "COST_BASED");
        }
        cacheAwareService.trackRoutingPrediction(balanceContext.getRequestId(), roleType, group, best.getStatus(),
                balanceContext.getRequest().getSeqLen(), bestCacheHit, cacheMatchResult,
                balanceContext.getTheoryHitTokens());
        balanceContext.recordCacheSelection(roleType, best.getIp(), bestCacheHit);
        recordDecision(balanceContext, roleType, group, totalWorkerCount, candidates, selected,
                balanceContext.selectionReason(roleType), cacheMatchResult);
        long selectedTtft = selected.estimatedTtftMs();
        if (selectedTtft >= 0L) {
            reportSelectedEstimates(roleType, best, balanceContext.getConfig(), selectedTtft, selected.prefillMs());
        }
    }

    private void recordV2PrefillMetrics(BalanceContext context,
                                        PrefillCandidateSet candidates,
                                        PrefillCandidateInfo selected) {
        CacheAffinityDecision decision = candidates.affinityDecision();
        Long overCapMs = decision == CacheAffinityDecision.OVER_CAP ? candidates.cacheAffinity().overCapMs() : null;
        if (decision != null && selected.endpoint().getStatus().getMetricIpPort() == null) {
            decision = null;
        }
        context.getMetrics()
                .setPrefillRouteMetrics(new PrefillRouteMetrics(selected.effectiveCacheHitTokens(),
                        candidates.maxEffectiveCacheHitTokens, decision == null ? null : decision.name(), overCapMs));
    }

    private void recordDecision(BalanceContext context,
                                RoleType role,
                                String group,
                                Integer totalWorkerCount,
                                PrefillCandidateSet candidates,
                                PrefillCandidateInfo selected,
                                String reason,
                                CacheMatchResult cacheMatchResult) {
        List<RoutingDecision.Candidate> snapshot = new ArrayList<>(Math.min(5, candidates.size()));
        snapshot.add(snapshotCandidate(selected, true));
        for (int index = 0; index < candidates.size() && snapshot.size() < 5; index++) {
            PrefillCandidateInfo candidate = candidates.get(index);
            if (candidate != selected) {
                snapshot.add(snapshotCandidate(candidate, false));
            }
        }
        CacheAffinityDecision decision = candidates.affinityDecision();
        context.recordRoutingDecision(
                new RoutingDecision(role, group, "CostBasedPrefill", reason == null ? "COST_BASED" : reason,
                        decision == null ? null : decision.name(), candidates.cacheAffinity(),
                        kvcmMatches(cacheMatchResult, context.getRequest().getSeqLen(),
                                selected.endpoint().getStatus()),
                        System.currentTimeMillis(), context.routingAttempt(role), selected.address(), totalWorkerCount,
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

    private static RoutingDecision.Candidate snapshotCandidate(PrefillCandidateInfo candidate, boolean selected) {
        long estimatedTtftMs = candidate.estimatedTtftMs();
        return new RoutingDecision.Candidate(candidate.address(), selected,
                estimatedTtftMs < 0L ? null : estimatedTtftMs, null, candidate.prefillMs(),
                candidate.effectiveCacheHitTokens(), 0L, null, null, null,
                estimatedTtftMs < 0L ? "UNMODELED_ENGINE_WORK" : "MODELED", candidate.statusVersion());
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
        CacheAffinityDecision affinityReason = null;
        if (cacheAffinity != null && survivors.hasKnownTtft) {
            long referenceHitTokens = 0L;
            long maxHitTokens = survivors.maxEffectiveCacheHitTokens;
            long maxExtraTtftMs = Math.max(0L, cacheAffinity.getMaxExtraTtftMs());
            double minimumHitRate = normalizedHitRate(cacheAffinity.getMinPrefixHitPercent());
            PrefillCandidateInfo fastestHigherHit = null;
            affinityCutoffMs = saturatingAdd(
                    minEstimatedTtftMs,
                    maxExtraTtftMs);
            affinityReason = maxHitTokens > 0L
                    ? CacheAffinityDecision.FASTEST_IS_CACHE_BEST
                    : CacheAffinityDecision.NO_CACHE_ANYWHERE;
            for (int i = 0; i < survivors.size(); i++) {
                PrefillCandidateInfo candidate = survivors.get(i);
                if (candidate.ttftRank() == minEstimatedTtftMs) {
                    referenceHitTokens = Math.max(referenceHitTokens, candidate.effectiveCacheHitTokens());
                }
            }
            if (maxHitTokens > referenceHitTokens) {
                boolean minimumHitRateMet = false;
                for (int i = 0; i < survivors.size(); i++) {
                    PrefillCandidateInfo candidate = survivors.get(i);
                    long hitTokens = candidate.effectiveCacheHitTokens();
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
                    if (candidate.estimatedTtftMs() < 0L) {
                        continue;
                    }
                    if (fastestHigherHit == null || candidate.estimatedTtftMs() < fastestHigherHit.estimatedTtftMs()) {
                        fastestHigherHit = candidate;
                    }
                    if (candidate.estimatedTtftMs() <= affinityCutoffMs) {
                        preferredCandidates.set(i);
                    }
                }
                affinityReason = !preferredCandidates.isEmpty()
                        ? CacheAffinityDecision.CACHE_LEADER
                        : minimumHitRateMet ? CacheAffinityDecision.OVER_CAP : CacheAffinityDecision.LOW_CACHE_HIT;
            }
            Long fastestHigherHitTtftMs = fastestHigherHit == null ? null : fastestHigherHit.estimatedTtftMs();
            survivors.recordCacheAffinity(new RoutingDecision.CacheAffinity(minEstimatedTtftMs, maxExtraTtftMs,
                    minimumHitRate, affinityCutoffMs, fastestHigherHit == null ? null : fastestHigherHit.address(),
                    fastestHigherHitTtftMs,
                    fastestHigherHit == null ? null : fastestHigherHit.effectiveCacheHitTokens(),
                    affinityReason == CacheAffinityDecision.OVER_CAP && fastestHigherHitTtftMs != null
                            ? fastestHigherHitTtftMs - affinityCutoffMs
                            : null));
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
                PrefillCandidateInfo selected = survivors.get(selectedIndex);
                Logger.debug(
                        "CostBasedPrefill cache-affinity decision - role: {}, group: {}, "
                                + "selected: {}, minEstimatedTtftMs: {}, "
                                + "selectedEstimatedTtftMs: {}, ttftCutoffMs: {}, " + "hitTokens: {}, reason: {}",
                        roleType, group, selected.address(), minEstimatedTtftMs, selected.ttftRank(), affinityCutoffMs,
                        selected.effectiveCacheHitTokens(), affinityReason);
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

    private int selectBaselineCandidate(PrefillCandidateSet survivors,
                                        long minimumTtftMs,
                                        RoleType role,
                                        String group) {
        return tieRotation.next(role, group, survivors.size(), i -> {
            PrefillCandidateInfo candidate = survivors.get(i);
            return candidate.ttftRank() == minimumTtftMs
                    && (!survivors.hasKnownTtft || (candidate.estimatedTtftMs() >= 0L
                            && candidate.localHitTokens() == survivors.maxLocalHitTokensAtMinTtft));
        }, i -> survivors.get(i).address());
    }

    /**
     * 缓存亲和仍先比综合命中、再比 TTFT；两项相同时优先本地命中更多的节点。
     */
    private int selectCacheLeader(PrefillCandidateSet survivors,
                                  BitSet preferredCandidates,
                                  RoleType role,
                                  String group) {
        long bestHit = Long.MIN_VALUE;
        long bestEstimatedTtftMs = Long.MAX_VALUE;
        long bestLocalHitTokens = 0L;
        for (int candidateIndex = 0;
                candidateIndex < survivors.size(); candidateIndex++) {
            if (!contains(preferredCandidates, candidateIndex)) {
                continue;
            }
            PrefillCandidateInfo candidate = survivors.get(candidateIndex);
            long hit = candidate.effectiveCacheHitTokens();
            long estimatedTtftMs = candidate.ttftRank();
            long localHitTokens = candidate.localHitTokens();
            if (hit > bestHit || hit == bestHit && (estimatedTtftMs < bestEstimatedTtftMs
                    || estimatedTtftMs == bestEstimatedTtftMs && localHitTokens > bestLocalHitTokens)) {
                bestHit = hit;
                bestEstimatedTtftMs = estimatedTtftMs;
                bestLocalHitTokens = localHitTokens;
            }
        }
        long selectedHit = bestHit;
        long selectedTtft = bestEstimatedTtftMs;
        long selectedLocalHitTokens = bestLocalHitTokens;
        return tieRotation.next(role, group, survivors.size(), i -> {
            PrefillCandidateInfo candidate = survivors.get(i);
            return contains(preferredCandidates, i) && candidate.effectiveCacheHitTokens() == selectedHit
                    && candidate.ttftRank() == selectedTtft && candidate.localHitTokens() == selectedLocalHitTokens;
        }, i -> survivors.get(i).address());
    }

    private static boolean contains(BitSet candidates, int index) {
        return candidates.get(index);
    }

    private List<EndpointRegistry.PrefillRoutingEntry> discoverActiveEndpoints(RoleType roleType, String group) {
        List<EndpointRegistry.PrefillRoutingEntry> directory = workerDirectory.getPrefillCandidates(roleType);
        ReadOnlyListFilter<EndpointRegistry.PrefillRoutingEntry> filtered = new ReadOnlyListFilter<>(directory);
        for (int index = 0; index < directory.size(); index++) {
            EndpointRegistry.PrefillRoutingEntry entry = directory.get(index);
            PrefillEndpoint endpoint = entry.endpoint();
            boolean matchesGroup = group == null || group.equals(endpoint.getStatus().topologySnapshot().group());
            filtered.keepIf(index, matchesGroup && !endpoint.isRetired());
        }
        return filtered.toList();
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

    private record CacheTokenMatch(long hitTokensForSelection, long hitTokensForWorkload, long localHitTokens) {
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
        double matchBlocksForSelection = localMatchBlocks + remoteMatchBlocks * inputs.remoteDiscount();
        long blockSize = inputs.blockSize();
        CacheStatus cacheStatus = status == null ? null : status.getCacheStatus();
        if (blockSize <= 0L && cacheStatus != null) {
            blockSize = cacheStatus.getBlockSize();
        }
        if (blockSize <= 0L) {
            return CacheTokenMatch.NONE;
        }
        double rawMatchTokens = blockSize * matchBlocksForSelection;
        long rawHit = rawMatchTokens >= Long.MAX_VALUE
                ? Long.MAX_VALUE
                : Math.max(0L, Math.round(rawMatchTokens));
        long hitTokensForSelection = rawHit >= seqLen ? Math.max(0L, seqLen - blockSize) : rawHit;
        long localHitTokens = CacheMatchResult.matchedTokens(localMatchBlocks, blockSize, seqLen);
        long remoteHitTokens = CacheMatchResult.matchedTokens(remoteMatchBlocks, blockSize, seqLen);
        long remoteWorkHitTokens = inputs.kvcmSource() ? inputs.globalHitTokens() - localHitTokens : remoteHitTokens;

        long hitTokensForWorkload =
                Math.round(localHitTokens * inputs.localWeight() + remoteWorkHitTokens * inputs.remoteWeight());

        // 初次缓存预测至少保留一块计算量；不足一块时保留全部输入。
        // 例如输入 1000、块大小 100、global 命中 900，local/remote 权重为 1.5/1：
        // local 命中 100/300 时，加权命中为 950/1050，均按 900 抵扣，避免跨全长阈值后抵扣反而变少。
        hitTokensForWorkload = Math.min(hitTokensForWorkload, Math.max(0L, seqLen - blockSize));
        return new CacheTokenMatch(hitTokensForSelection, hitTokensForWorkload, localHitTokens);
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

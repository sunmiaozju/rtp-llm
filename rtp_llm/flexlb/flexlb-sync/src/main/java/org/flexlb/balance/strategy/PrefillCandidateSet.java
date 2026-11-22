package org.flexlb.balance.strategy;

import org.flexlb.dao.pv.RoutingDecision;

import java.util.ArrayList;
import java.util.List;

/**
 * 一次 Prefill 选路的候选列表和评分统计。同一请求再次选路时也会新建一份。
 */
final class PrefillCandidateSet {

    /**
     * 本次参与评分的全部候选，按加入顺序保存；轮询和路由快照均使用这份列表。
     */
    private final List<PrefillCandidateInfo> candidates;

    /**
     * 已知 TTFT 中的最小值（毫秒）；尚无已知估算时为 {@link Long#MAX_VALUE}。
     */
    long minimumTtftRank = Long.MAX_VALUE;

    /**
     * 仅在达到最低已知 TTFT 的候选中统计的最大本地命中 token 数；遇到更低的 TTFT 时重新计数。
     */
    long maxLocalHitTokensAtMinTtft;

    /**
     * 是否有候选给出了非负 TTFT，用于区分没有估算和估算值恰为 {@link Long#MAX_VALUE}。
     */
    boolean hasKnownTtft;

    /**
     * 全部候选中最高的有效缓存命中 token 数，包括 TTFT 未知的候选。
     */
    long maxEffectiveCacheHitTokens;

    /**
     * 缓存亲和选路的结果原因，例如 CACHE_LEADER 或 OVER_CAP；未评估缓存亲和时为 null。
     */
    private CacheAffinityDecision affinityDecision;

    /**
     * 本次缓存亲和判断使用的 TTFT 基准、门槛和更高命中节点的比较结果；未评估时为 null。
     */
    private RoutingDecision.CacheAffinity cacheAffinity;

    PrefillCandidateSet(int expectedSize) {
        candidates = new ArrayList<>(expectedSize);
    }

    void addCandidate(PrefillCandidateInfo candidate) {
        candidates.add(candidate);
        long estimatedMs = candidate.estimatedTtftMs();
        if (estimatedMs >= 0L) {
            if (!hasKnownTtft || estimatedMs < minimumTtftRank) {
                minimumTtftRank = estimatedMs;
                maxLocalHitTokensAtMinTtft = candidate.localHitTokens();
            } else if (estimatedMs == minimumTtftRank) {
                maxLocalHitTokensAtMinTtft = Math.max(maxLocalHitTokensAtMinTtft, candidate.localHitTokens());
            }
            hasKnownTtft = true;
        }
        maxEffectiveCacheHitTokens = Math.max(maxEffectiveCacheHitTokens, candidate.effectiveCacheHitTokens());
    }

    CacheAffinityDecision affinityDecision() {
        return affinityDecision;
    }

    void recordAffinityDecision(CacheAffinityDecision decision) {
        affinityDecision = decision;
    }

    RoutingDecision.CacheAffinity cacheAffinity() {
        return cacheAffinity;
    }

    void recordCacheAffinity(RoutingDecision.CacheAffinity details) {
        cacheAffinity = details;
    }

    PrefillCandidateInfo get(int index) {
        return candidates.get(index);
    }

    int size() {
        return candidates.size();
    }
}

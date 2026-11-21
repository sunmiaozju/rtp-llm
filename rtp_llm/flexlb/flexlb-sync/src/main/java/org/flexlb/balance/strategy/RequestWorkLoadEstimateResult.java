package org.flexlb.balance.strategy;

import org.flexlb.cache.domain.CacheMatchResult;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 缓存匹配和候选 Prefill 的耗时估算
 */
public record RequestWorkLoadEstimateResult(CacheMatchResult cacheMatch,
                                       List<PrefillCandidateWork> candidates,
                                       Throwable failure,
                                       long preparedAtNanos) {

    public static RequestWorkLoadEstimateResult success(CacheMatchResult cacheMatch,
                                                        List<PrefillCandidateWork> candidates) {
        return new RequestWorkLoadEstimateResult(cacheMatch, candidates, null, System.nanoTime());
    }

    public static RequestWorkLoadEstimateResult failed(Throwable failure) {
        return new RequestWorkLoadEstimateResult(null, List.of(), failure, System.nanoTime());
    }

    public boolean expired(long ttlMs) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - preparedAtNanos) >= ttlMs;
    }
}

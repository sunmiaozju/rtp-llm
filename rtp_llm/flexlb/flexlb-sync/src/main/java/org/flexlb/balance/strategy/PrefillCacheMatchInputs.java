package org.flexlb.balance.strategy;

import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;

/**
 * 同一次候选遍历共用的请求参数、缓存查询结果和命中权重。节点 block size 的兜底值仍按节点读取。
 */
record PrefillCacheMatchInputs(CacheMatchResult cacheMatchResult,
                               long seqLen,
                               long blockSize,
                               double remoteDiscount,
                               double localWeight,
                               double remoteWeight,
                               long globalHitTokens,
                               boolean kvcmSource,
                               boolean kvcmQuerySucceeded) {

    static PrefillCacheMatchInputs build(BalanceContext context, CacheMatchResult cacheMatchResult) {
        Request request = context.getRequest();
        long seqLen = request == null ? 0L : request.getSeqLen();
        if (cacheMatchResult == null || seqLen <= 0L) {
            return new PrefillCacheMatchInputs(cacheMatchResult, seqLen, 0L, 0.0, 0.0, 0.0, 0L, false, false);
        }
        var prefillConfig = context.getConfig().getRouter().getRoles().getPrefill();
        var affinity = prefillConfig.getCacheAffinity();
        double remoteDiscount = affinity == null ? 0.2 : Math.max(0.0, affinity.getRemoteDiscount());
        var estimatorConfig = prefillConfig.getExecutionTimeEstimator();
        long blockSize = cacheMatchResult.blockSize();
        if (blockSize <= 0L) {
            blockSize = request.getBlockSize() > 0L ? request.getBlockSize() : request.getCacheKeyBlockSize();
        }
        boolean kvcmSource = cacheMatchResult.source() == CacheMatchSource.KVCM;
        return new PrefillCacheMatchInputs(cacheMatchResult, seqLen, blockSize, remoteDiscount,
                estimatorConfig.getLocalWeight(), estimatorConfig.getRemoteWeight(), context.getGlobalHitTokens(),
                kvcmSource, kvcmSource && cacheMatchResult.querySucceeded());
    }
}

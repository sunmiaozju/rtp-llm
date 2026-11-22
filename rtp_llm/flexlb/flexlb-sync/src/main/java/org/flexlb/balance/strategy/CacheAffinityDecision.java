package org.flexlb.balance.strategy;

/**
 * Prefill 缓存亲和选路的决策原因。命中数指本地命中加上折算后的远端命中；枚举名用于路由日志和监控标签。
 */
enum CacheAffinityDecision {
    /**
     * 所有候选的有效命中均为零，按基础策略选点。
     */
    NO_CACHE_ANYWHERE,

    /**
     * 所有候选的有效命中均为零，且请求长度不足一个缓存块。
     */
    NO_CACHE_ANYWHERE_BELOW_BLOCK_SIZE,

    /**
     * 所有候选的有效命中均为零；KVCM 查询成功，也没有返回任何全局命中块。
     */
    NO_CACHE_ANYWHERE_KVCM,

    /**
     * 所有候选的有效命中均为零，但 KVCM 查询成功且至少一个节点有全局命中块。
     */
    NO_CACHE_ANYWHERE_BUT_KVCM_HAS_HIT,

    /**
     * 最低 TTFT 的候选中已有节点达到全体最高有效命中，且命中数大于零，继续按基础策略选点。
     */
    FASTEST_IS_CACHE_BEST,

    /**
     * 有候选比最低 TTFT 候选中的最高命中更多，且满足最低命中率和额外 TTFT 上限，从这些候选中按缓存亲和规则选点。
     */
    CACHE_LEADER,

    /**
     * 有命中更多且达到最低命中率的候选，但它们的 TTFT 均超出上限或未知，仍按基础策略选点。
     */
    OVER_CAP,

    /**
     * 有命中更多的候选，但没有一个达到最低命中率，仍按基础策略选点。
     */
    LOW_CACHE_HIT
}

package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.PrefillEndpoint;

/**
 * 本次 Prefill 选路中，一个候选节点对当前请求的评分数据。
 *
 * @param address 节点在注册表中的地址，用于同分轮询和路由日志
 * @param endpoint 该地址对应的 Prefill 节点实例，选中后随选路结果返回
 * @param estimatedTtftMs 预计首 token 耗时（毫秒），计入节点已有工作量；负数表示无法估算
 * @param prefillMs 仅当前请求自身的预计 Prefill 执行耗时（毫秒），不含排队时间
 * @param effectiveCacheHitTokens 缓存亲和选路使用的命中 token 数，由本地命中和折算后的远端命中得到
 * @param localHitTokens 仅在该节点本地命中的 token 数，用于同分时优先选择本地命中更多的节点
 * @param statusVersion 评分时读到的节点 Prefill 队列状态版本，随候选评分快照记录
 */
record PrefillCandidateInfo(String address,
                            PrefillEndpoint endpoint,
                            long estimatedTtftMs,
                            long prefillMs,
                            long effectiveCacheHitTokens,
                            long localHitTokens,
                            long statusVersion) {

    long ttftRank() {
        return estimatedTtftMs < 0L ? Long.MAX_VALUE : estimatedTtftMs;
    }
}

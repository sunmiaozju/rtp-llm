package org.flexlb.balance.schedulev2.request;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.dao.loadbalance.ServerStatus;

/**
 * 主队列给请求选好的 Prefill。
 *
 * @param endpoint         选中的 Prefill。同一地址重新上线后是另一个对象，dispatch 前据此判断机器是否还是选中的那台
 * @param serverStatus     返回给调用方的 Prefill 地址等信息
 * @param hitCacheTokens   用于选路的预估缓存命中 token 数
 * @param predictedMs      预估的 Prefill 耗时
 * @param estimatedPrefillComputeTokens 预测耗时时使用的待计算 token 数，不是 Worker 实测值
 */
public record SelectedPrefill(PrefillEndpoint endpoint,
                              ServerStatus serverStatus,
                              long hitCacheTokens,
                              long predictedMs,
                              long estimatedPrefillComputeTokens) {
}

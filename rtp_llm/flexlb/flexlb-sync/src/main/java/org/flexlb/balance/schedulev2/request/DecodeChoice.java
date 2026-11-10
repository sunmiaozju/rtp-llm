package org.flexlb.balance.schedulev2.request;

import org.flexlb.balance.endpoint.DecodeEndpoint;

import java.util.List;

/**
 * Decode 队列给请求选好并 reserve 的 Decode。
 *
 * @param endpoint     选中的 Decode
 * @param reservation  在这台 Decode 上的 reserve，拿 permit 和归还时都用它
 * @param preemptedRequests 抢占成功时的受害者快照，响应成功返回后用于上报双方优先级和受害阶段
 */
public record DecodeChoice(DecodeEndpoint endpoint,
                           DecodeEndpoint.ReservationHandle reservation,
                           List<DecodeEndpoint.DecodeRequestView> preemptedRequests) {

    public DecodeChoice(DecodeEndpoint endpoint, DecodeEndpoint.ReservationHandle reservation) {
        this(endpoint, reservation, List.of());
    }
}
